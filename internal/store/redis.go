package store

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/redis/go-redis/v9"

	"proxy-monitor/internal/model"
)

// Redis key constants.
const (
	keyConfig        = "config"
	keyProxies       = "proxies"         // SET of proxy IDs
	keyActiveAlertID = "active_alert_id" // STRING
	keyAlerts        = "alerts"          // LIST of alert IDs (chronological)
	keyWebhooks      = "webhooks"        // SET of webhook IDs
	keyIntegrations  = "integrations"    // SET of integration IDs
)

func proxyKey(id string) string      { return "proxy:" + id }
func historyKey(id string) string     { return "proxy:" + id + ":history" }
func alertKey(id string) string       { return "alert:" + id }
func webhookKey(id string) string     { return "webhook:" + id }
func integrationKey(id string) string { return "integration:" + id }

// RedisStore implements Store using Redis as the backing datastore.
type RedisStore struct {
	rdb *redis.Client
	ctx context.Context
}

// NewRedisStore creates a RedisStore from an already-connected redis.Client.
func NewRedisStore(rdb *redis.Client) *RedisStore {
	return &RedisStore{rdb: rdb, ctx: context.Background()}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func marshalJSON(v any) (string, error) {
	b, err := json.Marshal(v)
	return string(b), err
}

func unmarshalJSON[T any](data string) (*T, error) {
	var v T
	if err := json.Unmarshal([]byte(data), &v); err != nil {
		return nil, err
	}
	return &v, nil
}

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

func (s *RedisStore) GetConfig() (*model.Config, error) {
	data, err := s.rdb.Get(s.ctx, keyConfig).Result()
	if err == redis.Nil {
		// Return defaults if no config stored yet.
		cfg := &model.Config{CheckIntervalSeconds: 30, RequestTimeoutMs: 5000}
		_ = s.SetConfig(cfg)
		return cfg, nil
	}
	if err != nil {
		return nil, fmt.Errorf("redis get config: %w", err)
	}
	return unmarshalJSON[model.Config](data)
}

func (s *RedisStore) SetConfig(cfg *model.Config) error {
	data, err := marshalJSON(cfg)
	if err != nil {
		return err
	}
	return s.rdb.Set(s.ctx, keyConfig, data, 0).Err()
}

// ---------------------------------------------------------------------------
// Proxies
// ---------------------------------------------------------------------------

func (s *RedisStore) GetProxy(id string) (*model.Proxy, error) {
	data, err := s.rdb.Get(s.ctx, proxyKey(id)).Result()
	if err == redis.Nil {
		return nil, nil // not found
	}
	if err != nil {
		return nil, fmt.Errorf("redis get proxy %s: %w", id, err)
	}
	p, err := unmarshalJSON[model.Proxy](data)
	if err != nil {
		return nil, err
	}
	// History is stored separately — load it.
	history, err := s.GetHistory(id)
	if err != nil {
		return nil, err
	}
	p.History = history
	return p, nil
}

func (s *RedisStore) SetProxy(p *model.Proxy) error {
	// Store the proxy without history (history lives in its own list).
	clone := *p
	clone.History = nil
	data, err := marshalJSON(clone)
	if err != nil {
		return err
	}
	pipe := s.rdb.Pipeline()
	pipe.Set(s.ctx, proxyKey(p.ID), data, 0)
	pipe.SAdd(s.ctx, keyProxies, p.ID)
	_, err = pipe.Exec(s.ctx)
	return err
}

func (s *RedisStore) GetAllProxies() ([]*model.Proxy, error) {
	ids, err := s.rdb.SMembers(s.ctx, keyProxies).Result()
	if err != nil {
		return nil, fmt.Errorf("redis smembers proxies: %w", err)
	}
	if len(ids) == 0 {
		return []*model.Proxy{}, nil
	}

	// Pipeline GET for all proxy keys.
	pipe := s.rdb.Pipeline()
	cmds := make(map[string]*redis.StringCmd, len(ids))
	for _, id := range ids {
		cmds[id] = pipe.Get(s.ctx, proxyKey(id))
	}
	_, err = pipe.Exec(s.ctx)
	if err != nil && err != redis.Nil {
		return nil, fmt.Errorf("redis pipeline get proxies: %w", err)
	}

	proxies := make([]*model.Proxy, 0, len(ids))
	for _, id := range ids {
		data, err := cmds[id].Result()
		if err == redis.Nil {
			continue
		}
		if err != nil {
			continue
		}
		p, err := unmarshalJSON[model.Proxy](data)
		if err != nil {
			continue
		}
		// Attach history.
		history, _ := s.GetHistory(id)
		p.History = history
		proxies = append(proxies, p)
	}
	return proxies, nil
}

func (s *RedisStore) GetAllProxyIDs() ([]string, error) {
	return s.rdb.SMembers(s.ctx, keyProxies).Result()
}

func (s *RedisStore) DeleteAllProxies() (int, error) {
	ids, err := s.rdb.SMembers(s.ctx, keyProxies).Result()
	if err != nil {
		return 0, err
	}
	if len(ids) == 0 {
		return 0, nil
	}

	pipe := s.rdb.Pipeline()
	for _, id := range ids {
		pipe.Del(s.ctx, proxyKey(id))
		pipe.Del(s.ctx, historyKey(id))
	}
	pipe.Del(s.ctx, keyProxies)
	_, err = pipe.Exec(s.ctx)
	return len(ids), err
}

// ---------------------------------------------------------------------------
// Proxy History
// ---------------------------------------------------------------------------

func (s *RedisStore) AppendHistory(proxyID string, result model.ProbeResult) error {
	data, err := marshalJSON(result)
	if err != nil {
		return err
	}
	return s.rdb.RPush(s.ctx, historyKey(proxyID), data).Err()
}

func (s *RedisStore) GetHistory(proxyID string) ([]model.ProbeResult, error) {
	items, err := s.rdb.LRange(s.ctx, historyKey(proxyID), 0, -1).Result()
	if err != nil {
		return nil, fmt.Errorf("redis lrange history %s: %w", proxyID, err)
	}
	results := make([]model.ProbeResult, 0, len(items))
	for _, item := range items {
		r, err := unmarshalJSON[model.ProbeResult](item)
		if err != nil {
			continue
		}
		results = append(results, *r)
	}
	return results, nil
}

// ---------------------------------------------------------------------------
// Alerts
// ---------------------------------------------------------------------------

func (s *RedisStore) AddAlert(a *model.Alert) error {
	data, err := marshalJSON(a)
	if err != nil {
		return err
	}
	pipe := s.rdb.Pipeline()
	pipe.Set(s.ctx, alertKey(a.AlertID), data, 0)
	pipe.RPush(s.ctx, keyAlerts, a.AlertID)
	_, err = pipe.Exec(s.ctx)
	return err
}

func (s *RedisStore) GetAlert(id string) (*model.Alert, error) {
	data, err := s.rdb.Get(s.ctx, alertKey(id)).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("redis get alert %s: %w", id, err)
	}
	return unmarshalJSON[model.Alert](data)
}

func (s *RedisStore) UpdateAlert(a *model.Alert) error {
	data, err := marshalJSON(a)
	if err != nil {
		return err
	}
	return s.rdb.Set(s.ctx, alertKey(a.AlertID), data, 0).Err()
}

func (s *RedisStore) GetAllAlerts() ([]*model.Alert, error) {
	alertIDs, err := s.rdb.LRange(s.ctx, keyAlerts, 0, -1).Result()
	if err != nil {
		return nil, fmt.Errorf("redis lrange alerts: %w", err)
	}
	if len(alertIDs) == 0 {
		return []*model.Alert{}, nil
	}

	pipe := s.rdb.Pipeline()
	cmds := make([]*redis.StringCmd, len(alertIDs))
	for i, id := range alertIDs {
		cmds[i] = pipe.Get(s.ctx, alertKey(id))
	}
	_, err = pipe.Exec(s.ctx)
	if err != nil && err != redis.Nil {
		return nil, fmt.Errorf("redis pipeline get alerts: %w", err)
	}

	alerts := make([]*model.Alert, 0, len(alertIDs))
	for _, cmd := range cmds {
		data, err := cmd.Result()
		if err != nil {
			continue
		}
		a, err := unmarshalJSON[model.Alert](data)
		if err != nil {
			continue
		}
		alerts = append(alerts, a)
	}
	return alerts, nil
}

func (s *RedisStore) GetActiveAlertID() (string, error) {
	id, err := s.rdb.Get(s.ctx, keyActiveAlertID).Result()
	if err == redis.Nil {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("redis get active_alert_id: %w", err)
	}
	return id, nil
}

func (s *RedisStore) SetActiveAlertID(id string) error {
	return s.rdb.Set(s.ctx, keyActiveAlertID, id, 0).Err()
}

func (s *RedisStore) ClearActiveAlertID() error {
	return s.rdb.Del(s.ctx, keyActiveAlertID).Err()
}

// ---------------------------------------------------------------------------
// Webhooks
// ---------------------------------------------------------------------------

func (s *RedisStore) AddWebhook(wh *model.Webhook) error {
	data, err := marshalJSON(wh)
	if err != nil {
		return err
	}
	pipe := s.rdb.Pipeline()
	pipe.Set(s.ctx, webhookKey(wh.ID), data, 0)
	pipe.SAdd(s.ctx, keyWebhooks, wh.ID)
	_, err = pipe.Exec(s.ctx)
	return err
}

func (s *RedisStore) GetAllWebhooks() ([]*model.Webhook, error) {
	ids, err := s.rdb.SMembers(s.ctx, keyWebhooks).Result()
	if err != nil {
		return nil, fmt.Errorf("redis smembers webhooks: %w", err)
	}
	if len(ids) == 0 {
		return []*model.Webhook{}, nil
	}

	pipe := s.rdb.Pipeline()
	cmds := make([]*redis.StringCmd, len(ids))
	for i, id := range ids {
		cmds[i] = pipe.Get(s.ctx, webhookKey(id))
	}
	_, err = pipe.Exec(s.ctx)
	if err != nil && err != redis.Nil {
		return nil, fmt.Errorf("redis pipeline get webhooks: %w", err)
	}

	webhooks := make([]*model.Webhook, 0, len(ids))
	for _, cmd := range cmds {
		data, err := cmd.Result()
		if err != nil {
			continue
		}
		wh, err := unmarshalJSON[model.Webhook](data)
		if err != nil {
			continue
		}
		webhooks = append(webhooks, wh)
	}
	return webhooks, nil
}

// ---------------------------------------------------------------------------
// Integrations
// ---------------------------------------------------------------------------

func (s *RedisStore) AddIntegration(ig *model.Integration) error {
	data, err := marshalJSON(ig)
	if err != nil {
		return err
	}
	pipe := s.rdb.Pipeline()
	pipe.Set(s.ctx, integrationKey(ig.ID), data, 0)
	pipe.SAdd(s.ctx, keyIntegrations, ig.ID)
	_, err = pipe.Exec(s.ctx)
	return err
}

func (s *RedisStore) GetAllIntegrations() ([]*model.Integration, error) {
	ids, err := s.rdb.SMembers(s.ctx, keyIntegrations).Result()
	if err != nil {
		return nil, fmt.Errorf("redis smembers integrations: %w", err)
	}
	if len(ids) == 0 {
		return []*model.Integration{}, nil
	}

	pipe := s.rdb.Pipeline()
	cmds := make([]*redis.StringCmd, len(ids))
	for i, id := range ids {
		cmds[i] = pipe.Get(s.ctx, integrationKey(id))
	}
	_, err = pipe.Exec(s.ctx)
	if err != nil && err != redis.Nil {
		return nil, fmt.Errorf("redis pipeline get integrations: %w", err)
	}

	integrations := make([]*model.Integration, 0, len(ids))
	for _, cmd := range cmds {
		data, err := cmd.Result()
		if err != nil {
			continue
		}
		ig, err := unmarshalJSON[model.Integration](data)
		if err != nil {
			continue
		}
		integrations = append(integrations, ig)
	}
	return integrations, nil
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

func (s *RedisStore) Ping() error {
	return s.rdb.Ping(s.ctx).Err()
}

func (s *RedisStore) Close() error {
	return s.rdb.Close()
}
