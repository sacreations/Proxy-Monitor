// Package monitor implements the background polling engine and global alert
// state machine. It probes every registered proxy on each tick and evaluates
// whether to fire or resolve the single global alert.
package monitor

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"proxy-monitor/internal/model"
	"proxy-monitor/internal/store"
)

// Monitor encapsulates the background poller and alert evaluator.
type Monitor struct {
	store   store.Store
	restart chan struct{}
}

// New creates a Monitor bound to the given store.
func New(s store.Store) *Monitor {
	return &Monitor{
		store:   s,
		restart: make(chan struct{}, 1),
	}
}

// Restart signals the poller to pick up a new config immediately.
func (m *Monitor) Restart() {
	select {
	case m.restart <- struct{}{}:
	default:
	}
}

// ---------------------------------------------------------------------------
// Poll loop
// ---------------------------------------------------------------------------

// Run starts the continuous polling loop. Blocks forever — call in a goroutine.
func (m *Monitor) Run() {
	for {
		cfg, err := m.store.GetConfig()
		if err != nil {
			log.Printf("poller: failed to read config: %v", err)
			time.Sleep(5 * time.Second)
			continue
		}
		interval := time.Duration(cfg.CheckIntervalSeconds) * time.Second

		log.Printf("⏱️  poller started with interval %v", interval)
		ticker := time.NewTicker(interval)

		func() {
			for {
				select {
				case <-ticker.C:
					m.runCycle()
				case <-m.restart:
					ticker.Stop()
					log.Println("🔄 poller restarting with new config")
					return
				}
			}
		}()
	}
}

// runCycle probes every proxy concurrently, then evaluates alerts.
func (m *Monitor) runCycle() {
	ids, err := m.store.GetAllProxyIDs()
	if err != nil {
		log.Printf("poll cycle: failed to get proxy IDs: %v", err)
		return
	}
	if len(ids) == 0 {
		return
	}

	cfg, err := m.store.GetConfig()
	if err != nil {
		log.Printf("poll cycle: failed to get config: %v", err)
		return
	}
	timeout := time.Duration(cfg.RequestTimeoutMs) * time.Millisecond

	log.Printf("🔍 polling %d proxies", len(ids))

	var wg sync.WaitGroup
	for _, id := range ids {
		wg.Add(1)
		go func(pid string) {
			defer wg.Done()
			m.probe(pid, timeout)
		}(id)
	}
	wg.Wait()

	m.evaluateGlobalAlert()
}

// ---------------------------------------------------------------------------
// Probe
// ---------------------------------------------------------------------------

func (m *Monitor) probe(proxyID string, timeout time.Duration) {
	p, err := m.store.GetProxy(proxyID)
	if err != nil || p == nil {
		return
	}
	proxyURL := p.URL

	client := &http.Client{Timeout: timeout}
	start := time.Now()

	result := model.ProbeResult{Timestamp: start.UTC()}

	resp, err := client.Get(proxyURL)
	result.Latency = time.Since(start).Milliseconds()

	if err != nil {
		result.StatusCode = 0
		result.Result = model.StatusDown
		result.Error = err.Error()
	} else {
		resp.Body.Close()
		result.StatusCode = resp.StatusCode
		switch {
		case resp.StatusCode >= 200 && resp.StatusCode < 300:
			result.Result = model.StatusUp
		case resp.StatusCode >= 500:
			result.Result = model.StatusDown
			result.Error = fmt.Sprintf("server error: %d", resp.StatusCode)
		default:
			result.Result = model.StatusDown
			result.Error = fmt.Sprintf("non-2xx: %d", resp.StatusCode)
		}
	}

	// Append history to Redis.
	if err := m.store.AppendHistory(proxyID, result); err != nil {
		log.Printf("probe: failed to append history for %s: %v", proxyID, err)
	}

	// Update proxy metadata.
	now := result.Timestamp
	p.Status = result.Result
	p.LastCheckedAt = &now
	p.UpdatedAt = now

	// Recount from stored history for accuracy.
	history, err := m.store.GetHistory(proxyID)
	if err != nil {
		log.Printf("probe: failed to get history for %s: %v", proxyID, err)
		return
	}
	p.TotalChecks = len(history)

	upCount := 0
	for _, h := range history {
		if h.Result == model.StatusUp {
			upCount++
		}
	}
	if p.TotalChecks > 0 {
		p.UptimePercentage = float64(upCount) / float64(p.TotalChecks) * 100.0
	}

	consec := 0
	for i := len(history) - 1; i >= 0; i-- {
		if history[i].Result == model.StatusDown {
			consec++
		} else {
			break
		}
	}
	p.ConsecutiveFailures = consec

	if err := m.store.SetProxy(p); err != nil {
		log.Printf("probe: failed to update proxy %s: %v", proxyID, err)
	}
}

// ---------------------------------------------------------------------------
// Global Alert State Machine
// ---------------------------------------------------------------------------

func (m *Monitor) evaluateGlobalAlert() {
	proxies, err := m.store.GetAllProxies()
	if err != nil {
		log.Printf("alert eval: failed to get proxies: %v", err)
		return
	}

	total := len(proxies)
	if total == 0 {
		return
	}

	downCount := 0
	failedIDs := make([]string, 0)
	for _, p := range proxies {
		if p.Status == model.StatusDown {
			downCount++
			failedIDs = append(failedIDs, p.ID)
		}
	}
	failureRate := float64(downCount) / float64(total)

	activeAlertID, _ := m.store.GetActiveAlertID()

	if failureRate >= model.FailureThreshold {
		if activeAlertID == "" {
			alertID := generateAlertID()
			now := time.Now().UTC()
			alert := &model.Alert{
				AlertID:        alertID,
				Status:         model.AlertActive,
				FailureRate:    failureRate,
				TotalProxies:   total,
				FailedProxies:  downCount,
				FailedProxyIDs: failedIDs,
				Threshold:      model.FailureThreshold,
				FiredAt:        now,
				Message:        fmt.Sprintf("failure rate %.2f%% >= %.2f%% threshold", failureRate*100, model.FailureThreshold*100),
			}
			if err := m.store.AddAlert(alert); err != nil {
				log.Printf("alert eval: failed to add alert: %v", err)
				return
			}
			if err := m.store.SetActiveAlertID(alertID); err != nil {
				log.Printf("alert eval: failed to set active alert: %v", err)
				return
			}
			log.Printf("🚨 ALERT FIRED alert=%s rate=%.2f%%", alertID, failureRate*100)
			go m.dispatchNotifications(model.EventAlertFired, alert)
		} else {
			// Update existing active alert stats.
			a, err := m.store.GetAlert(activeAlertID)
			if err == nil && a != nil {
				a.FailureRate = failureRate
				a.TotalProxies = total
				a.FailedProxies = downCount
				a.FailedProxyIDs = failedIDs
				_ = m.store.UpdateAlert(a)
			}
		}
	} else {
		if activeAlertID != "" {
			a, err := m.store.GetAlert(activeAlertID)
			if err == nil && a != nil {
				now := time.Now().UTC()
				a.Status = model.AlertResolved
				a.FailureRate = failureRate
				a.TotalProxies = total
				a.FailedProxies = downCount
				a.FailedProxyIDs = failedIDs
				a.ResolvedAt = &now
				_ = m.store.UpdateAlert(a)
				log.Printf("✅ ALERT RESOLVED alert=%s rate=%.2f%%", activeAlertID, failureRate*100)
				go m.dispatchNotifications(model.EventAlertResolved, a)
			}
			_ = m.store.ClearActiveAlertID()
		}
	}
}

func generateAlertID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return fmt.Sprintf("alert_%x", b)
}

// ---------------------------------------------------------------------------
// Notification Dispatch
// ---------------------------------------------------------------------------

func (m *Monitor) dispatchNotifications(event string, alert *model.Alert) {
	webhooks, _ := m.store.GetAllWebhooks()
	integrations, _ := m.store.GetAllIntegrations()

	payload := model.WebhookPayload{
		Event:          event,
		AlertID:        alert.AlertID,
		Status:         alert.Status,
		FailureRate:    alert.FailureRate,
		TotalProxies:   alert.TotalProxies,
		FailedProxies:  alert.FailedProxies,
		FailedProxyIDs: alert.FailedProxyIDs,
		Threshold:      alert.Threshold,
		Message:        alert.Message,
		Timestamp:      time.Now().UTC().Format(time.RFC3339),
	}

	for _, wh := range webhooks {
		go deliverWebhook(wh.URL, payload)
	}
	for _, ig := range integrations {
		switch ig.Type {
		case "slack":
			go deliverSlack(ig.Endpoint, payload)
		case "discord":
			go deliverDiscord(ig.Endpoint, payload)
		}
	}
}

// ---------------------------------------------------------------------------
// Delivery helpers (retry until success on transient errors)
// ---------------------------------------------------------------------------

func isTransient(code int) bool {
	return code == 500 || code == 502 || code == 503 || code == 504
}

func deliverWebhook(url string, payload model.WebhookPayload) {
	body, err := json.Marshal(payload)
	if err != nil {
		log.Printf("webhook marshal error: %v", err)
		return
	}
	deliverWithRetry("webhook", url, body)
}

func deliverSlack(endpoint string, payload model.WebhookPayload) {
	emoji := "🚨"
	color := "#e74c3c"
	if payload.Event == model.EventAlertResolved {
		emoji = "✅"
		color = "#2ecc71"
	}
	slack := model.SlackPayload{
		Text: fmt.Sprintf("%s Proxy Monitor Alert", emoji),
		Attachments: []model.SlackAttachment{
			{
				Color: color,
				Title: fmt.Sprintf("Alert %s — %s", payload.AlertID, payload.Status),
				Text:  payload.Message,
				Fields: []model.SlackField{
					{Title: "Failure Rate", Value: fmt.Sprintf("%.2f%%", payload.FailureRate*100), Short: true},
					{Title: "Failed Proxies", Value: fmt.Sprintf("%d/%d", payload.FailedProxies, payload.TotalProxies), Short: true},
					{Title: "Event", Value: payload.Event, Short: true},
					{Title: "Timestamp", Value: payload.Timestamp, Short: true},
				},
			},
		},
	}
	body, err := json.Marshal(slack)
	if err != nil {
		log.Printf("slack marshal error: %v", err)
		return
	}
	deliverWithRetry("slack", endpoint, body)
}

func deliverDiscord(endpoint string, payload model.WebhookPayload) {
	color := 0xe74c3c
	title := "🚨 Alert Fired"
	if payload.Event == model.EventAlertResolved {
		color = 0x2ecc71
		title = "✅ Alert Resolved"
	}
	discord := model.DiscordPayload{
		Content: "Proxy Monitor Alert",
		Embeds: []model.DiscordEmbed{
			{
				Title:       title,
				Description: payload.Message,
				Color:       color,
				Fields: []model.DiscordEmbedField{
					{Name: "Alert ID", Value: payload.AlertID, Inline: true},
					{Name: "Status", Value: payload.Status, Inline: true},
					{Name: "Failure Rate", Value: fmt.Sprintf("%.2f%%", payload.FailureRate*100), Inline: true},
					{Name: "Failed Proxies", Value: fmt.Sprintf("%d/%d", payload.FailedProxies, payload.TotalProxies), Inline: true},
				},
				Timestamp: payload.Timestamp,
			},
		},
	}
	body, err := json.Marshal(discord)
	if err != nil {
		log.Printf("discord marshal error: %v", err)
		return
	}
	deliverWithRetry("discord", endpoint, body)
}

func deliverWithRetry(label, endpoint string, body []byte) {
	backoff := 1 * time.Second
	maxBackoff := 30 * time.Second

	for attempt := 0; ; attempt++ {
		if attempt > 0 {
			log.Printf("%s retry #%d to %s (backoff %v)", label, attempt, endpoint, backoff)
			time.Sleep(backoff)
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}

		resp, err := http.Post(endpoint, "application/json", bytes.NewReader(body))
		if err != nil {
			log.Printf("%s delivery error to %s: %v", label, endpoint, err)
			continue
		}
		resp.Body.Close()

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			log.Printf("✅ %s delivered to %s", label, endpoint)
			return
		}
		if isTransient(resp.StatusCode) {
			log.Printf("⚠️  %s transient failure to %s (status %d)", label, endpoint, resp.StatusCode)
			continue
		}

		log.Printf("❌ %s rejected by %s (status %d), not retrying", label, endpoint, resp.StatusCode)
		return
	}
}
