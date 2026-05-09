// Package store defines the Store interface and provides a Redis-backed
// implementation for the proxy monitor service.
package store

import (
	"proxy-monitor/internal/model"
)

// Store is the abstraction over all state management. Both the handler
// and monitor packages depend only on this interface, making the storage
// backend swappable (Redis, in-memory, etc.).
type Store interface {
	// ── Config ─────────────────────────────────────────────────────────
	GetConfig() (*model.Config, error)
	SetConfig(cfg *model.Config) error

	// ── Proxies ────────────────────────────────────────────────────────
	GetProxy(id string) (*model.Proxy, error)
	SetProxy(p *model.Proxy) error
	GetAllProxies() ([]*model.Proxy, error)
	GetAllProxyIDs() ([]string, error)
	DeleteAllProxies() (int, error)

	// ── Proxy History ──────────────────────────────────────────────────
	AppendHistory(proxyID string, result model.ProbeResult) error
	GetHistory(proxyID string) ([]model.ProbeResult, error)

	// ── Alerts ─────────────────────────────────────────────────────────
	AddAlert(a *model.Alert) error
	GetAlert(id string) (*model.Alert, error)
	UpdateAlert(a *model.Alert) error
	GetAllAlerts() ([]*model.Alert, error)
	GetActiveAlertID() (string, error)
	SetActiveAlertID(id string) error
	ClearActiveAlertID() error

	// ── Webhooks ───────────────────────────────────────────────────────
	AddWebhook(wh *model.Webhook) error
	GetAllWebhooks() ([]*model.Webhook, error)

	// ── Integrations ───────────────────────────────────────────────────
	AddIntegration(ig *model.Integration) error
	GetAllIntegrations() ([]*model.Integration, error)

	// ── Lifecycle ──────────────────────────────────────────────────────
	Ping() error
	Close() error
}
