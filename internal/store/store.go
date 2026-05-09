// Package store provides the thread-safe, in-memory state for the proxy
// monitor service. All mutations go through the exported Store methods which
// hold the appropriate lock.
package store

import (
	"sync"

	"proxy-monitor/internal/model"
)

// Store is the central thread-safe in-memory state for the entire application.
type Store struct {
	Mu            sync.RWMutex
	Config        *model.Config
	Proxies       map[string]*model.Proxy
	Alerts        []*model.Alert
	AlertsByID    map[string]*model.Alert
	ActiveAlertID     string
	Webhooks          map[string]*model.Webhook
	Integrations      map[string]*model.Integration
	WebhookDeliveries int
}

// New creates and returns an initialised Store with sensible defaults.
func New() *Store {
	return &Store{
		Config:       &model.Config{CheckIntervalSeconds: 30, RequestTimeoutMs: 5000},
		Proxies:      make(map[string]*model.Proxy),
		Alerts:       make([]*model.Alert, 0),
		AlertsByID:   make(map[string]*model.Alert),
		Webhooks:     make(map[string]*model.Webhook),
		Integrations: make(map[string]*model.Integration),
	}
}
