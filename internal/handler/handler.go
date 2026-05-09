// Package handler provides the HTTP route wiring and all JSON endpoint
// handlers for the proxy monitor service.
package handler

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"proxy-monitor/internal/model"
	"proxy-monitor/internal/monitor"
	"proxy-monitor/internal/store"
)

// NewRouter creates and returns a fully wired http.Handler.
func NewRouter(s store.Store, mon *monitor.Monitor) http.Handler {
	h := &handlers{store: s, monitor: mon}

	mux := http.NewServeMux()

	mux.HandleFunc("GET /health", h.health)

	mux.HandleFunc("POST /config", h.configSet)
	mux.HandleFunc("GET /config", h.configGet)

	mux.HandleFunc("POST /proxies", h.proxiesCreate)
	mux.HandleFunc("GET /proxies", h.proxiesList)
	mux.HandleFunc("GET /proxies/{id}", h.proxyGet)
	mux.HandleFunc("GET /proxies/{id}/history", h.proxyHistory)
	mux.HandleFunc("DELETE /proxies", h.proxiesDelete)

	mux.HandleFunc("GET /alerts", h.alertsList)

	mux.HandleFunc("POST /webhooks", h.webhookCreate)

	mux.HandleFunc("POST /integrations", h.integrationCreate)

	mux.HandleFunc("GET /metrics", h.metrics)

	return withLogging(mux)
}

// ---------------------------------------------------------------------------
// Internal handler group
// ---------------------------------------------------------------------------

type handlers struct {
	store   store.Store
	monitor *monitor.Monitor
}

// ---------------------------------------------------------------------------
// Middleware
// ---------------------------------------------------------------------------

func withLogging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Printf("%-6s %s", r.Method, r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		next.ServeHTTP(w, r)
	})
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("json encode error: %v", err)
	}
}

func readJSON(r *http.Request, dst any) error {
	return json.NewDecoder(r.Body).Decode(dst)
}

func errResp(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// extractProxyID derives the proxy ID as the final path segment of the URL.
func extractProxyID(rawURL string) string {
	rawURL = strings.TrimRight(rawURL, "/")
	if idx := strings.LastIndex(rawURL, "/"); idx >= 0 {
		return rawURL[idx+1:]
	}
	return rawURL
}

// ---------------------------------------------------------------------------
// Health
// ---------------------------------------------------------------------------

func (h *handlers) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

func (h *handlers) configSet(w http.ResponseWriter, r *http.Request) {
	var input model.Config
	if err := readJSON(r, &input); err != nil {
		errResp(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	if input.CheckIntervalSeconds <= 0 {
		errResp(w, http.StatusBadRequest, "check_interval_seconds must be > 0")
		return
	}
	if input.RequestTimeoutMs <= 0 {
		errResp(w, http.StatusBadRequest, "request_timeout_ms must be > 0")
		return
	}

	if err := h.store.SetConfig(&input); err != nil {
		errResp(w, http.StatusInternalServerError, "failed to save config: "+err.Error())
		return
	}

	h.monitor.Restart()

	writeJSON(w, http.StatusOK, map[string]any{
		"message":                "configuration updated",
		"check_interval_seconds": input.CheckIntervalSeconds,
		"request_timeout_ms":     input.RequestTimeoutMs,
	})
}

func (h *handlers) configGet(w http.ResponseWriter, _ *http.Request) {
	cfg, err := h.store.GetConfig()
	if err != nil {
		errResp(w, http.StatusInternalServerError, "failed to read config: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, cfg)
}

// ---------------------------------------------------------------------------
// Proxies
// ---------------------------------------------------------------------------

func (h *handlers) proxiesCreate(w http.ResponseWriter, r *http.Request) {
	var input model.ProxyInput
	if err := readJSON(r, &input); err != nil {
		errResp(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	if len(input.URLs) == 0 {
		errResp(w, http.StatusBadRequest, "urls list must not be empty")
		return
	}

	now := time.Now().UTC()

	// If replace is true, clear the existing proxy pool.
	if input.Replace {
		if _, err := h.store.DeleteAllProxies(); err != nil {
			errResp(w, http.StatusInternalServerError, "failed to clear proxies: "+err.Error())
			return
		}
	}

	var created []*model.Proxy
	for _, u := range input.URLs {
		u = strings.TrimSpace(u)
		if u == "" {
			continue
		}
		id := extractProxyID(u)
		// Check if already exists.
		existing, err := h.store.GetProxy(id)
		if err != nil {
			errResp(w, http.StatusInternalServerError, "failed to check proxy: "+err.Error())
			return
		}
		if existing != nil {
			continue // deduplicate
		}
		p := &model.Proxy{
			ID:        id,
			URL:       u,
			Status:    model.StatusPending,
			CreatedAt: now,
			UpdatedAt: now,
			History:   []model.ProbeResult{},
		}
		if err := h.store.SetProxy(p); err != nil {
			errResp(w, http.StatusInternalServerError, "failed to save proxy: "+err.Error())
			return
		}
		created = append(created, p)
	}

	writeJSON(w, http.StatusCreated, map[string]any{
		"message": fmt.Sprintf("ingested %d new proxies", len(created)),
		"proxies": created,
	})
}

func (h *handlers) proxiesList(w http.ResponseWriter, _ *http.Request) {
	proxies, err := h.store.GetAllProxies()
	if err != nil {
		errResp(w, http.StatusInternalServerError, "failed to list proxies: "+err.Error())
		return
	}

	up, down := 0, 0
	for _, p := range proxies {
		switch p.Status {
		case model.StatusUp:
			up++
		case model.StatusDown:
			down++
		}
	}
	total := len(proxies)

	var failureRate float64
	if total > 0 {
		failureRate = float64(down) / float64(total)
	}

	writeJSON(w, http.StatusOK, model.ProxiesListResponse{
		Total:       total,
		Up:          up,
		Down:        down,
		FailureRate: failureRate,
		Proxies:     proxies,
	})
}

func (h *handlers) proxyGet(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	p, err := h.store.GetProxy(id)
	if err != nil {
		errResp(w, http.StatusInternalServerError, "failed to get proxy: "+err.Error())
		return
	}
	if p == nil {
		errResp(w, http.StatusNotFound, "proxy not found: "+id)
		return
	}

	writeJSON(w, http.StatusOK, model.ProxyDetailResponse{
		ID:                  p.ID,
		URL:                 p.URL,
		Status:              p.Status,
		LastCheckedAt:       p.LastCheckedAt,
		TotalChecks:         p.TotalChecks,
		UptimePercentage:    p.UptimePercentage,
		ConsecutiveFailures: p.ConsecutiveFailures,
		CreatedAt:           p.CreatedAt,
		UpdatedAt:           p.UpdatedAt,
		History:             p.History,
	})
}

func (h *handlers) proxyHistory(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	p, err := h.store.GetProxy(id)
	if err != nil {
		errResp(w, http.StatusInternalServerError, "failed to get proxy: "+err.Error())
		return
	}
	if p == nil {
		errResp(w, http.StatusNotFound, "proxy not found: "+id)
		return
	}

	history, err := h.store.GetHistory(id)
	if err != nil {
		errResp(w, http.StatusInternalServerError, "failed to get history: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, history)
}

// handleProxiesDelete clears the proxy pool but does NOT delete alert history.
func (h *handlers) proxiesDelete(w http.ResponseWriter, _ *http.Request) {
	count, err := h.store.DeleteAllProxies()
	if err != nil {
		errResp(w, http.StatusInternalServerError, "failed to delete proxies: "+err.Error())
		return
	}
	_ = h.store.ClearActiveAlertID()

	writeJSON(w, http.StatusOK, map[string]any{
		"message":         "proxy pool cleared",
		"proxies_removed": count,
	})
}

// ---------------------------------------------------------------------------
// Alerts
// ---------------------------------------------------------------------------

func (h *handlers) alertsList(w http.ResponseWriter, _ *http.Request) {
	alerts, err := h.store.GetAllAlerts()
	if err != nil {
		errResp(w, http.StatusInternalServerError, "failed to list alerts: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, alerts)
}

// ---------------------------------------------------------------------------
// Webhooks
// ---------------------------------------------------------------------------

func (h *handlers) webhookCreate(w http.ResponseWriter, r *http.Request) {
	var input model.WebhookInput
	if err := readJSON(r, &input); err != nil {
		errResp(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	if input.URL == "" {
		errResp(w, http.StatusBadRequest, "url is required")
		return
	}

	id := fmt.Sprintf("wh_%d", time.Now().UnixNano())
	wh := &model.Webhook{ID: id, URL: input.URL}

	if err := h.store.AddWebhook(wh); err != nil {
		errResp(w, http.StatusInternalServerError, "failed to save webhook: "+err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, wh)
}

// ---------------------------------------------------------------------------
// Integrations
// ---------------------------------------------------------------------------

func (h *handlers) integrationCreate(w http.ResponseWriter, r *http.Request) {
	var input model.IntegrationInput
	if err := readJSON(r, &input); err != nil {
		errResp(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	if input.Type != "slack" && input.Type != "discord" {
		errResp(w, http.StatusBadRequest, "type must be 'slack' or 'discord'")
		return
	}
	if input.Endpoint == "" {
		errResp(w, http.StatusBadRequest, "endpoint is required")
		return
	}

	id := fmt.Sprintf("int_%d", time.Now().UnixNano())
	ig := &model.Integration{ID: id, Type: input.Type, Endpoint: input.Endpoint}

	if err := h.store.AddIntegration(ig); err != nil {
		errResp(w, http.StatusInternalServerError, "failed to save integration: "+err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, ig)
}

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

func (h *handlers) metrics(w http.ResponseWriter, _ *http.Request) {
	proxies, err := h.store.GetAllProxies()
	if err != nil {
		errResp(w, http.StatusInternalServerError, "failed to read proxies: "+err.Error())
		return
	}

	totalChecks := 0
	up, down, pending := 0, 0, 0
	for _, p := range proxies {
		totalChecks += p.TotalChecks
		switch p.Status {
		case model.StatusUp:
			up++
		case model.StatusDown:
			down++
		case model.StatusPending:
			pending++
		}
	}

	totalProxies := len(proxies)

	alerts, err := h.store.GetAllAlerts()
	if err != nil {
		errResp(w, http.StatusInternalServerError, "failed to read alerts: "+err.Error())
		return
	}

	activeAlerts, resolvedAlerts := 0, 0
	for _, a := range alerts {
		switch a.Status {
		case model.AlertActive:
			activeAlerts++
		case model.AlertResolved:
			resolvedAlerts++
		}
	}

	webhooks, _ := h.store.GetAllWebhooks()

	var failureRate float64
	if totalProxies > 0 {
		failureRate = float64(down) / float64(totalProxies)
	}

	m := model.Metrics{
		TotalChecks:    totalChecks,
		TotalProxies:   totalProxies,
		ProxiesUp:      up,
		ProxiesDown:    down,
		ProxiesPending: pending,
		ActiveAlerts:   activeAlerts,
		ResolvedAlerts: resolvedAlerts,
		TotalAlerts:    len(alerts),
		FailureRate:    failureRate,
		WebhookCount:   len(webhooks),
	}

	writeJSON(w, http.StatusOK, m)
}
