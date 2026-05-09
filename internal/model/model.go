// Package model defines the domain types and JSON contracts for the proxy
// monitor service. All structs here carry the exact json tags the black-box
// evaluator expects.
package model

import "time"

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

// Config holds the polling cadence set via POST /config.
type Config struct {
	CheckIntervalSeconds int `json:"check_interval_seconds"`
	RequestTimeoutMs     int `json:"request_timeout_ms"`
}

// ---------------------------------------------------------------------------
// Proxy
// ---------------------------------------------------------------------------

// Proxy represents a monitored proxy endpoint.
type Proxy struct {
	ID                  string        `json:"id"`
	URL                 string        `json:"url"`
	Status              string        `json:"status"` // "pending", "up", "down"
	LastCheckedAt       *time.Time    `json:"last_checked_at"`
	TotalChecks         int           `json:"total_checks"`
	UptimePercentage    float64       `json:"uptime_percentage"`
	ConsecutiveFailures int           `json:"consecutive_failures"`
	CreatedAt           time.Time     `json:"created_at"`
	UpdatedAt           time.Time     `json:"updated_at"`
	History             []ProbeResult `json:"history"`
}

// ProbeResult stores the outcome of a single health check probe.
type ProbeResult struct {
	Timestamp  time.Time `json:"timestamp"`
	StatusCode int       `json:"status_code"`
	Latency    int64     `json:"latency_ms"`
	Result     string    `json:"result"` // "up" | "down"
	Error      string    `json:"error,omitempty"`
}

// ProxyInput is the expected JSON body for POST /proxies.
type ProxyInput struct {
	URLs    []string `json:"urls"`
	Replace bool     `json:"replace"`
}

// ProxiesListResponse is the JSON response for GET /proxies.
type ProxiesListResponse struct {
	Total       int      `json:"total"`
	Up          int      `json:"up"`
	Down        int      `json:"down"`
	FailureRate float64  `json:"failure_rate"`
	Proxies     []*Proxy `json:"proxies"`
}

// ProxyDetailResponse is the JSON response for GET /proxies/{id}.
type ProxyDetailResponse struct {
	ID                  string        `json:"id"`
	URL                 string        `json:"url"`
	Status              string        `json:"status"`
	LastCheckedAt       *time.Time    `json:"last_checked_at"`
	TotalChecks         int           `json:"total_checks"`
	UptimePercentage    float64       `json:"uptime_percentage"`
	ConsecutiveFailures int           `json:"consecutive_failures"`
	CreatedAt           time.Time     `json:"created_at"`
	UpdatedAt           time.Time     `json:"updated_at"`
	History             []ProbeResult `json:"history"`
}

// ---------------------------------------------------------------------------
// Alert
// ---------------------------------------------------------------------------

// Alert represents an alert fired when the global failure rate breaches the
// threshold. Only ONE alert can be active at a time.
type Alert struct {
	AlertID        string     `json:"alert_id"`
	Status         string     `json:"status"` // "active", "resolved"
	FailureRate    float64    `json:"failure_rate"`
	TotalProxies   int        `json:"total_proxies"`
	FailedProxies  int        `json:"failed_proxies"`
	FailedProxyIDs []string   `json:"failed_proxy_ids"`
	Threshold      float64    `json:"threshold"`
	FiredAt        time.Time  `json:"fired_at"`
	ResolvedAt     *time.Time `json:"resolved_at,omitempty"`
	Message        string     `json:"message"`
}

// ---------------------------------------------------------------------------
// Webhook
// ---------------------------------------------------------------------------

// Webhook is a registered receiver for alert notifications.
type Webhook struct {
	ID  string `json:"id"`
	URL string `json:"url"`
}

// WebhookInput is the expected JSON body for POST /webhooks.
type WebhookInput struct {
	URL string `json:"url"`
}

// WebhookPayload is the JSON body delivered to webhook receivers.
type WebhookPayload struct {
	Event          string   `json:"event"` // "alert.fired" | "alert.resolved"
	AlertID        string   `json:"alert_id"`
	Status         string   `json:"status"`
	FailureRate    float64  `json:"failure_rate"`
	TotalProxies   int      `json:"total_proxies"`
	FailedProxies  int      `json:"failed_proxies"`
	FailedProxyIDs []string `json:"failed_proxy_ids"`
	Threshold      float64  `json:"threshold"`
	Message        string   `json:"message"`
	Timestamp      string   `json:"timestamp"`
}

// ---------------------------------------------------------------------------
// Integration (Slack / Discord)
// ---------------------------------------------------------------------------

// Integration represents a Slack or Discord integration.
type Integration struct {
	ID       string `json:"id"`
	Type     string `json:"type"` // "slack" | "discord"
	Endpoint string `json:"endpoint"`
}

// IntegrationInput is the expected JSON body for POST /integrations.
type IntegrationInput struct {
	Type     string `json:"type"`
	Endpoint string `json:"endpoint"`
}

// SlackPayload follows the Slack Incoming Webhook format.
type SlackPayload struct {
	Text        string            `json:"text"`
	Attachments []SlackAttachment `json:"attachments,omitempty"`
}

// SlackAttachment is a single attachment block in a Slack message.
type SlackAttachment struct {
	Color  string       `json:"color"`
	Title  string       `json:"title"`
	Text   string       `json:"text"`
	Fields []SlackField `json:"fields,omitempty"`
}

// SlackField is a key-value field inside a Slack attachment.
type SlackField struct {
	Title string `json:"title"`
	Value string `json:"value"`
	Short bool   `json:"short"`
}

// DiscordPayload follows the Discord Webhook format.
type DiscordPayload struct {
	Content string         `json:"content"`
	Embeds  []DiscordEmbed `json:"embeds,omitempty"`
}

// DiscordEmbed is a single rich embed in a Discord webhook message.
type DiscordEmbed struct {
	Title       string              `json:"title"`
	Description string              `json:"description"`
	Color       int                 `json:"color"`
	Fields      []DiscordEmbedField `json:"fields,omitempty"`
	Timestamp   string              `json:"timestamp,omitempty"`
}

// DiscordEmbedField is a key-value field in a Discord embed.
type DiscordEmbedField struct {
	Name   string `json:"name"`
	Value  string `json:"value"`
	Inline bool   `json:"inline"`
}

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

// Metrics is the response for GET /metrics.
type Metrics struct {
	TotalChecks    int     `json:"total_checks"`
	TotalProxies   int     `json:"total_proxies"`
	ProxiesUp      int     `json:"proxies_up"`
	ProxiesDown    int     `json:"proxies_down"`
	ProxiesPending int     `json:"proxies_pending"`
	ActiveAlerts   int     `json:"active_alerts"`
	ResolvedAlerts int     `json:"resolved_alerts"`
	TotalAlerts    int     `json:"total_alerts"`
	FailureRate    float64 `json:"failure_rate"`
	WebhookCount   int     `json:"webhook_count"`
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const (
	// FailureThreshold is the ratio at which an alert is triggered (>= 0.20).
	FailureThreshold = 0.20

	// StatusPending is used before the first probe.
	StatusPending = "pending"
	// StatusUp indicates a healthy proxy.
	StatusUp = "up"
	// StatusDown indicates an unhealthy proxy.
	StatusDown = "down"

	// AlertActive marks an alert that is currently firing.
	AlertActive = "active"
	// AlertResolved marks an alert that has recovered.
	AlertResolved = "resolved"

	// EventAlertFired is the webhook event name when an alert fires.
	EventAlertFired = "alert.fired"
	// EventAlertResolved is the webhook event name when an alert resolves.
	EventAlertResolved = "alert.resolved"
)
