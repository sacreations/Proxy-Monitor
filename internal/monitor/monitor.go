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
	store   *store.Store
	restart chan struct{}
}

// New creates a Monitor bound to the given store.
func New(s *store.Store) *Monitor {
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
		m.store.Mu.RLock()
		interval := time.Duration(m.store.Config.CheckIntervalSeconds) * time.Second
		m.store.Mu.RUnlock()

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
	m.store.Mu.RLock()
	ids := make([]string, 0, len(m.store.Proxies))
	for id := range m.store.Proxies {
		ids = append(ids, id)
	}
	timeoutMs := m.store.Config.RequestTimeoutMs
	m.store.Mu.RUnlock()

	if len(ids) == 0 {
		return
	}

	log.Printf("🔍 polling %d proxies", len(ids))
	timeout := time.Duration(timeoutMs) * time.Millisecond

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
	m.store.Mu.RLock()
	p, ok := m.store.Proxies[proxyID]
	if !ok {
		m.store.Mu.RUnlock()
		return
	}
	proxyURL := p.URL
	m.store.Mu.RUnlock()

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

	now := result.Timestamp
	m.store.Mu.Lock()
	p, ok = m.store.Proxies[proxyID]
	if ok {
		p.History = append(p.History, result)
		p.Status = result.Result
		p.LastCheckedAt = &now
		p.UpdatedAt = now
		p.TotalChecks = len(p.History)

		upCount := 0
		for _, h := range p.History {
			if h.Result == model.StatusUp {
				upCount++
			}
		}
		p.UptimePercentage = float64(upCount) / float64(p.TotalChecks) * 100.0

		consec := 0
		for i := len(p.History) - 1; i >= 0; i-- {
			if p.History[i].Result == model.StatusDown {
				consec++
			} else {
				break
			}
		}
		p.ConsecutiveFailures = consec
	}
	m.store.Mu.Unlock()
}

// ---------------------------------------------------------------------------
// Global Alert State Machine
// ---------------------------------------------------------------------------

func (m *Monitor) evaluateGlobalAlert() {
	m.store.Mu.Lock()
	defer m.store.Mu.Unlock()

	total := len(m.store.Proxies)
	if total == 0 {
		return
	}

	downCount := 0
	failedIDs := make([]string, 0)
	for _, p := range m.store.Proxies {
		if p.Status == model.StatusDown {
			downCount++
			failedIDs = append(failedIDs, p.ID)
		}
	}
	failureRate := float64(downCount) / float64(total)

	if failureRate >= model.FailureThreshold {
		if m.store.ActiveAlertID == "" {
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
			m.store.Alerts = append(m.store.Alerts, alert)
			m.store.AlertsByID[alertID] = alert
			m.store.ActiveAlertID = alertID
			log.Printf("🚨 ALERT FIRED alert=%s rate=%.2f%%", alertID, failureRate*100)
			go m.dispatchNotifications(model.EventAlertFired, alert)
		} else {
			if a, ok := m.store.AlertsByID[m.store.ActiveAlertID]; ok {
				a.FailureRate = failureRate
				a.TotalProxies = total
				a.FailedProxies = downCount
				a.FailedProxyIDs = failedIDs
			}
		}
	} else {
		if m.store.ActiveAlertID != "" {
			if a, ok := m.store.AlertsByID[m.store.ActiveAlertID]; ok {
				now := time.Now().UTC()
				a.Status = model.AlertResolved
				a.FailureRate = failureRate
				a.TotalProxies = total
				a.FailedProxies = downCount
				a.FailedProxyIDs = failedIDs
				a.ResolvedAt = &now
				log.Printf("✅ ALERT RESOLVED alert=%s rate=%.2f%%", m.store.ActiveAlertID, failureRate*100)
				go m.dispatchNotifications(model.EventAlertResolved, a)
			}
			m.store.ActiveAlertID = ""
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
	m.store.Mu.RLock()
	webhooks := make([]*model.Webhook, 0, len(m.store.Webhooks))
	for _, wh := range m.store.Webhooks {
		webhooks = append(webhooks, wh)
	}
	integrations := make([]*model.Integration, 0, len(m.store.Integrations))
	for _, ig := range m.store.Integrations {
		integrations = append(integrations, ig)
	}
	m.store.Mu.RUnlock()

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
