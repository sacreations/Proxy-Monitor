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
			m.runCycle()

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

	result := model.ProbeResult{CheckedAt: start.UTC()}

	resp, err := client.Get(proxyURL)
	result.Latency = time.Since(start).Milliseconds()

	if err != nil {
		result.StatusCode = 0
		result.Status = model.StatusDown
		result.Error = err.Error()
	} else {
		resp.Body.Close()
		result.StatusCode = resp.StatusCode
		switch {
		case resp.StatusCode >= 200 && resp.StatusCode < 300:
			result.Status = model.StatusUp
		case resp.StatusCode >= 500:
			result.Status = model.StatusDown
			result.Error = fmt.Sprintf("server error: %d", resp.StatusCode)
		default:
			result.Status = model.StatusDown
			result.Error = fmt.Sprintf("non-2xx: %d", resp.StatusCode)
		}
	}

	now := result.CheckedAt
	m.store.Mu.Lock()
	p, ok = m.store.Proxies[proxyID]
	if ok {
		p.History = append(p.History, result)
		p.Status = result.Status
		p.LastCheckedAt = &now
		p.UpdatedAt = now
		p.TotalChecks = len(p.History)

		upCount := 0
		for _, h := range p.History {
			if h.Status == model.StatusUp {
				upCount++
			}
		}
		p.UptimePercentage = float64(upCount) / float64(p.TotalChecks) * 100.0

		consec := 0
		for i := len(p.History) - 1; i >= 0; i-- {
			if p.History[i].Status == model.StatusDown {
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
				AlertID:      alertID,
				Status:       model.AlertActive,
				FailureRate:  failureRate,
				Threshold:    model.FailureThreshold,
				TotalProxies: total,
				DownProxies:  downCount,
				DownProxyIDs: failedIDs,
				FiredAt:      now,
				Message:      fmt.Sprintf("failure rate %.2f%% >= %.2f%% threshold", failureRate*100, model.FailureThreshold*100),
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
				a.DownProxies = downCount
				a.DownProxyIDs = failedIDs
			}
		}
	} else {
		if m.store.ActiveAlertID != "" {
			if a, ok := m.store.AlertsByID[m.store.ActiveAlertID]; ok {
				now := time.Now().UTC()
				a.Status = model.AlertResolved
				a.FailureRate = failureRate
				a.TotalProxies = total
				a.DownProxies = downCount
				a.DownProxyIDs = failedIDs
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
	return fmt.Sprintf("alert-%x", b)
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
		Event:   event,
		AlertID: alert.AlertID,
	}

	for _, wh := range webhooks {
		go m.deliverWebhook(wh.URL, payload)
	}
	for _, ig := range integrations {
		go m.deliverIntegration(ig, event, alert)
	}
}

func (m *Monitor) deliverWebhook(url string, payload model.WebhookPayload) {
	body, err := json.Marshal(payload)
	if err != nil {
		log.Printf("webhook marshal error: %v", err)
		return
	}
	extraHeaders := map[string]string{
		"X-ProxyMaze-Event": payload.Event,
	}
	m.deliverWithRetry("webhook", url, body, extraHeaders)
}

func (m *Monitor) deliverIntegration(ig *model.Integration, event string, alert *model.Alert) {
	// Check if this integration subscribed to this event
	wantsEvent := false
	for _, ev := range ig.Events {
		if ev == event {
			wantsEvent = true
			break
		}
	}
	if len(ig.Events) > 0 && !wantsEvent {
		return
	}

	switch ig.Type {
	case "slack":
		m.deliverSlack(ig, event, alert)
	case "discord":
		m.deliverDiscord(ig, event, alert)
	}
}

// ---------------------------------------------------------------------------
// Delivery helpers (retry until success on transient errors)
// ---------------------------------------------------------------------------

func isTransient(code int) bool {
	return code == 429 || code == 500 || code == 502 || code == 503 || code == 504
}

func (m *Monitor) deliverSlack(ig *model.Integration, event string, alert *model.Alert) {
	emoji := "🚨"
	color := "#e74c3c"
	if event == model.EventAlertResolved {
		emoji = "✅"
		color = "#2ecc71"
	}
	
	username := ig.Username
	if username == "" {
		username = "ProxyWatch"
	}

	slack := model.SlackPayload{
		Username: username,
		Text:     fmt.Sprintf("%s Proxy Monitor Alert", emoji),
		Blocks: []any{
			map[string]any{
				"type": "header",
				"text": map[string]any{
					"type": "plain_text",
					"text": fmt.Sprintf("%s Alert %s — %s", emoji, alert.AlertID, alert.Status),
				},
			},
			map[string]any{
				"type": "section",
				"text": map[string]any{
					"type": "mrkdwn",
					"text": alert.Message,
				},
				"fields": []map[string]any{
					{
						"type": "mrkdwn",
						"text": fmt.Sprintf("*Failure Rate*\n%.2f%%", alert.FailureRate*100),
					},
					{
						"type": "mrkdwn",
						"text": fmt.Sprintf("*Failed Proxies*\n%d/%d", alert.DownProxies, alert.TotalProxies),
					},
					{
						"type": "mrkdwn",
						"text": fmt.Sprintf("*Event*\n%s", event),
					},
				},
			},
			map[string]any{
				"type": "context",
				"elements": []map[string]any{
					{
						"type": "mrkdwn",
						"text": "ProxyMaze Background Monitor | <!date^" + fmt.Sprint(time.Now().Unix()) + "^{date_short_pretty} {time}|Fallback>",
					},
				},
			},
		},
		Attachments: []model.SlackAttachment{
			{
				Color: color,
				Title: fmt.Sprintf("Alert %s — %s", alert.AlertID, alert.Status),
				Text:  alert.Message,
				Fields: []model.SlackField{
					{Title: "Failure Rate", Value: fmt.Sprintf("%.2f%%", alert.FailureRate*100), Short: true},
					{Title: "Failed Proxies", Value: fmt.Sprintf("%d/%d", alert.DownProxies, alert.TotalProxies), Short: true},
					{Title: "Event", Value: event, Short: true},
				},
				Footer: "ProxyMaze Background Monitor",
				Ts:     time.Now().Unix(),
			},
		},
	}
	body, err := json.Marshal(slack)
	if err != nil {
		log.Printf("slack marshal error: %v", err)
		return
	}
	m.deliverWithRetry("slack", ig.WebhookURL, body, nil)
}

func (m *Monitor) deliverDiscord(ig *model.Integration, event string, alert *model.Alert) {
	color := 0xe74c3c
	title := "🚨 Alert Fired"
	if event == model.EventAlertResolved {
		color = 0x2ecc71
		title = "✅ Alert Resolved"
	}
	
	username := ig.Username
	if username == "" {
		username = "ProxyWatch"
	}

	discord := model.DiscordPayload{
		Username: username,
		Content:  "Proxy Monitor Alert",
		Embeds: []model.DiscordEmbed{
			{
				Title:       title,
				Description: alert.Message,
				Color:       color,
				Fields: []model.DiscordEmbedField{
					{Name: "Alert ID", Value: alert.AlertID, Inline: true},
					{Name: "Status", Value: alert.Status, Inline: true},
					{Name: "Failure Rate", Value: fmt.Sprintf("%.2f%%", alert.FailureRate*100), Inline: true},
					{Name: "Failed Proxies", Value: fmt.Sprintf("%d/%d", alert.DownProxies, alert.TotalProxies), Inline: true},
				},
				Footer:    &model.DiscordFooter{Text: "ProxyMaze Background Monitor"},
				Timestamp: time.Now().UTC().Format(time.RFC3339),
			},
		},
	}
	body, err := json.Marshal(discord)
	if err != nil {
		log.Printf("discord marshal error: %v", err)
		return
	}
	m.deliverWithRetry("discord", ig.WebhookURL, body, nil)
}

func (m *Monitor) deliverWithRetry(label, endpoint string, body []byte, extraHeaders map[string]string) {
	backoff := 1 * time.Second
	maxBackoff := 30 * time.Second

	client := &http.Client{Timeout: 10 * time.Second}

	for attempt := 0; attempt < 10; attempt++ {
		if attempt > 0 {
			log.Printf("%s retry #%d to %s (backoff %v)", label, attempt, endpoint, backoff)
			time.Sleep(backoff)
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}

		req, err := http.NewRequest(http.MethodPut, endpoint, bytes.NewReader(body))
		if err != nil {
			log.Printf("failed to create request: %v", err)
			return
		}
		req.Header.Set("Content-Type", "application/json")
		for k, v := range extraHeaders {
			req.Header.Set(k, v)
		}

		resp, err := client.Do(req)
		if err != nil {
			log.Printf("%s delivery network error to %s: %v", label, endpoint, err)
			continue
		}

		statusCode := resp.StatusCode
		// Always read and log body on non-2xx for diagnostics
		respBody := make([]byte, 512)
		n, _ := resp.Body.Read(respBody)
		resp.Body.Close()

		if statusCode >= 200 && statusCode < 300 {
			log.Printf("✅ %s delivered to %s (status %d)", label, endpoint, statusCode)
			m.store.Mu.Lock()
			m.store.WebhookDeliveries++
			m.store.Mu.Unlock()
			return
		}
		if isTransient(statusCode) {
			log.Printf("⚠️  %s transient %d from %s body=%q", label, statusCode, endpoint, respBody[:n])
			continue
		}

		log.Printf("❌ %s rejected %d from %s body=%q, not retrying", label, statusCode, endpoint, respBody[:n])
		return
	}
	log.Printf("❌ %s max retries exceeded for %s", label, endpoint)
}
