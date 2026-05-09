# Proxy Monitor

Continuous background monitoring service for proxy endpoints. Built for the **Torch Labs ProxyMaze '26** challenge by **Team Net Runners**.

Zero external dependencies — pure Go standard library + `sync.RWMutex` for in-memory state.

---

## Final Score — ProxyMaze '26

| Phase | Criteria | Score |
|---|---|---|
| Phase 1 — Bootstrap | Health + Config | 10 / 10 |
| Phase 2 — Proxy Ingestion | Ingest, Poll, Fields | 30 / 30 |
| Phase 3 — Single Failure | Detect, No False Alert, Recover | 30 / 30 |
| Phase 4 — Threshold Alerts | Breach, Payload, Webhooks, Retry, Dedup, Consistency | 90 / 90 |
| Phase 5 — Alert Resolution | Recovery, Resolved Webhook | 20 / 20 |
| Phase 6 — Re-breach Lifecycle | New ID, Ordering | 30 / 30 |
| Phase 7 — Pool Ops | Replace, Delete, History, Metrics | 25 / 25 |
| Bonus B1 — Slack Block Kit | Attachment-format delivery | +10 |
| Bonus B2 — Discord Embeds | Embed-format delivery | +10 |
| **Total** | | **255–270 / 270** |

**Passing score: 186** ✅

---

## Bug Fix Journal

A complete record of every non-trivial bug diagnosed and resolved during the competition.

---

### 🐛 BUG-01 — ARM Build Failure (`stat /src/cmd/server: directory not found`)

**Symptom**
```
ERROR [proxy-monitor builder 7/7] RUN CGO_ENABLED=0 ... go build ... ./cmd/server
stat /src/cmd/server: directory not found
```

**Root Cause**  
Dockerfile used `GOARCH=amd64` hard-coded while the deployment node runs ARM64 (Ubuntu on ARM). The build context also referenced the wrong working-directory path (`/src` vs `/app`).

**Fix**  
- Removed hard-coded `GOOS/GOARCH` to allow Docker BuildKit to detect the host platform automatically.  
- Corrected the build context `WORKDIR` from `/src` to `/app`.

---

### 🐛 BUG-02 — Wrong JSON Field Names on Alert (spec mismatch)

**Symptom**  
Criterion 10 (alert payload) scored 7/10. `GET /alerts` returned fields the evaluator didn't recognise.

**Root Cause**  
Alert struct used internal Go naming that didn't match the spec:
```
down_proxies      → should be  failed_proxies
down_proxy_ids    → should be  failed_proxy_ids
```

**Fix**  
Renamed `Alert.DownProxies` → `Alert.FailedProxies` and `Alert.DownProxyIDs` → `Alert.FailedProxyIDs` with matching JSON tags. Removed `omitempty` from `resolved_at` so it serialises as `null` (not omitted) for active alerts.

---

### 🐛 BUG-03 — `POST /webhooks` Returned Wrong ID Field

**Symptom**  
Webhook registration returned `{"id": "wh_xxx"}` but the spec requires `{"webhook_id": "wh-xxx"}`.

**Fix**  
- Renamed `Webhook.ID` → `Webhook.WebhookID` with `json:"webhook_id"`.  
- Changed ID prefix format from `wh_` to `wh-`.

---

### 🐛 BUG-04 — Single Monolithic Webhook Payload (spec requires two distinct shapes)

**Symptom**  
Webhook delivery sent the same struct for both `alert.fired` and `alert.resolved`. The evaluator validates each event against its own exact schema.

**Root Cause**  
`alert.fired` and `alert.resolved` have very different required fields:

```jsonc
// alert.fired — 9 fields
{ "event", "alert_id", "fired_at", "failure_rate",
  "total_proxies", "failed_proxies", "failed_proxy_ids",
  "threshold", "message" }

// alert.resolved — 3 fields only
{ "event", "alert_id", "resolved_at" }
```

**Fix**  
Replaced a single `WebhookPayload` struct with two separate structs:  
- `WebhookFiredPayload` — 9 exact fields  
- `WebhookResolvedPayload` — 3 exact fields  

`dispatchNotifications` now builds the correct struct per event type.

---

### 🐛 BUG-05 — Transient Retry Included 429 (not in spec)

**Symptom**  
Criterion 24 (retry on 5xx) — potential false retries on 429.

**Root Cause**  
`isTransient()` included `429` but the spec only lists `500, 502, 503, 504` as retry-able codes.

**Fix**
```go
// Before
return code == 429 || code == 500 || code == 502 || code == 503 || code == 504

// After  
return code == 500 || code == 502 || code == 503 || code == 504
```

---

### 🐛 BUG-06 — All Webhooks Rejected with `405 Method Not Allowed`

**Symptom** *(the hardest bug — 7 criteria failing)*
```
❌ webhook rejected 405 from http://evaluator.torchproxies.com/__capture/...
   body="{\"detail\":\"Method Not Allowed\"}"
```

The response body `{"detail":"Method Not Allowed"}` is a **FastAPI** error format. This meant the route existed but the HTTP method was wrong. Both POST and PUT were tried — both returned 405.

**Root Cause (discovered via response body logging)**  
The evaluator's capture server runs at `http://evaluator.torchproxies.com` (plain HTTP). Go's `http.Client` followed the **301 redirect** to `https://evaluator.torchproxies.com` — but Go's default redirect behaviour **silently converts POST → GET** on 301/302 redirects (per legacy RFC 2616). The HTTPS endpoint returned `405` because it only accepts POST, not GET.

**Diagnostic steps**:
1. Added `body=` logging to read and print the response body on non-2xx responses.
2. Saw `{"detail":"Method Not Allowed"}` — identified FastAPI.
3. Hypothesis: HTTP → HTTPS redirect converting POST → GET.
4. Fix: disabled automatic redirect following with `CheckRedirect: return http.ErrUseLastResponse`, then manually re-issued `POST` to the `Location` header URL.

**Fix**
```go
client := &http.Client{
    Timeout: 10 * time.Second,
    CheckRedirect: func(req *http.Request, via []*http.Request) error {
        return http.ErrUseLastResponse // stop auto-follow
    },
}

// After getting 301/302/307/308, manually reissue POST to Location:
if statusCode == 301 || statusCode == 302 || ... {
    location := resp.Header.Get("Location")
    currentURL = location
    continue
}
```

**Log after fix**
```
webhook redirect 301 → https://evaluator.torchproxies.com/__capture/.../capture/...
✅ webhook delivered to https://... (status 200)
```

This single fix unlocked **7 criteria simultaneously**: 4.6, 5.4, 4.6a, 4.6b, 4.7, 4.8, and both bonus integrations.

---

### 🐛 BUG-07 — Slack Payload Used `blocks` Instead of `attachments`

**Symptom**  
Criterion 19 (Slack Block Kit) — 0/10 even after delivery succeeded with status 200.

**Root Cause**  
We had been guessing the Slack format. The actual spec (Part Four) explicitly requires **legacy attachment format**:

```
attachments[0].color     "#RRGGBB" hex string
attachments[0].fields[]  {title, value} — must include:
                           Alert ID, Failure Rate, Failed Proxies,
                           Threshold, Failed IDs, Fired At
attachments[0].footer    non-empty string
attachments[0].ts        Unix epoch integer (not float, not string)
```

We had removed `attachments` and replaced with `blocks`, which broke the criterion entirely.

**Fix**  
Reverted to `attachments`-only payload with all 6 required field titles including `"Failed IDs"` (joining `FailedProxyIDs` with `, `) and `"Fired At"` (RFC3339 UTC timestamp). `ts` uses `alert.FiredAt.Unix()` (Go `int64` → JSON integer, never a float).

---

### 🐛 BUG-08 — Discord Missing `"Failed IDs"` Field

**Symptom**  
Criterion 20 (Discord Embeds) — 5/10 partial.

**Root Cause**  
Discord spec requires `embeds[0].fields` names to include (case-insensitive): Alert ID, Failure Rate, Failed Proxies, Threshold, and **Failed IDs**. Our payload had all except "Failed IDs".

**Fix**  
Added:
```go
{Name: "Failed IDs", Value: strings.Join(alert.FailedProxyIDs, ", "), Inline: false}
```
Also removed the spurious `content` top-level field and corrected color values from Go hex literals (`0xe74c3c`) to JSON-compatible decimal integers (`15158332`).

---

## Project Structure

```
proxy-monitor/
├── cmd/
│   └── server/
│       └── main.go              # Application entrypoint
├── internal/
│   ├── handler/
│   │   ├── handler.go           # HTTP route wiring & JSON endpoint handlers
│   │   └── dashboard.html       # Embedded live dashboard UI
│   ├── model/
│   │   └── model.go             # Domain types, JSON contracts, constants
│   ├── monitor/
│   │   └── monitor.go           # Background poller, prober, alert state machine, notifier
│   └── store/
│       └── store.go             # Thread-safe in-memory state (sync.RWMutex)
├── Dockerfile                   # Multi-stage build → distroless runtime (~5 MB)
├── docker-compose.yml           # Production compose with resource limits
├── .dockerignore
├── go.mod
└── README.md
```

---

## Quick Start

### Run locally

```bash
go run ./cmd/server
```

### Docker

```bash
docker compose up --build -d
docker compose logs -f proxy-monitor
```

---

## API Reference

### Health
| Method | Path | Response |
|---|---|---|
| GET | `/health` | `{"status": "ok"}` |

### Configuration
| Method | Path | Description |
|---|---|---|
| POST | `/config` | Set `check_interval_seconds` + `request_timeout_ms` |
| GET | `/config` | Get current config |

### Proxies
| Method | Path | Description |
|---|---|---|
| POST | `/proxies` | Register URLs (`"replace": true` clears pool first) |
| GET | `/proxies` | Pool summary + per-proxy state |
| GET | `/proxies/{id}` | Single proxy detail + history |
| GET | `/proxies/{id}/history` | Time-series probe history array |
| DELETE | `/proxies` | Clear pool (alert history preserved) |

**ID rule**: final URL path segment → `https://example.com/proxy/px-101` → `px-101`

### Alerts
| Method | Path | Description |
|---|---|---|
| GET | `/alerts` | All alerts (active + resolved) |

- Threshold: `failure_rate ≥ 0.20` fires; `< 0.20` resolves
- At most one active alert at any time
- Re-breach after resolution mints a new `alert_id`

### Webhooks
| Method | Path | Description |
|---|---|---|
| POST | `/webhooks` | Register a callback URL |

Delivers `alert.fired` (9 fields) and `alert.resolved` (3 fields) payloads.  
Retries on 500/502/503/504 with exponential backoff.

### Integrations
| Method | Path | Description |
|---|---|---|
| POST | `/integrations` | Register Slack or Discord webhook |

```json
{ "type": "slack", "webhook_url": "...", "username": "ProxyWatch", "events": ["alert.fired", "alert.resolved"] }
```

### Metrics
| Method | Path | Description |
|---|---|---|
| GET | `/metrics` | `total_checks`, `current_pool_size`, `active_alerts`, `total_alerts`, `webhook_deliveries` |

---

## Architecture

```
┌──────────────────────────────────────────────┐
│                   HTTP API                    │
│             (internal/handler)                │
├──────────────────────────────────────────────┤
│   ┌───────────┐        ┌──────────────────┐  │
│   │   Store   │◄───────│    Monitor       │  │
│   │ (RWMutex) │        │  ┌────────────┐  │  │
│   │           │        │  │   Poller   │  │  │
│   │ proxies   │        │  │  (ticker)  │  │  │
│   │ alerts    │        │  ├────────────┤  │  │
│   │ webhooks  │        │  │  Alerter   │  │  │
│   │ integrs.  │        │  │ (threshold)│  │  │
│   └───────────┘        │  ├────────────┤  │  │
│                        │  │ Notifier   │  │  │
│                        │  │ (retry +   │  │  │
│                        │  │  redirect) │  │  │
│                        │  └────────────┘  │  │
│                        └──────────────────┘  │
└──────────────────────────────────────────────┘
```

- **Store**: Single `sync.RWMutex`-guarded struct. All state is in-memory.
- **Monitor**: Background goroutine; probes all proxies concurrently with `sync.WaitGroup`, then evaluates the global alert threshold.
- **Notifier**: Delivers webhooks/integrations in separate goroutines. Disables Go's default HTTP redirect to preserve `POST` method across 301 redirects.

---

## Docker Details

| Feature | Detail |
|---|---|
| Base image | `gcr.io/distroless/static-debian12` |
| Final image size | ~5 MB |
| User | `nonroot` (UID 65534) |
| Filesystem | Read-only |
| Memory limit | 128 MB |
| CPU limit | 0.50 cores |
| Log rotation | 3 × 10 MB JSON files |
| Restart policy | `unless-stopped` |

---

## License

MIT
