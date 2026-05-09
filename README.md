# Proxy Monitor

Continuous background monitoring service for proxy endpoints. Built for the **Torch Labs Proxy Maze 26** challenge.

Uses **Redis** for sub-millisecond state access and persistence across restarts. Go standard library `net/http` for the API layer. Zero-framework architecture.

---

## Project Structure

```
proxy-monitor/
├── cmd/
│   └── server/
│       └── main.go              # Application entrypoint (Redis connection, graceful shutdown)
├── internal/
│   ├── handler/
│   │   └── handler.go           # HTTP route wiring & JSON endpoint handlers
│   ├── model/
│   │   └── model.go             # Domain types, JSON contracts, constants
│   ├── monitor/
│   │   └── monitor.go           # Background poller, prober, alert state machine, notifier
│   └── store/
│       ├── store.go             # Store interface (abstraction over storage backend)
│       └── redis.go             # Redis-backed Store implementation
├── Dockerfile                   # Multi-stage build → distroless runtime (~5 MB)
├── docker-compose.yml           # App + Redis with persistence, resource limits
├── .dockerignore
├── .gitignore
├── go.mod
├── go.sum
└── README.md
```

---

## Quick Start

### Docker (recommended)

```bash
# Build and run (app + Redis)
docker compose up --build -d

# View logs
docker compose logs -f proxy-monitor

# Stop
docker compose down

# Stop and remove data
docker compose down -v
```

### Run locally

Requires a running Redis instance:

```bash
# Start Redis (if not using Docker)
redis-server

# Run the app
REDIS_URL=redis://localhost:6379 go run ./cmd/server
```

The server starts on `http://localhost:8080` by default. Override with `PORT`:

```bash
PORT=3000 REDIS_URL=redis://localhost:6379 go run ./cmd/server
```

---

## Environment Variables

| Variable    | Default                  | Description                       |
|-------------|--------------------------|-----------------------------------|
| `PORT`      | `8080`                   | HTTP server port                  |
| `REDIS_URL` | `redis://localhost:6379`  | Redis connection URL              |

---

## API Reference

All endpoints return `Content-Type: application/json`.

### Health

| Method | Path      | Description          |
|--------|-----------|----------------------|
| GET    | `/health` | Returns `{"status": "ok"}` |

### Configuration

| Method | Path      | Description                                |
|--------|-----------|--------------------------------------------|
| POST   | `/config` | Set `check_interval_seconds` and `request_timeout_ms` |
| GET    | `/config` | Get current config                         |

```bash
# Set polling interval to 10s with 3s timeout
curl -X POST http://localhost:8080/config \
  -H "Content-Type: application/json" \
  -d '{"check_interval_seconds": 10, "request_timeout_ms": 3000}'
```

### Proxies

| Method | Path                      | Description                                  |
|--------|---------------------------|----------------------------------------------|
| POST   | `/proxies`                | Register proxy URLs (`replace` to clear pool) |
| GET    | `/proxies`                | List all proxies with aggregate stats         |
| GET    | `/proxies/{id}`           | Get proxy details with history                |
| GET    | `/proxies/{id}/history`   | Get time-stamped check history array          |
| DELETE | `/proxies`                | Clear proxy pool (preserves alert history)    |

```bash
# Register proxies
curl -X POST http://localhost:8080/proxies \
  -H "Content-Type: application/json" \
  -d '{"urls": ["http://proxy.example.com/proxy/px-101", "http://proxy.example.com/proxy/px-102"], "replace": false}'

# List all with stats
curl http://localhost:8080/proxies
```

**ID generation**: The proxy ID is the final segment of its URL.  
`http://example.com/proxy/px-101` → ID: `px-101`

### Alerts

| Method | Path      | Description                      |
|--------|-----------|----------------------------------|
| GET    | `/alerts` | List all active and resolved alerts |

**Alert rules**:
- Failure rate = `down / total` across the entire proxy pool
- Threshold: `≥ 0.20` fires, `< 0.20` resolves
- Only ONE alert can be active at a time
- A resolved + re-breached state produces a new `alert_id`

### Webhooks

| Method | Path        | Description               |
|--------|-------------|---------------------------|
| POST   | `/webhooks` | Register a webhook URL    |

```bash
curl -X POST http://localhost:8080/webhooks \
  -H "Content-Type: application/json" \
  -d '{"url": "https://your-server.com/webhook"}'
```

Webhook payloads (`alert.fired` / `alert.resolved`) are delivered within 60 seconds. Transient errors (500, 502, 503, 504) are retried with exponential backoff until successful.

### Integrations

| Method | Path             | Description                      |
|--------|------------------|----------------------------------|
| POST   | `/integrations`  | Register a Slack or Discord webhook |

```bash
curl -X POST http://localhost:8080/integrations \
  -H "Content-Type: application/json" \
  -d '{"type": "slack", "endpoint": "https://hooks.slack.com/services/..."}'
```

### Metrics

| Method | Path       | Description            |
|--------|------------|------------------------|
| GET    | `/metrics` | Operational statistics |

Returns: `total_checks`, `total_proxies`, `proxies_up`, `proxies_down`, `proxies_pending`, `active_alerts`, `resolved_alerts`, `total_alerts`, `failure_rate`, `webhook_count`.

---

## Architecture

```
┌─────────────────────────────────────────────────┐
│                   HTTP API                       │
│              (internal/handler)                  │
├─────────────────────────────────────────────────┤
│                                                  │
│   ┌──────────────┐       ┌──────────────┐        │
│   │ Store (iface) │◄──────│   Monitor    │        │
│   │               │       │  (Poller +   │        │
│   │  GetProxy()   │       │   Alerter +  │        │
│   │  SetProxy()   │       │   Notifier)  │        │
│   │  AddAlert()   │       │              │        │
│   │  ...          │       │  goroutine   │        │
│   └──────┬───────┘       └──────┬───────┘        │
│          │                       │                │
│   ┌──────▼───────┐       ┌──────▼───────┐        │
│   │    Redis 7    │       │ HTTP Probes  │        │
│   │  (AOF + LRU)  │       │ (concurrent) │        │
│   └───────────────┘       └──────────────┘        │
└─────────────────────────────────────────────────┘
```

- **Store interface**: Decouples handler/monitor from storage. Currently backed by Redis; swappable for testing.
- **Redis**: JSON values with SET/GET, Lists for history (O(1) append), Sets for ID indexes, pipelines for batch reads.
- **Monitor**: Background goroutine ticking at `check_interval_seconds`. Probes all proxies concurrently, then evaluates the global alert threshold.
- **Handler**: Stateless HTTP handlers reading/writing through the Store interface.

### Redis Key Layout

| Key Pattern              | Type   | Purpose                        |
|--------------------------|--------|--------------------------------|
| `config`                 | String | JSON config blob               |
| `proxies`                | Set    | All proxy IDs                  |
| `proxy:{id}`             | String | JSON proxy metadata            |
| `proxy:{id}:history`     | List   | Probe results (chronological)  |
| `alerts`                 | List   | Alert IDs (chronological)      |
| `alert:{id}`             | String | JSON alert blob                |
| `active_alert_id`        | String | Current active alert ID        |
| `webhooks`               | Set    | All webhook IDs                |
| `webhook:{id}`           | String | JSON webhook blob              |
| `integrations`           | Set    | All integration IDs            |
| `integration:{id}`       | String | JSON integration blob          |

---

## Docker Details

| Component        | Image                                 | Details                         |
|------------------|---------------------------------------|---------------------------------|
| **App**          | `gcr.io/distroless/static-debian12`   | ~5 MB, nonroot, read-only FS   |
| **Redis**        | `redis:7-alpine`                      | AOF persistence, 64 MB limit   |

| Feature            | App                    | Redis                   |
|--------------------|------------------------|-------------------------|
| Memory limit       | 128 MB                 | 128 MB                  |
| CPU limit          | 0.50 cores             | 0.25 cores              |
| Persistence        | Stateless              | AOF + named volume      |
| Log rotation       | 3 × 10 MB              | 3 × 5 MB               |
| Restart policy     | `unless-stopped`       | `unless-stopped`        |
| Health check       | –                      | `redis-cli ping`        |

---

## License

MIT
