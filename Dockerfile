# ─── Build stage ────────────────────────────────────────────────────────────
FROM golang:1.24-alpine AS builder

RUN apk add --no-cache ca-certificates git

WORKDIR /src

# Cache dependency layer.
COPY go.mod go.sum ./
RUN go mod download

# Copy source and build a fully static binary.
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -trimpath -ldflags="-s -w" -o /bin/proxy-monitor ./cmd/server

# ─── Runtime stage (distroless — smallest, most secure) ─────────────────────
FROM gcr.io/distroless/static-debian12:nonroot

LABEL org.opencontainers.image.title="proxy-monitor" \
      org.opencontainers.image.description="Continuous background proxy monitoring service"

COPY --from=builder /bin/proxy-monitor /proxy-monitor

EXPOSE 8080

ENV PORT=8080

USER nonroot:nonroot

ENTRYPOINT ["/proxy-monitor"]
