# ─── Build stage ────────────────────────────────────────────────────────────
FROM --platform=$BUILDPLATFORM golang:1.24-alpine AS builder

RUN apk add --no-cache ca-certificates git

WORKDIR /src

# Build arguments provided by Docker BuildKit
ARG TARGETOS
ARG TARGETARCH

# Copy go.mod and download dependencies (even if zero)
COPY go.mod ./
RUN go mod download

# Copy source and build.
COPY . .

RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags="-s -w" -o /bin/proxy-monitor ./cmd/server

# ─── Runtime stage (distroless — smallest, most secure) ─────────────────────
FROM gcr.io/distroless/static-debian12:nonroot

LABEL org.opencontainers.image.title="proxy-monitor" \
      org.opencontainers.image.description="Continuous background proxy monitoring service" \
      org.opencontainers.image.source="https://github.com/your-org/proxy-monitor"

COPY --from=builder /bin/proxy-monitor /proxy-monitor

EXPOSE 8080

ENV PORT=8080

USER nonroot:nonroot

ENTRYPOINT ["/proxy-monitor"]
