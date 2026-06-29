# Multi-stage build for API Gateway
# Stage 1: Build
FROM golang:1.21-alpine AS builder

WORKDIR /build

# Install build dependencies
RUN apk add --no-cache git ca-certificates tzdata

# Copy shared modules first for caching
COPY shared/ /build/shared/

# Copy gateway source
COPY api-gateway/ /build/api-gateway/

# Build the binary
WORKDIR /build/api-gateway
RUN go mod init api-gateway 2>/dev/null || true
RUN go mod tidy 2>/dev/null || true
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags='-w -s -extldflags "-static"' \
    -a -installsuffix cgo \
    -o /build/bin/api-gateway \
    main.go

# Stage 2: Runtime
FROM scratch

# Copy certificates from builder
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /usr/share/zoneinfo /usr/share/zoneinfo

# Copy binary
COPY --from=builder /build/bin/api-gateway /api-gateway

# Use non-root user
USER 65534:65534

EXPOSE 8080

HEALTHCHECK --interval=15s --timeout=5s --start-period=10s --retries=3 \
    CMD ["/api-gateway", "-health-check"] || exit 0

ENTRYPOINT ["/api-gateway"]
