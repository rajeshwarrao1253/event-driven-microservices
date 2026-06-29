# Multi-stage build for Order Service
FROM golang:1.21-alpine AS builder

WORKDIR /build

RUN apk add --no-cache git ca-certificates tzdata

# Copy shared modules
COPY shared/ /build/shared/

# Copy service source
COPY services/order-service/ /build/services/order-service/

WORKDIR /build/services/order-service

# Initialize module and add dependencies
RUN go mod init order-service 2>/dev/null || true && \
    go mod edit -replace event-driven-microservices/shared=/build/shared 2>/dev/null || true

# Add required dependencies
RUN go get github.com/lib/pq 2>/dev/null || true

RUN go mod tidy 2>/dev/null || true

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags='-w -s -extldflags "-static"' \
    -a -installsuffix cgo \
    -o /build/bin/order-service \
    main.go

# Runtime stage
FROM alpine:3.18

RUN apk --no-cache add ca-certificates tzdata

COPY --from=builder /build/bin/order-service /order-service

# Create non-root user
RUN adduser -D -u 65534 appuser
USER appuser

EXPOSE 8081

HEALTHCHECK --interval=15s --timeout=5s --start-period=10s --retries=3 \
    CMD wget -qO- http://localhost:8081/health || exit 1

ENTRYPOINT ["/order-service"]
