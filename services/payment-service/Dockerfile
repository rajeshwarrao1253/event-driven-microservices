# Multi-stage build for Payment Service
FROM golang:1.21-alpine AS builder

WORKDIR /build

RUN apk add --no-cache git ca-certificates tzdata

# Copy shared modules
COPY shared/ /build/shared/

# Copy service source
COPY services/payment-service/ /build/services/payment-service/

WORKDIR /build/services/payment-service

RUN go mod init payment-service 2>/dev/null || true && \
    go mod edit -replace event-driven-microservices/shared=/build/shared 2>/dev/null || true

RUN go get github.com/lib/pq 2>/dev/null || true

RUN go mod tidy 2>/dev/null || true

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags='-w -s -extldflags "-static"' \
    -a -installsuffix cgo \
    -o /build/bin/payment-service \
    main.go

# Runtime stage
FROM alpine:3.18

RUN apk --no-cache add ca-certificates tzdata

COPY --from=builder /build/bin/payment-service /payment-service

RUN adduser -D -u 65534 appuser
USER appuser

EXPOSE 8082

HEALTHCHECK --interval=15s --timeout=5s --start-period=10s --retries=3 \
    CMD wget -qO- http://localhost:8082/health || exit 1

ENTRYPOINT ["/payment-service"]
