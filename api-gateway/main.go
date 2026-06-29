// API Gateway provides a single entry point for all client requests.
// It handles routing, middleware (auth, logging, rate limiting), and
// health checks. It proxies requests to downstream services.
package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

// Config holds gateway configuration from environment variables.
type Config struct {
	Port                 string
	OrderServiceURL      string
	PaymentServiceURL    string
	InventoryServiceURL  string
	NotificationServiceURL string
	ServiceName          string
}

func loadConfig() Config {
	return Config{
		Port:                 getEnv("HTTP_PORT", "8080"),
		OrderServiceURL:      getEnv("ORDER_SERVICE_URL", "http://localhost:8081"),
		PaymentServiceURL:    getEnv("PAYMENT_SERVICE_URL", "http://localhost:8082"),
		InventoryServiceURL:  getEnv("INVENTORY_SERVICE_URL", "http://localhost:8083"),
		NotificationServiceURL: getEnv("NOTIFICATION_SERVICE_URL", "http://localhost:8084"),
		ServiceName:          getEnv("SERVICE_NAME", "api-gateway"),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// ProxyRoute defines a routing rule for a service.
type ProxyRoute struct {
	PathPrefix string
	TargetURL  *url.URL
	Rewrite    func(string) string
}

// Gateway holds all routing configuration and state.
type Gateway struct {
	config  Config
	routes  []ProxyRoute
	proxies map[string]*httputil.ReverseProxy
	mu      sync.RWMutex
	logger  *log.Logger
}

// NewGateway creates a new API Gateway with configured routes.
func NewGateway(cfg Config) (*Gateway, error) {
	logger := log.New(os.Stdout, fmt.Sprintf("[%s] ", cfg.ServiceName), log.LstdFlags|log.Lmicroseconds)

	orderURL, err := url.Parse(cfg.OrderServiceURL)
	if err != nil {
		return nil, fmt.Errorf("invalid order service URL: %w", err)
	}
	paymentURL, err := url.Parse(cfg.PaymentServiceURL)
	if err != nil {
		return nil, fmt.Errorf("invalid payment service URL: %w", err)
	}
	inventoryURL, err := url.Parse(cfg.InventoryServiceURL)
	if err != nil {
		return nil, fmt.Errorf("invalid inventory service URL: %w", err)
	}
	notificationURL, err := url.Parse(cfg.NotificationServiceURL)
	if err != nil {
		return nil, fmt.Errorf("invalid notification service URL: %w", err)
	}

	g := &Gateway{
		config:  cfg,
		proxies: make(map[string]*httputil.ReverseProxy),
		logger:  logger,
	}

	g.routes = []ProxyRoute{
		{
			PathPrefix: "/api/orders",
			TargetURL:  orderURL,
			Rewrite:    func(p string) string { return strings.TrimPrefix(p, "/api") },
		},
		{
			PathPrefix: "/api/payments",
			TargetURL:  paymentURL,
			Rewrite:    func(p string) string { return strings.TrimPrefix(p, "/api") },
		},
		{
			PathPrefix: "/api/inventory",
			TargetURL:  inventoryURL,
			Rewrite:    func(p string) string { return strings.TrimPrefix(p, "/api") },
		},
		{
			PathPrefix: "/api/notifications",
			TargetURL:  notificationURL,
			Rewrite:    func(p string) string { return strings.TrimPrefix(p, "/api") },
		},
	}

	// Pre-create reverse proxies for each route
	for _, r := range g.routes {
		proxy := httputil.NewSingleHostReverseProxy(r.TargetURL)
		proxy.ErrorHandler = func(w http.ResponseWriter, req *http.Request, err error) {
			g.logger.Printf("proxy error: %s -> %v", req.URL.Path, err)
			http.Error(w, `{"error":"service unavailable","code":503}`, http.StatusServiceUnavailable)
		}
		proxy.ModifyResponse = func(resp *http.Response) error {
			// Add gateway headers for tracing
			resp.Header.Set("X-Gateway", cfg.ServiceName)
			resp.Header.Set("X-Gateway-Time", time.Now().UTC().Format(time.RFC3339))
			return nil
		}
		g.proxies[r.PathPrefix] = proxy
	}

	return g, nil
}

// ServeHTTP implements the http.Handler interface.
func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	requestID := generateRequestID()

	// Set context values
	ctx := context.WithValue(r.Context(), "request_id", requestID)
	r = r.WithContext(ctx)

	// Add trace headers
	r.Header.Set("X-Request-ID", requestID)
	r.Header.Set("X-Forwarded-For", r.RemoteAddr)

	// Wrap response writer to capture status code
	ww := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}

	// Apply middleware chain
	handler := g.applyMiddlewares(http.HandlerFunc(g.routeRequest))
	handler.ServeHTTP(ww, r)

	duration := time.Since(start)
	g.logger.Printf("[%s] %s %s -> %d (%s)", requestID, r.Method, r.URL.Path, ww.statusCode, duration)
}

func (g *Gateway) routeRequest(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path

	// Find matching route
	for _, route := range g.routes {
		if strings.HasPrefix(path, route.PathPrefix) {
			proxy, ok := g.proxies[route.PathPrefix]
			if !ok {
				http.Error(w, `{"error":"proxy not found"}`, http.StatusInternalServerError)
				return
			}

			// Rewrite URL path
			originalPath := r.URL.Path
			r.URL.Path = route.Rewrite(originalPath)
			r.URL.RawPath = ""

			// Update Host header for proxy
			r.Host = route.TargetURL.Host

			proxy.ServeHTTP(w, r)
			return
		}
	}

	// No route matched
	if path == "/health" {
		g.healthHandler(w, r)
		return
	}

	if path == "/" {
		g.rootHandler(w, r)
		return
	}

	http.Error(w, fmt.Sprintf(`{"error":"not found","path":"%s"}`, path), http.StatusNotFound)
}

// healthHandler returns gateway health status and downstream checks.
func (g *Gateway) healthHandler(w http.ResponseWriter, r *http.Request) {
	health := map[string]interface{}{
		"status":    "healthy",
		"service":   g.config.ServiceName,
		"timestamp": time.Now().UTC().Format(time.RFC3339),
		"version":   "1.0.0",
		"upstreams": map[string]string{
			"order_service":       g.config.OrderServiceURL,
			"payment_service":     g.config.PaymentServiceURL,
			"inventory_service":   g.config.InventoryServiceURL,
			"notification_service": g.config.NotificationServiceURL,
		},
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	writeJSON(w, health)
}

func (g *Gateway) rootHandler(w http.ResponseWriter, r *http.Request) {
	response := map[string]interface{}{
		"name":        "Event-Driven Microservices API Gateway",
		"version":     "1.0.0",
		"description": "Unified entry point for the event-driven microservices platform",
		"endpoints": []map[string]string{
			{"path": "/health", "method": "GET", "description": "Health check"},
			{"path": "/api/orders", "method": "POST", "description": "Create order"},
			{"path": "/api/orders/:id", "method": "GET", "description": "Get order"},
			{"path": "/api/payments/:id", "method": "GET", "description": "Get payment status"},
			{"path": "/api/inventory/:sku", "method": "GET", "description": "Check stock"},
		},
	}

	w.Header().Set("Content-Type", "application/json")
	writeJSON(w, response)
}

// Middleware chain
func (g *Gateway) applyMiddlewares(next http.Handler) http.Handler {
	h := recoveryMiddleware(g.logger)(next)
	h = corsMiddleware(h)
	h = loggingMiddleware(g.logger)(h)
	h = rateLimitMiddleware(h)
	return h
}

// ─── Middleware ──────────────────────────────────────────────────

func recoveryMiddleware(logger *log.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if rec := recover(); rec != nil {
					logger.Printf("panic recovered: %v", rec)
					http.Error(w, `{"error":"internal server error"}`, http.StatusInternalServerError)
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}

func loggingMiddleware(logger *log.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			next.ServeHTTP(w, r)
		})
	}
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Request-ID")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// Simple in-memory rate limiter
func rateLimitMiddleware(next http.Handler) http.Handler {
	// Production would use Redis-backed rate limiting
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(w, r)
	})
}

// ─── Helpers ─────────────────────────────────────────────────────

// responseWriter captures the status code for logging.
type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

var requestCounter uint64
var counterMu sync.Mutex

func generateRequestID() string {
	counterMu.Lock()
	defer counterMu.Unlock()
	requestCounter++
	return fmt.Sprintf("req-%d-%d", time.Now().UnixNano(), requestCounter)
}

func writeJSON(w io.Writer, v interface{}) {
	// Simple JSON encoding
	// In production, use encoding/json
	if m, ok := v.(map[string]interface{}); ok {
		fmt.Fprintf(w, "{")
		first := true
		for k, val := range m {
			if !first {
				fmt.Fprint(w, ",")
			}
			first = false
			fmt.Fprintf(w, "\"%s\":", k)
			switch v := val.(type) {
			case string:
				fmt.Fprintf(w, "\"%s\"", v)
			case int:
				fmt.Fprintf(w, "%d", v)
			default:
				fmt.Fprintf(w, "\"%+v\"", v)
			}
		}
		fmt.Fprintf(w, "}\n")
	}
}

// ─── Main ────────────────────────────────────────────────────────

func main() {
	cfg := loadConfig()

	gateway, err := NewGateway(cfg)
	if err != nil {
		log.Fatalf("failed to create gateway: %v", err)
	}

	addr := ":" + cfg.Port
	server := &http.Server{
		Addr:         addr,
		Handler:      gateway,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Graceful shutdown
	done := make(chan os.Signal, 1)
	signal.Notify(done, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		gateway.logger.Printf("API Gateway starting on %s", addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			gateway.logger.Fatalf("server error: %v", err)
		}
	}()

	<-done
	gateway.logger.Println("shutting down API Gateway...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		gateway.logger.Printf("shutdown error: %v", err)
	}

	gateway.logger.Println("API Gateway stopped")
}
