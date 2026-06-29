// Order Service is the saga orchestrator for the order processing flow.
// It manages the complete order lifecycle: creation, payment, inventory,
// and handles compensating transactions on failure.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	_ "github.com/lib/pq"

	"event-driven-microservices/shared/events"
)

// ─── Configuration ───────────────────────────────────────────────

type Config struct {
	ServiceName string
	Port        string
	DBHost      string
	DBPort      string
	DBName      string
	DBUser      string
	DBPassword  string
	KafkaBrokers string
	LogLevel    string
}

func loadConfig() Config {
	return Config{
		ServiceName:  getEnv("SERVICE_NAME", "order-service"),
		Port:         getEnv("HTTP_PORT", "8081"),
		DBHost:       getEnv("DB_HOST", "localhost"),
		DBPort:       getEnv("DB_PORT", "5432"),
		DBName:       getEnv("DB_NAME", "orders"),
		DBUser:       getEnv("DB_USER", "order_user"),
		DBPassword:   getEnv("DB_PASSWORD", "order_pass"),
		KafkaBrokers: getEnv("KAFKA_BROKERS", "localhost:9092"),
		LogLevel:     getEnv("LOG_LEVEL", "info"),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// ─── Domain Models ───────────────────────────────────────────────

type Order struct {
	ID            string           `json:"order_id"`
	UserID        string           `json:"user_id"`
	Items         []events.OrderItem `json:"items"`
	TotalAmount   float64          `json:"total_amount"`
	Currency      string           `json:"currency"`
	Status        string           `json:"status"` // pending, confirmed, cancelled
	ShippingAddr  events.ShippingAddress `json:"shipping_address"`
	CreatedAt     time.Time        `json:"created_at"`
	UpdatedAt     time.Time        `json:"updated_at"`
}

type CreateOrderRequest struct {
	UserID       string                 `json:"user_id"`
	Items        []events.OrderItem     `json:"items"`
	ShippingAddr events.ShippingAddress `json:"shipping_address"`
}

type OrderResponse struct {
	OrderID     string  `json:"order_id"`
	Status      string  `json:"status"`
	TotalAmount float64 `json:"total_amount"`
	Message     string  `json:"message,omitempty"`
}

// ─── Outbox Pattern ──────────────────────────────────────────────

type OutboxRecord struct {
	ID        int64     `json:"id"`
	Topic     string    `json:"topic"`
	Key       string    `json:"key"`
	Payload   string    `json:"payload"`
	Headers   string    `json:"headers"`
	CreatedAt time.Time `json:"created_at"`
	Processed bool      `json:"processed"`
}

// ─── Idempotency Store ───────────────────────────────────────────

type IdempotencyStore struct {
	mu     sync.RWMutex
	keys   map[string]time.Time // event_id -> processed_at
	ttl    time.Duration
}

func NewIdempotencyStore(ttl time.Duration) *IdempotencyStore {
	s := &IdempotencyStore{
		keys: make(map[string]time.Time),
		ttl:  ttl,
	}
	go s.cleanupLoop()
	return s
}

func (s *IdempotencyStore) IsProcessed(eventID string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.keys[eventID]
	return ok
}

func (s *IdempotencyStore) MarkProcessed(eventID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.keys[eventID] = time.Now()
}

func (s *IdempotencyStore) cleanupLoop() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		s.mu.Lock()
		now := time.Now()
		for k, v := range s.keys {
			if now.Sub(v) > s.ttl {
				delete(s.keys, k)
			}
		}
		s.mu.Unlock()
	}
}

// ─── Application ─────────────────────────────────────────────────

type OrderService struct {
	config     Config
	db         *sql.DB
	logger     *log.Logger
	idempotency *IdempotencyStore
	mu         sync.RWMutex
	orders     map[string]*Order // in-memory store for demo
}

func NewOrderService(cfg Config) (*OrderService, error) {
	logger := log.New(os.Stdout, fmt.Sprintf("[%s] ", cfg.ServiceName), log.LstdFlags|log.Lmicroseconds)

	// Database connection
	dsn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		cfg.DBHost, cfg.DBPort, cfg.DBUser, cfg.DBPassword, cfg.DBName)

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(5 * time.Minute)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	svc := &OrderService{
		config:      cfg,
		db:          db,
		logger:      logger,
		idempotency: NewIdempotencyStore(24 * time.Hour),
		orders:      make(map[string]*Order),
	}

	// Initialize schema
	if err := svc.initSchema(ctx); err != nil {
		return nil, fmt.Errorf("failed to init schema: %w", err)
	}

	logger.Println("order service initialized successfully")
	return svc, nil
}

func (s *OrderService) initSchema(ctx context.Context) error {
	schema := `
	CREATE TABLE IF NOT EXISTS orders (
		id VARCHAR(64) PRIMARY KEY,
		user_id VARCHAR(64) NOT NULL,
		items JSONB NOT NULL,
		total_amount DECIMAL(12,2) NOT NULL,
		currency VARCHAR(3) DEFAULT 'USD',
		status VARCHAR(20) NOT NULL DEFAULT 'pending',
		shipping_address JSONB,
		created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
		updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
	);

	CREATE INDEX IF NOT EXISTS idx_orders_user_id ON orders(user_id);
	CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
	CREATE INDEX IF NOT EXISTS idx_orders_created_at ON orders(created_at);

	CREATE TABLE IF NOT EXISTS outbox (
		id SERIAL PRIMARY KEY,
		topic VARCHAR(128) NOT NULL,
		key VARCHAR(64) NOT NULL,
		payload TEXT NOT NULL,
		headers TEXT,
		created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
		processed BOOLEAN DEFAULT FALSE,
		processed_at TIMESTAMP WITH TIME ZONE
	);

	CREATE INDEX IF NOT EXISTS idx_outbox_processed ON outbox(processed, created_at) WHERE processed = FALSE;
	`

	_, err := s.db.ExecContext(ctx, schema)
	return err
}

func (s *OrderService) generateID(prefix string) string {
	return fmt.Sprintf("%s-%d-%d", prefix, time.Now().UnixNano(), os.Getpid())
}

// ─── HTTP Handlers ───────────────────────────────────────────────

func (s *OrderService) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	method := r.Method

	switch {
	case path == "/health":
		s.healthHandler(w, r)
	case path == "/orders" && method == http.MethodPost:
		s.createOrderHandler(w, r)
	case strings.HasPrefix(path, "/orders/") && method == http.MethodGet:
		s.getOrderHandler(w, r)
	case strings.HasPrefix(path, "/orders/") && method == http.MethodDelete:
		s.cancelOrderHandler(w, r)
	default:
		jsonError(w, "not found", http.StatusNotFound)
	}
}

func (s *OrderService) healthHandler(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	dbStatus := "healthy"
	if err := s.db.PingContext(ctx); err != nil {
		dbStatus = "unhealthy: " + err.Error()
	}

	response := map[string]interface{}{
		"status":      "healthy",
		"service":     s.config.ServiceName,
		"database":    dbStatus,
		"timestamp":   time.Now().UTC().Format(time.RFC3339),
		"version":     "1.0.0",
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (s *OrderService) createOrderHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	var req CreateOrderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.logger.Printf("error decoding request: %v", err)
		jsonError(w, "invalid request body", http.StatusBadRequest)
		return
	}

	// Validate request
	if req.UserID == "" {
		jsonError(w, "user_id is required", http.StatusBadRequest)
		return
	}
	if len(req.Items) == 0 {
		jsonError(w, "at least one item is required", http.StatusBadRequest)
		return
	}

	// Calculate total
	var total float64
	for _, item := range req.Items {
		if item.Quantity <= 0 {
			jsonError(w, fmt.Sprintf("invalid quantity for SKU %s", item.SKU), http.StatusBadRequest)
			return
		}
		total += item.Price * float64(item.Quantity)
	}

	// Create order
	orderID := s.generateID("ord")
	order := &Order{
		ID:           orderID,
		UserID:       req.UserID,
		Items:        req.Items,
		TotalAmount:  total,
		Currency:     "USD",
		Status:       "pending",
		ShippingAddr: req.ShippingAddr,
		CreatedAt:    time.Now().UTC(),
		UpdatedAt:    time.Now().UTC(),
	}

	// Persist order using Outbox Pattern
	eventID := s.generateID("evt")
	if err := s.saveOrderWithOutbox(ctx, order, eventID); err != nil {
		s.logger.Printf("error saving order: %v", err)
		jsonError(w, "failed to create order", http.StatusInternalServerError)
		return
	}

	// Store in memory for quick access
	s.mu.Lock()
	s.orders[orderID] = order
	s.mu.Unlock()

	// Simulate publishing outbox events (in production, a background worker reads outbox)
	go s.publishOutboxEvents()

	s.logger.Printf("order created: %s (user=%s, total=%.2f)", orderID, req.UserID, total)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(OrderResponse{
		OrderID:     orderID,
		Status:      order.Status,
		TotalAmount: total,
		Message:     "Order created successfully. Processing payment and inventory...",
	})
}

func (s *OrderService) getOrderHandler(w http.ResponseWriter, r *http.Request) {
	orderID := strings.TrimPrefix(r.URL.Path, "/orders/")

	// Check memory first
	s.mu.RLock()
	order, ok := s.orders[orderID]
	s.mu.RUnlock()

	if !ok {
		// Fallback to DB
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()

		var dbOrder Order
		var itemsJSON, addrJSON string
		err := s.db.QueryRowContext(ctx,
			"SELECT id, user_id, items, total_amount, currency, status, shipping_address, created_at, updated_at FROM orders WHERE id = $1",
			orderID,
		).Scan(&dbOrder.ID, &dbOrder.UserID, &itemsJSON, &dbOrder.TotalAmount, &dbOrder.Currency, &dbOrder.Status, &addrJSON, &dbOrder.CreatedAt, &dbOrder.UpdatedAt)

		if err == sql.ErrNoRows {
			jsonError(w, "order not found", http.StatusNotFound)
			return
		}
		if err != nil {
			s.logger.Printf("error fetching order: %v", err)
			jsonError(w, "failed to fetch order", http.StatusInternalServerError)
			return
		}

		json.Unmarshal([]byte(itemsJSON), &dbOrder.Items)
		json.Unmarshal([]byte(addrJSON), &dbOrder.ShippingAddr)
		order = &dbOrder
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(order)
}

func (s *OrderService) cancelOrderHandler(w http.ResponseWriter, r *http.Request) {
	orderID := strings.TrimPrefix(r.URL.Path, "/orders/")

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	// Update order status
	result, err := s.db.ExecContext(ctx,
		"UPDATE orders SET status = 'cancelled', updated_at = NOW() WHERE id = $1 AND status = 'pending'",
		orderID,
	)
	if err != nil {
		s.logger.Printf("error cancelling order: %v", err)
		jsonError(w, "failed to cancel order", http.StatusInternalServerError)
		return
	}

	rows, _ := result.RowsAffected()
	if rows == 0 {
		jsonError(w, "order not found or already processed", http.StatusBadRequest)
		return
	}

	// Write cancellation event to outbox
	eventID := s.generateID("evt")
	cancelEvent := events.OrderCancelled{
		EventMetadata: events.NewEventMetadata(eventID, "OrderCancelled", s.config.ServiceName, orderID),
		OrderID:       orderID,
		Reason:        "user_requested",
	}

	eventJSON, _ := json.Marshal(cancelEvent)
	_, err = s.db.ExecContext(ctx,
		"INSERT INTO outbox (topic, key, payload, headers) VALUES ($1, $2, $3, $4)",
		"orders.cancelled", orderID, string(eventJSON), `{"source":"order-service"}`,
	)
	if err != nil {
		s.logger.Printf("error writing cancel outbox: %v", err)
	}

	// Update in-memory
	s.mu.Lock()
	if o, ok := s.orders[orderID]; ok {
		o.Status = "cancelled"
		o.UpdatedAt = time.Now().UTC()
	}
	s.mu.Unlock()

	go s.publishOutboxEvents()

	s.logger.Printf("order cancelled: %s", orderID)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"order_id": orderID,
		"status":   "cancelled",
		"message":  "Order cancelled successfully",
	})
}

// ─── Outbox Pattern Implementation ───────────────────────────────

func (s *OrderService) saveOrderWithOutbox(ctx context.Context, order *Order, eventID string) error {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback()

	itemsJSON, _ := json.Marshal(order.Items)
	addrJSON, _ := json.Marshal(order.ShippingAddr)

	_, err = tx.ExecContext(ctx,
		`INSERT INTO orders (id, user_id, items, total_amount, currency, status, shipping_address, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		order.ID, order.UserID, string(itemsJSON), order.TotalAmount, order.Currency, order.Status, string(addrJSON), order.CreatedAt, order.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("insert order: %w", err)
	}

	// Create OrderCreated event
	createdEvent := events.OrderCreated{
		EventMetadata: events.NewEventMetadata(eventID, "OrderCreated", s.config.ServiceName, order.ID),
		OrderID:       order.ID,
		UserID:        order.UserID,
		Items:         order.Items,
		TotalAmount:   order.TotalAmount,
		Currency:      order.Currency,
		Status:        order.Status,
		ShippingAddr:  order.ShippingAddr,
	}

	eventJSON, _ := json.Marshal(createdEvent)
	_, err = tx.ExecContext(ctx,
		"INSERT INTO outbox (topic, key, payload, headers) VALUES ($1, $2, $3, $4)",
		"orders.created", order.ID, string(eventJSON), `{"source":"order-service","event_type":"OrderCreated"}`,
	)
	if err != nil {
		return fmt.Errorf("insert outbox: %w", err)
	}

	return tx.Commit()
}

// publishOutboxEvents simulates publishing outbox events to Kafka.
// In production, this runs as a background worker that polls the outbox table.
func (s *OrderService) publishOutboxEvents() {
	ctx := context.Background()

	rows, err := s.db.QueryContext(ctx,
		"SELECT id, topic, key, payload, headers FROM outbox WHERE processed = FALSE ORDER BY created_at LIMIT 100",
	)
	if err != nil {
		s.logger.Printf("outbox query error: %v", err)
		return
	}
	defer rows.Close()

	var records []OutboxRecord
	for rows.Next() {
		var r OutboxRecord
		if err := rows.Scan(&r.ID, &r.Topic, &r.Key, &r.Payload, &r.Headers); err != nil {
			continue
		}
		records = append(records, r)
	}

	for _, r := range records {
		// Simulate Kafka publish
		s.logger.Printf("[OUTBOX-PUBLISH] topic=%s key=%s payload=%d bytes", r.Topic, r.Key, len(r.Payload))

		_, err := s.db.ExecContext(ctx,
			"UPDATE outbox SET processed = TRUE, processed_at = NOW() WHERE id = $1",
			r.ID,
		)
		if err != nil {
			s.logger.Printf("outbox mark processed error: %v", err)
		}
	}
}

// ─── Kafka Consumer (Simulated) ──────────────────────────────────

func (s *OrderService) startEventConsumer() {
	// In production, this would use a real Kafka consumer (e.g., segmentio/kafka-go)
	// listening to: payments.processed, inventory.reserved for saga coordination
	s.logger.Println("starting event consumer (simulated)...")

	// Simulate processing payment and inventory events
	// In production, this updates order status based on downstream events
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		s.mu.RLock()
		pendingCount := 0
		for _, o := range s.orders {
			if o.Status == "pending" {
				pendingCount++
			}
		}
		s.mu.RUnlock()

		if pendingCount > 0 {
			s.logger.Printf("pending orders awaiting saga completion: %d", pendingCount)
		}
	}
}

// ─── Main ────────────────────────────────────────────────────────

func main() {
	cfg := loadConfig()

	svc, err := NewOrderService(cfg)
	if err != nil {
		log.Fatalf("failed to create order service: %v", err)
	}
	defer svc.db.Close()

	// Start background event consumer
	go svc.startEventConsumer()

	// Start periodic outbox publisher
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			svc.publishOutboxEvents()
		}
	}()

	server := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      svc,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	done := make(chan os.Signal, 1)
	signal.Notify(done, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		svc.logger.Printf("Order Service starting on :%s", cfg.Port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			svc.logger.Fatalf("server error: %v", err)
		}
	}()

	<-done
	svc.logger.Println("shutting down Order Service...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		svc.logger.Printf("shutdown error: %v", err)
	}

	svc.logger.Println("Order Service stopped")
}

func jsonError(w http.ResponseWriter, message string, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": message, "status": fmt.Sprintf("%d", code)})
}
