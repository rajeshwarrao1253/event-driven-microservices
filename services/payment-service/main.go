// Payment Service handles payment processing for orders.
// It consumes order events from Kafka, processes payments,
// and emits payment events for saga coordination.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
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
	ServiceName  string
	Port         string
	DBHost       string
	DBPort       string
	DBName       string
	DBUser       string
	DBPassword   string
	KafkaBrokers string
	LogLevel     string
}

func loadConfig() Config {
	return Config{
		ServiceName:  getEnv("SERVICE_NAME", "payment-service"),
		Port:         getEnv("HTTP_PORT", "8082"),
		DBHost:       getEnv("DB_HOST", "localhost"),
		DBPort:       getEnv("DB_PORT", "5432"),
		DBName:       getEnv("DB_NAME", "payments"),
		DBUser:       getEnv("DB_USER", "payment_user"),
		DBPassword:   getEnv("DB_PASSWORD", "payment_pass"),
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

type Payment struct {
	ID            string               `json:"payment_id"`
	OrderID       string               `json:"order_id"`
	UserID        string               `json:"user_id"`
	Amount        float64              `json:"amount"`
	Currency      string               `json:"currency"`
	Status        events.PaymentStatus `json:"status"`
	Method        string               `json:"method"`
	FailureReason string               `json:"failure_reason,omitempty"`
	TransactionID string               `json:"transaction_id,omitempty"`
	ProcessedAt   *time.Time           `json:"processed_at,omitempty"`
	CreatedAt     time.Time            `json:"created_at"`
	UpdatedAt     time.Time            `json:"updated_at"`
}

type PaymentMethod string

const (
	MethodCreditCard PaymentMethod = "credit_card"
	MethodDebitCard  PaymentMethod = "debit_card"
	MethodWallet     PaymentMethod = "wallet"
)

// ─── Application ─────────────────────────────────────────────────

type PaymentService struct {
	config      Config
	db          *sql.DB
	logger      *log.Logger
	idempotency *IdempotencyStore
	mu          sync.RWMutex
	payments    map[string]*Payment
	// Simulated Kafka consumer channels
	eventCh chan OrderEvent
}

type OrderEvent struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

func NewPaymentService(cfg Config) (*PaymentService, error) {
	logger := log.New(os.Stdout, fmt.Sprintf("[%s] ", cfg.ServiceName), log.LstdFlags|log.Lmicroseconds)

	dsn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		cfg.DBHost, cfg.DBPort, cfg.DBUser, cfg.DBPassword, cfg.DBName)

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	svc := &PaymentService{
		config:      cfg,
		db:          db,
		logger:      logger,
		idempotency: NewIdempotencyStore(24 * time.Hour),
		payments:    make(map[string]*Payment),
		eventCh:     make(chan OrderEvent, 100),
	}

	if err := svc.initSchema(ctx); err != nil {
		return nil, fmt.Errorf("failed to init schema: %w", err)
	}

	logger.Println("payment service initialized successfully")
	return svc, nil
}

func (s *PaymentService) initSchema(ctx context.Context) error {
	schema := `
	CREATE TABLE IF NOT EXISTS payments (
		id VARCHAR(64) PRIMARY KEY,
		order_id VARCHAR(64) NOT NULL,
		user_id VARCHAR(64) NOT NULL,
		amount DECIMAL(12,2) NOT NULL,
		currency VARCHAR(3) DEFAULT 'USD',
		status VARCHAR(20) NOT NULL DEFAULT 'pending',
		method VARCHAR(30) NOT NULL DEFAULT 'credit_card',
		failure_reason TEXT,
		transaction_id VARCHAR(128),
		processed_at TIMESTAMP WITH TIME ZONE,
		created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
		updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
	);

	CREATE INDEX IF NOT EXISTS idx_payments_order_id ON payments(order_id);
	CREATE INDEX IF NOT EXISTS idx_payments_status ON payments(status);
	CREATE INDEX IF NOT EXISTS idx_payments_user_id ON payments(user_id);

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

	CREATE TABLE IF NOT EXISTS processed_events (
		event_id VARCHAR(64) PRIMARY KEY,
		processed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
	);
	`

	_, err := s.db.ExecContext(ctx, schema)
	return err
}

func (s *PaymentService) generateID(prefix string) string {
	return fmt.Sprintf("%s-%d-%d", prefix, time.Now().UnixNano(), os.Getpid())
}

// ─── Idempotency Store ───────────────────────────────────────────

type IdempotencyStore struct {
	mu   sync.RWMutex
	keys map[string]time.Time	ttl  time.Duration
}

func NewIdempotencyStore(ttl time.Duration) *IdempotencyStore {
	store := &IdempotencyStore{
		keys: make(map[string]time.Time),
		ttl:  ttl,
	}
	go store.cleanupLoop()
	return store
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

// ─── HTTP Handlers ───────────────────────────────────────────────

func (s *PaymentService) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	method := r.Method

	switch {
	case path == "/health":
		s.healthHandler(w, r)
	case strings.HasPrefix(path, "/payments/") && method == http.MethodGet:
		s.getPaymentHandler(w, r)
	case path == "/webhook/orders" && method == http.MethodPost:
		s.handleOrderWebhook(w, r)
	default:
		jsonError(w, "not found", http.StatusNotFound)
	}
}

func (s *PaymentService) healthHandler(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	dbStatus := "healthy"
	if err := s.db.PingContext(ctx); err != nil {
		dbStatus = "unhealthy: " + err.Error()
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":    "healthy",
		"service":   s.config.ServiceName,
		"database":  dbStatus,
		"timestamp": time.Now().UTC().Format(time.RFC3339),
		"version":   "1.0.0",
	})
}

func (s *PaymentService) getPaymentHandler(w http.ResponseWriter, r *http.Request) {
	paymentID := strings.TrimPrefix(r.URL.Path, "/payments/")

	// Check in-memory first
	s.mu.RLock()
	payment, ok := s.payments[paymentID]
	s.mu.RUnlock()

	if !ok {
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()

		var dbPayment Payment
		var processedAt sql.NullTime
		err := s.db.QueryRowContext(ctx,
			"SELECT id, order_id, user_id, amount, currency, status, method, failure_reason, transaction_id, processed_at, created_at, updated_at FROM payments WHERE id = $1",
			paymentID,
		).Scan(&dbPayment.ID, &dbPayment.OrderID, &dbPayment.UserID, &dbPayment.Amount, &dbPayment.Currency, &dbPayment.Status, &dbPayment.Method, &dbPayment.FailureReason, &dbPayment.TransactionID, &processedAt, &dbPayment.CreatedAt, &dbPayment.UpdatedAt)

		if err == sql.ErrNoRows {
			jsonError(w, "payment not found", http.StatusNotFound)
			return
		}
		if err != nil {
			s.logger.Printf("error fetching payment: %v", err)
			jsonError(w, "failed to fetch payment", http.StatusInternalServerError)
			return
		}
		if processedAt.Valid {
			dbPayment.ProcessedAt = &processedAt.Time
		}
		payment = &dbPayment
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(payment)
}

func (s *PaymentService) handleOrderWebhook(w http.ResponseWriter, r *http.Request) {
	var event OrderEvent
	if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
		jsonError(w, "invalid event body", http.StatusBadRequest)
		return
	}

	s.eventCh <- event

	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]string{"status": "accepted"})
}

// ─── Payment Processing ──────────────────────────────────────────

func (s *PaymentService) startEventProcessor() {
	s.logger.Println("starting payment event processor...")

	for event := range s.eventCh {
		switch event.Type {
		case "OrderCreated":
			var orderCreated events.OrderCreated
			if err := json.Unmarshal(event.Payload, &orderCreated); err != nil {
				s.logger.Printf("error unmarshalling OrderCreated: %v", err)
				continue
			}
			s.processPayment(orderCreated)

		case "OrderCancelled":
			var orderCancelled events.OrderCancelled
			if err := json.Unmarshal(event.Payload, &orderCancelled); err != nil {
				s.logger.Printf("error unmarshalling OrderCancelled: %v", err)
				continue
			}
			s.processRefund(orderCancelled)

		default:
			s.logger.Printf("unknown event type: %s", event.Type)
		}
	}
}

func (s *PaymentService) processPayment(order events.OrderCreated) {
	ctx := context.Background()

	// Idempotency check
	if s.idempotency.IsProcessed(order.EventMetadata.EventID) {
		s.logger.Printf("event already processed: %s", order.EventMetadata.EventID)
		return
	}

	s.logger.Printf("processing payment for order: %s (amount=%.2f %s)", order.OrderID, order.TotalAmount, order.Currency)

	// Check if payment already exists for this order
	var existingID string
	err := s.db.QueryRowContext(ctx, "SELECT id FROM payments WHERE order_id = $1", order.OrderID).Scan(&existingID)
	if err == nil && existingID != "" {
		s.logger.Printf("payment already exists for order %s: %s", order.OrderID, existingID)
		s.idempotency.MarkProcessed(order.EventMetadata.EventID)
		return
	}

	// Create payment record
	paymentID := s.generateID("pay")
	payment := &Payment{
		ID:        paymentID,
		OrderID:   order.OrderID,
		UserID:    order.UserID,
		Amount:    order.TotalAmount,
		Currency:  order.Currency,
		Status:    events.PaymentStatusPending,
		Method:    string(MethodCreditCard),
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}

	// Persist payment and publish event via outbox
	eventID := s.generateID("evt")
	if err := s.savePaymentWithOutbox(ctx, payment, eventID); err != nil {
		s.logger.Printf("error saving payment: %v", err)
		return
	}

	// Simulate payment gateway call
	// In production, this would call Stripe/Braintree/PayPal
	time.Sleep(100 * time.Millisecond) // Simulate network latency

	success := s.simulatePaymentGateway(payment)
	now := time.Now().UTC()

	if success {
		payment.Status = events.PaymentStatusCompleted
		payment.TransactionID = s.generateID("txn")
		payment.ProcessedAt = &now
		s.logger.Printf("payment successful: %s (order=%s, txn=%s)", paymentID, order.OrderID, payment.TransactionID)
	} else {
		payment.Status = events.PaymentStatusFailed
		payment.FailureReason = "insufficient_funds"
		s.logger.Printf("payment failed: %s (order=%s, reason=insufficient_funds)", paymentID, order.OrderID)
	}

	// Update payment status
	_, err = s.db.ExecContext(ctx,
		"UPDATE payments SET status = $1, transaction_id = $2, failure_reason = $3, processed_at = $4, updated_at = $5 WHERE id = $6",
		payment.Status, payment.TransactionID, payment.FailureReason, payment.ProcessedAt, now, payment.ID,
	)
	if err != nil {
		s.logger.Printf("error updating payment status: %v", err)
		return
	}

	// Publish payment event
	s.publishPaymentEvent(payment, eventID)

	// Store in memory
	s.mu.Lock()
	s.payments[paymentID] = payment
	s.mu.Unlock()

	s.idempotency.MarkProcessed(order.EventMetadata.EventID)
}

func (s *PaymentService) processRefund(cancel events.OrderCancelled) {
	ctx := context.Background()

	s.logger.Printf("processing refund for cancelled order: %s", cancel.OrderID)

	// Find the payment
	var payment Payment
	var processedAt sql.NullTime
	err := s.db.QueryRowContext(ctx,
		"SELECT id, order_id, user_id, amount, currency, status, transaction_id, processed_at FROM payments WHERE order_id = $1",
		cancel.OrderID,
	).Scan(&payment.ID, &payment.OrderID, &payment.UserID, &payment.Amount, &payment.Currency, &payment.Status, &payment.TransactionID, &processedAt)

	if err == sql.ErrNoRows {
		s.logger.Printf("no payment found for order %s, nothing to refund", cancel.OrderID)
		return
	}
	if err != nil {
		s.logger.Printf("error finding payment: %v", err)
		return
	}
	if processedAt.Valid {
		payment.ProcessedAt = &processedAt.Time
	}

	// Only refund completed payments
	if payment.Status != events.PaymentStatusCompleted {
		s.logger.Printf("payment %s status is %s, skipping refund", payment.ID, payment.Status)
		return
	}

	// Process refund
	refundID := s.generateID("ref")
	now := time.Now().UTC()

	_, err = s.db.ExecContext(ctx,
		"UPDATE payments SET status = $1, updated_at = $2 WHERE id = $3",
		events.PaymentStatusRefunded, now, payment.ID,
	)
	if err != nil {
		s.logger.Printf("error updating payment to refunded: %v", err)
		return
	}

	// Publish refund event
	eventID := s.generateID("evt")
	refundEvent := events.PaymentRefunded{
		EventMetadata: events.NewEventMetadata(eventID, "PaymentRefunded", s.config.ServiceName, cancel.OrderID),
		PaymentID:     payment.ID,
		OrderID:       cancel.OrderID,
		UserID:        payment.UserID,
		Amount:        payment.Amount,
		Currency:      payment.Currency,
		Reason:        "order_cancelled",
	}

	if err := s.saveEventToOutbox(ctx, "payments.refunded", cancel.OrderID, refundEvent); err != nil {
		s.logger.Printf("error saving refund outbox: %v", err)
		return
	}

	go s.publishOutboxEvents()

	s.logger.Printf("refund processed: %s (payment=%s, amount=%.2f)", refundID, payment.ID, payment.Amount)
}

// simulatePaymentGateway simulates a payment gateway with 90% success rate.
func (s *PaymentService) simulatePaymentGateway(payment *Payment) bool {
	// Simulate fraud detection
	if payment.Amount > 10000 {
		return false // Flag large transactions
	}

	// 90% success rate
	return rand.Float32() < 0.9
}

// ─── Outbox Pattern ──────────────────────────────────────────────

func (s *PaymentService) savePaymentWithOutbox(ctx context.Context, payment *Payment, eventID string) error {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(ctx,
		`INSERT INTO payments (id, order_id, user_id, amount, currency, status, method, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		payment.ID, payment.OrderID, payment.UserID, payment.Amount, payment.Currency, payment.Status, payment.Method, payment.CreatedAt, payment.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("insert payment: %w", err)
	}

	return tx.Commit()
}

func (s *PaymentService) publishPaymentEvent(payment *Payment, eventID string) {
	ctx := context.Background()

	processedEvent := events.PaymentProcessed{
		EventMetadata: events.NewEventMetadata(eventID, "PaymentProcessed", s.config.ServiceName, payment.OrderID),
		PaymentID:     payment.ID,
		OrderID:       payment.OrderID,
		UserID:        payment.UserID,
		Amount:        payment.Amount,
		Currency:      payment.Currency,
		Status:        payment.Status,
		Method:        payment.Method,
		FailureReason: payment.FailureReason,
	}

	if err := s.saveEventToOutbox(ctx, "payments.processed", payment.OrderID, processedEvent); err != nil {
		s.logger.Printf("error saving payment outbox: %v", err)
		return
	}

	go s.publishOutboxEvents()
}

func (s *PaymentService) saveEventToOutbox(ctx context.Context, topic, key string, event interface{}) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	_, err = s.db.ExecContext(ctx,
		"INSERT INTO outbox (topic, key, payload, headers) VALUES ($1, $2, $3, $4)",
		topic, key, string(payload), fmt.Sprintf(`{"source":"%s"}`, s.config.ServiceName),
	)
	return err
}

func (s *PaymentService) publishOutboxEvents() {
	ctx := context.Background()

	rows, err := s.db.QueryContext(ctx,
		"SELECT id, topic, key, payload, headers FROM outbox WHERE processed = FALSE ORDER BY created_at LIMIT 100",
	)
	if err != nil {
		s.logger.Printf("outbox query error: %v", err)
		return
	}
	defer rows.Close()

	type outboxRec struct {
		ID      int64
		Topic   string
		Key     string
		Payload string
	}

	var records []outboxRec
	for rows.Next() {
		var r outboxRec
		var headers string
		if err := rows.Scan(&r.ID, &r.Topic, &r.Key, &r.Payload, &headers); err != nil {
			continue
		}
		records = append(records, r)
	}

	for _, r := range records {
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

// ─── Simulated Kafka Consumer ────────────────────────────────────

func (s *PaymentService) startSimulatedConsumer() {
	s.logger.Println("starting simulated Kafka consumer for orders.created topic...")

	// In production, this uses kafka-go to consume from orders.created
	// For demo, we simulate receiving events periodically
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		// Poll for pending payments and simulate event processing
		s.checkPendingPayments()
	}
}

func (s *PaymentService) checkPendingPayments() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rows, err := s.db.QueryContext(ctx,
		"SELECT order_id, user_id, amount, currency FROM payments WHERE status = 'pending' AND created_at < NOW() - INTERVAL '30 seconds'",
	)
	if err != nil {
		s.logger.Printf("error checking pending payments: %v", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var orderID, userID, currency string
		var amount float64
		if err := rows.Scan(&orderID, &userID, &amount, &currency); err != nil {
			continue
		}

		s.logger.Printf("found stale pending payment for order %s, retrying...", orderID)

		// Simulate processing
		orderEvent := events.OrderCreated{
			OrderID:     orderID,
			UserID:      userID,
			TotalAmount: amount,
			Currency:    currency,
		}
		orderJSON, _ := json.Marshal(orderEvent)
		s.eventCh <- OrderEvent{Type: "OrderCreated", Payload: orderJSON}
	}
}

// ─── Main ────────────────────────────────────────────────────────

func main() {
	rand.Seed(time.Now().UnixNano())

	cfg := loadConfig()

	svc, err := NewPaymentService(cfg)
	if err != nil {
		log.Fatalf("failed to create payment service: %v", err)
	}
	defer svc.db.Close()

	// Start event processor
	go svc.startEventProcessor()

	// Start simulated consumer
	go svc.startSimulatedConsumer()

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
		svc.logger.Printf("Payment Service starting on :%s", cfg.Port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			svc.logger.Fatalf("server error: %v", err)
		}
	}()

	<-done
	svc.logger.Println("shutting down Payment Service...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		svc.logger.Printf("shutdown error: %v", err)
	}

	close(svc.eventCh)
	svc.logger.Println("Payment Service stopped")
}

func jsonError(w http.ResponseWriter, message string, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": message, "status": fmt.Sprintf("%d", code)})
}
