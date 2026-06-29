// Package events defines shared event types used across all microservices
// in the event-driven platform. These types represent the contract for
// all messages flowing through Kafka topics.
package events

import (
	"encoding/json"
	"time"
)

// EventMetadata contains common metadata present in all events.
type EventMetadata struct {
	EventID       string    `json:"event_id"`
	CorrelationID string    `json:"correlation_id"`
	CausationID   string    `json:"causation_id,omitempty"`
	EventType     string    `json:"event_type"`
	Source        string    `json:"source"`
	Timestamp     time.Time `json:"timestamp"`
	Version       int       `json:"version"`
}

// NewEventMetadata creates event metadata with common fields populated.
func NewEventMetadata(eventID, eventType, source, correlationID string) EventMetadata {
	return EventMetadata{
		EventID:       eventID,
		EventType:     eventType,
		Source:        source,
		CorrelationID: correlationID,
		Timestamp:     time.Now().UTC(),
		Version:       1,
	}
}

// OrderItem represents a single item within an order.
type OrderItem struct {
	SKU      string  `json:"sku"`
	Name     string  `json:"name,omitempty"`
	Quantity int     `json:"quantity"`
	Price    float64 `json:"price"`
}

// ShippingAddress represents the delivery address for an order.
type ShippingAddress struct {
	Street  string `json:"street"`
	City    string `json:"city"`
	State   string `json:"state,omitempty"`
	Country string `json:"country,omitempty"`
	ZipCode string `json:"zip"`
}

// ─── Order Events ────────────────────────────────────────────────

// OrderCreated is emitted when a new order is placed.
type OrderCreated struct {
	EventMetadata `json:"meta"`
	OrderID       string          `json:"order_id"`
	UserID        string          `json:"user_id"`
	Items         []OrderItem     `json:"items"`
	TotalAmount   float64         `json:"total_amount"`
	Currency      string          `json:"currency"`
	Status        string          `json:"status"` // pending, confirmed, cancelled
	ShippingAddr  ShippingAddress `json:"shipping_address"`
}

// OrderCancelled is emitted when an order is cancelled (compensating action).
type OrderCancelled struct {
	EventMetadata `json:"meta"`
	OrderID       string `json:"order_id"`
	UserID        string `json:"user_id"`
	Reason        string `json:"reason"`
}

// OrderConfirmed is emitted when all saga steps complete successfully.
type OrderConfirmed struct {
	EventMetadata `json:"meta"`
	OrderID       string `json:"order_id"`
	UserID        string `json:"user_id"`
}

// ─── Payment Events ──────────────────────────────────────────────

// PaymentStatus represents the state of a payment.
type PaymentStatus string

const (
	PaymentStatusPending   PaymentStatus = "pending"
	PaymentStatusCompleted PaymentStatus = "completed"
	PaymentStatusFailed    PaymentStatus = "failed"
	PaymentStatusRefunded  PaymentStatus = "refunded"
)

// PaymentProcessed is emitted when a payment is processed.
type PaymentProcessed struct {
	EventMetadata `json:"meta"`
	PaymentID     string        `json:"payment_id"`
	OrderID       string        `json:"order_id"`
	UserID        string        `json:"user_id"`
	Amount        float64       `json:"amount"`
	Currency      string        `json:"currency"`
	Status        PaymentStatus `json:"status"`
	Method        string        `json:"method"` // credit_card, debit_card, wallet
	FailureReason string        `json:"failure_reason,omitempty"`
}

// PaymentRefunded is emitted when a payment is refunded (compensating action).
type PaymentRefunded struct {
	EventMetadata `json:"meta"`
	PaymentID     string  `json:"payment_id"`
	OrderID       string  `json:"order_id"`
	UserID        string  `json:"user_id"`
	Amount        float64 `json:"amount"`
	Currency      string  `json:"currency"`
	Reason        string  `json:"reason"`
}

// ─── Inventory Events ────────────────────────────────────────────

// InventoryReserved is emitted when stock is reserved for an order.
type InventoryReserved struct {
	EventMetadata `json:"meta"`
	ReservationID string      `json:"reservation_id"`
	OrderID       string      `json:"order_id"`
	Items         []OrderItem `json:"items"`
	Status        string      `json:"status"` // reserved, failed
}

// InventoryReleased is emitted when reserved stock is released (rollback).
type InventoryReleased struct {
	EventMetadata `json:"meta"`
	ReservationID string      `json:"reservation_id"`
	OrderID       string      `json:"order_id"`
	Items         []OrderItem `json:"items"`
	Reason        string      `json:"reason"`
}

// InventoryUpdated is emitted when stock levels change.
type InventoryUpdated struct {
	EventMetadata `json:"meta"`
	SKU           string `json:"sku"`
	Quantity      int    `json:"quantity"`
	Operation     string `json:"operation"` // add, subtract, set
}

// ─── Notification Events ─────────────────────────────────────────

// NotificationChannel represents delivery channels.
type NotificationChannel string

const (
	ChannelEmail NotificationChannel = "email"
	ChannelSMS   NotificationChannel = "sms"
	ChannelPush  NotificationChannel = "push"
)

// NotificationSent is emitted when a notification is dispatched.
type NotificationSent struct {
	EventMetadata `json:"meta"`
	NotificationID string              `json:"notification_id"`
	UserID         string              `json:"user_id"`
	OrderID        string              `json:"order_id,omitempty"`
	Channel        NotificationChannel `json:"channel"`
	Subject        string              `json:"subject"`
	Body           string              `json:"body"`
	Status         string              `json:"status"` // sent, failed
}

// ─── Dead Letter Events ──────────────────────────────────────────

// DeadLetterEvent wraps a failed event for later inspection/reprocessing.
type DeadLetterEvent struct {
	EventMetadata `json:"meta"`
	OriginalTopic string          `json:"original_topic"`
	OriginalKey   string          `json:"original_key"`
	OriginalValue json.RawMessage `json:"original_value"`
	ErrorMessage  string          `json:"error_message"`
	RetryCount    int             `json:"retry_count"`
}

// ─── Event Type Registry ─────────────────────────────────────────

// EventTypeTopicMap maps event types to their Kafka topics.
var EventTypeTopicMap = map[string]string{
	"OrderCreated":        "orders.created",
	"OrderCancelled":      "orders.cancelled",
	"OrderConfirmed":      "orders.confirmed",
	"PaymentProcessed":    "payments.processed",
	"PaymentRefunded":     "payments.refunded",
	"InventoryReserved":   "inventory.reserved",
	"InventoryReleased":   "inventory.released",
	"InventoryUpdated":    "inventory.updated",
	"NotificationSent":    "notifications.sent",
	"DeadLetterEvent":     "dead-letter",
}

// GetTopicForEventType returns the Kafka topic for a given event type.
func GetTopicForEventType(eventType string) (string, bool) {
	topic, ok := EventTypeTopicMap[eventType]
	return topic, ok
}
