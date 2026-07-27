package domain

import (
	"time"
)

type Channel string

const (
	ChannelSMS   Channel = "sms"
	ChannelEmail Channel = "email"
)

func ValidChannel(s string) bool {
	switch Channel(s) {
	case ChannelSMS, ChannelEmail:
		return true
	}
	return false
}

type Status string

const (
	StatusQueued    Status = "queued"
	StatusSent      Status = "sent"
	StatusDelivered Status = "delivered"
	StatusRejected  Status = "rejected"
)

// IsTerminal returns true if the status is a final (non-retryable) state.
func (s Status) IsTerminal() bool {
	return s == StatusDelivered || s == StatusRejected
}

func ValidStatus(s string) bool {
	switch Status(s) {
	case StatusQueued, StatusSent, StatusDelivered, StatusRejected:
		return true
	}
	return false
}

type Priority int

const (
	PriorityTransactional Priority = 9
	PriorityMarketing     Priority = 2
	PriorityDefault       Priority = 5
)

// PriorityFromString maps a string priority label to its numeric value.
func PriorityFromString(p string) Priority {
	switch p {
	case "transactional":
		return PriorityTransactional
	case "marketing":
		return PriorityMarketing
	default:
		return PriorityDefault
	}
}

// Notification is the core domain entity.
type Notification struct {
	ID              int64          `json:"id"`
	SubscriberID    string         `json:"subscriber_id"`
	Channel         Channel        `json:"channel"`
	Message         string         `json:"message"`
	Status          Status         `json:"status"`
	Priority        Priority       `json:"priority"`
	IdempotencyKey  string         `json:"idempotency_key"`
	BatchID         string         `json:"batch_id"`
	Attempts        int            `json:"attempts"`
	GatewayResponse map[string]any `json:"gateway_response,omitempty"`
	CreatedAt       time.Time      `json:"created_at"`
	UpdatedAt       time.Time      `json:"updated_at"`
}

func (n *Notification) IsTerminal() bool {
	return n.Status.IsTerminal()
}
