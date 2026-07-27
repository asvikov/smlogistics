package gateway

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"math/big"
	"time"
)

// MockSmsGateway simulates an SMS delivery provider.
type MockSmsGateway struct {
	failureRate int // 0-100
	logger      *slog.Logger
}

func NewMockSmsGateway(failureRate int, logger *slog.Logger) *MockSmsGateway {
	return &MockSmsGateway{failureRate: failureRate, logger: logger}
}

func (g *MockSmsGateway) Send(ctx context.Context, recipient string, message string) (GatewayResponse, error) {
	g.logger.Info("MockSmsGateway: sending SMS",
		"recipient", recipient,
		"message", truncate(message, 50),
	)

	// Simulate network delay
	time.Sleep(50 * time.Millisecond)

	// Simulate failure
	if randPct() <= g.failureRate {
		if randPct() <= 50 {
			g.logger.Warn("MockSmsGateway: temporary failure", "recipient", recipient)
			return GatewayResponse{}, ErrTemporary
		}

		g.logger.Warn("MockSmsGateway: permanent failure", "recipient", recipient)
		return Rejected("Invalid phone number or recipient unreachable"), nil
	}

	g.logger.Info("MockSmsGateway: SMS delivered", "recipient", recipient)
	return Delivered(), nil
}

// MockEmailGateway simulates an Email delivery provider.
type MockEmailGateway struct {
	failureRate int
	logger      *slog.Logger
}

func NewMockEmailGateway(failureRate int, logger *slog.Logger) *MockEmailGateway {
	return &MockEmailGateway{failureRate: failureRate, logger: logger}
}

func (g *MockEmailGateway) Send(ctx context.Context, recipient string, message string) (GatewayResponse, error) {
	g.logger.Info("MockEmailGateway: sending Email",
		"recipient", recipient,
		"message", truncate(message, 50),
	)

	// Simulate network delay
	time.Sleep(50 * time.Millisecond)

	// Simulate failure
	if randPct() <= g.failureRate {
		if randPct() <= 50 {
			g.logger.Warn("MockEmailGateway: temporary failure", "recipient", recipient)
			return GatewayResponse{}, ErrTemporary
		}

		g.logger.Warn("MockEmailGateway: permanent failure", "recipient", recipient)
		return Rejected("Invalid email address or mailbox full"), nil
	}

	g.logger.Info("MockEmailGateway: Email delivered", "recipient", recipient)
	return Delivered(), nil
}

// ResolveGateway returns the given channel.
func ResolveGateway(channel string, smsFailureRate, emailFailureRate int, logger *slog.Logger) Gateway {
	switch channel {
	case "sms":
		return NewMockSmsGateway(smsFailureRate, logger)
	case "email":
		return NewMockEmailGateway(emailFailureRate, logger)
	default:
		return nil
	}
}

// randPct returns a pseudo-random integer in [1, 100] using crypto/rand.
func randPct() int {
	n, _ := rand.Int(rand.Reader, big.NewInt(100))
	return int(n.Int64()) + 1
}

func SecureRandomHex(n int) string {
	b := make([]byte, n/2)
	if _, err := rand.Read(b); err != nil {
		return hex.EncodeToString([]byte("fallback"))
	}
	return hex.EncodeToString(b)
}

func truncate(s string, maxLen int) string {
	runes := []rune(s)
	if len(runes) <= maxLen {
		return s
	}
	return string(runes[:maxLen])
}
