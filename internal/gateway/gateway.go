package gateway

import (
	"context"
	"errors"
)

// ErrTemporary is the sentinel error returned when a gateway experiences
// a retryable (temporary) failure.
var ErrTemporary = errors.New("gateway temporarily unavailable")

type GatewayResponse struct {
	Success           bool           `json:"success"`
	Status            string         `json:"status"`
	ProviderMessageID string         `json:"provider_message_id,omitempty"`
	ErrorMessage      string         `json:"error_message,omitempty"`
	Raw               map[string]any `json:"raw,omitempty"`
}

func Delivered() GatewayResponse {
	return GatewayResponse{
		Success:           true,
		Status:            "delivered",
		ProviderMessageID: "msg_" + randomHex(16),
	}
}

func Rejected(errorMessage string) GatewayResponse {
	return GatewayResponse{
		Success:      false,
		Status:       "rejected",
		ErrorMessage: errorMessage,
	}
}

// Gateway is the interface that all notification channel gateways implement.
type Gateway interface {
	Send(ctx context.Context, recipient string, message string) (GatewayResponse, error)
}

// TODO production should use rand.
func randomHex(n int) string {
	const hexChars = "0123456789abcdef"
	b := make([]byte, n)
	for i := range b {
		b[i] = hexChars[i%len(hexChars)]
	}
	return string(b)
}
