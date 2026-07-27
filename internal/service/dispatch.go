package service

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/asvikov/smlogistics/internal/domain"
	"github.com/asvikov/smlogistics/internal/store"
)

type DispatchRequest struct {
	Channel        string
	Message        string
	Recipients     []string
	Priority       string
	IdempotencyKey string
}

// a successful dispatch.
type DispatchResult struct {
	BatchID       string `json:"batch_id"`
	AcceptedCount int    `json:"accepted_count"`
}

type DispatchService struct {
	store     *store.PGStore
	amqpCh    *amqp.Channel
	queueName string
	logger    *slog.Logger
}

func NewDispatchService(pgStore *store.PGStore, amqpCh *amqp.Channel, queueName string, logger *slog.Logger) *DispatchService {
	return &DispatchService{
		store:     pgStore,
		amqpCh:    amqpCh,
		queueName: queueName,
		logger:    logger,
	}
}

func (s *DispatchService) DispatchBulk(ctx context.Context, req DispatchRequest) (DispatchResult, error) {
	batchID := uuid.New().String()
	priorityNum := domain.PriorityFromString(req.Priority)

	var records []domain.Notification
	for _, recipient := range req.Recipients {
		perKey := hashSHA256(req.IdempotencyKey + ":" + recipient + ":" + req.Channel)
		records = append(records, domain.Notification{
			SubscriberID:   recipient,
			Channel:        domain.Channel(req.Channel),
			Message:        req.Message,
			Status:         domain.StatusQueued,
			Priority:       priorityNum,
			IdempotencyKey: perKey,
			BatchID:        batchID,
			Attempts:       0,
		})
	}

	acceptedCount, err := s.store.BulkUpsertNotifications(ctx, records)
	if err != nil {
		s.logger.Error("dispatch: bulk upsert failed", "error", err, "batch_id", batchID)
		return DispatchResult{}, fmt.Errorf("dispatch: bulk upsert: %w", err)
	}

	s.logger.Info("dispatch: records upserted",
		"batch_id", batchID,
		"total", len(records),
		"accepted", acceptedCount,
	)

	queued, err := s.store.GetQueuedByBatch(ctx, batchID, 100)
	if err != nil {
		return DispatchResult{}, fmt.Errorf("dispatch: fetch queued: %w", err)
	}

	for _, n := range queued {
		if err := s.publishToQueue(n); err != nil {
			s.logger.Error("dispatch: publish failed", "error", err, "notification_id", n.ID)
			// Continue publishing others; a single failure shouldn't block the batch
		}
	}

	return DispatchResult{
		BatchID:       batchID,
		AcceptedCount: acceptedCount,
	}, nil
}

// JobMessage is the JSON envelope published to RabbitMQ.
type JobMessage struct {
	NotificationID int64  `json:"notification_id"`
	SubscriberID   string `json:"subscriber_id"`
	Channel        string `json:"channel"`
	Message        string `json:"message"`
	Priority       int    `json:"priority"`
	Attempts       int    `json:"attempts"`
}

func (s *DispatchService) publishToQueue(n domain.Notification) error {
	body := JobMessage{
		NotificationID: n.ID,
		SubscriberID:   n.SubscriberID,
		Channel:        string(n.Channel),
		Message:        n.Message,
		Priority:       int(n.Priority),
		Attempts:       n.Attempts,
	}

	data, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("dispatch: marshal: %w", err)
	}

	return s.amqpCh.Publish(
		"",          // exchange — use default exchange
		s.queueName, // routing key = queue name
		false,       // mandatory
		false,       // immediate
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "application/json",
			Priority:     uint8(n.Priority),
			Body:         data,
		},
	)
}

func hashSHA256(s string) string {
	h := sha256.Sum256([]byte(s))
	return fmt.Sprintf("%x", h)
}
