package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/asvikov/smlogistics/internal/config"
	"github.com/asvikov/smlogistics/internal/domain"
	"github.com/asvikov/smlogistics/internal/gateway"
	"github.com/asvikov/smlogistics/internal/store"
)

type Consumer struct {
	cfg        *config.Config
	store      *store.PGStore
	conn       *amqp.Connection
	ch         *amqp.Channel
	logger     *slog.Logger
	maxRetries int
}

func NewConsumer(cfg *config.Config, pgStore *store.PGStore, logger *slog.Logger) (*Consumer, error) {
	conn, err := amqp.Dial(cfg.RabbitMQURL())
	if err != nil {
		return nil, fmt.Errorf("worker: dial rabbitmq: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("worker: open channel: %w", err)
	}

	// Declare the durable queue with priority support
	// Queue is created in docker/rabbitmq/definitions.json
	_, err = ch.QueueDeclare(
		cfg.RabbitMQQueue,
		cfg.RabbitMQDurable,
		cfg.RabbitMQAutoDelete,
		cfg.RabbitMQExclusive,
		cfg.RabbitMQNoWait,
		amqp.Table{
			"x-max-priority": cfg.RabbitMQMaxPriority,
			"x-queue-type":   cfg.RabbitMQQueueType,
		},
	)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("worker: declare queue: %w", err)
	}

	// Fair dispatch: one message per consumer
	if err := ch.Qos(1, 0, false); err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("worker: set qos: %w", err)
	}

	return &Consumer{
		cfg:        cfg,
		store:      pgStore,
		conn:       conn,
		ch:         ch,
		logger:     logger,
		maxRetries: cfg.MaxRetries,
	}, nil
}

// Close shuts down the RabbitMQ connection.
func (c *Consumer) Close() {
	if c.ch != nil {
		c.ch.Close()
	}
	if c.conn != nil {
		c.conn.Close()
	}
}

// matches the dispatch service's message.
// TODO: maybe create a common one
type JobMessage struct {
	NotificationID int64  `json:"notification_id"`
	SubscriberID   string `json:"subscriber_id"`
	Channel        string `json:"channel"`
	Message        string `json:"message"`
	Priority       int    `json:"priority"`
	Attempts       int    `json:"attempts"`
}

// Run starts consuming messages in a blocking loop.
// It handles SIGTERM/SIGINT for graceful shutdown. But
// TODO: maybe place it in main.go (how for APIServer)
func (c *Consumer) Run(ctx context.Context) error {
	msgs, err := c.ch.Consume(
		c.cfg.RabbitMQQueue,
		"",    // consumer tag
		false, // auto-ack (manual ack for reliability)
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("worker: consume: %w", err)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)

	c.logger.Info("worker: started consuming", "queue", c.cfg.RabbitMQQueue)

	for {
		select {
		case <-ctx.Done():
			c.logger.Info("worker: context cancelled, shutting down")
			return nil

		case s := <-sig:
			c.logger.Info("worker: received signal, shutting down", "signal", s)
			return nil

		case msg, ok := <-msgs:
			if !ok {
				c.logger.Info("worker: channel closed")
				return nil
			}
			c.processMessage(ctx, msg)
		}
	}
}

func (c *Consumer) processMessage(ctx context.Context, msg amqp.Delivery) {
	var job JobMessage
	if err := json.Unmarshal(msg.Body, &job); err != nil {
		c.logger.Error("worker: failed to parse message", "error", err)
		msg.Nack(false, false) // reject, don't requeue
		return
	}

	logger := c.logger.With("notification_id", job.NotificationID, "channel", job.Channel)

	notification, err := c.store.GetByID(ctx, job.NotificationID)
	if err != nil {
		logger.Error("worker: failed to load notification", "error", err)
		msg.Nack(false, true) // requeue
		return
	}

	// check: if already delivered or rejected, skip
	if notification.IsTerminal() {
		logger.Info("worker: skipping terminal notification", "status", notification.Status)
		msg.Ack(false)
		return
	}

	attempts := notification.Attempts + 1

	gw := gateway.ResolveGateway(job.Channel, c.cfg.MockSMSFailureRate, c.cfg.MockEmailFailureRate, c.logger)
	if gw == nil {
		logger.Error("worker: unknown channel")
		if err := c.store.UpdateStatus(ctx, notification.ID, domain.StatusRejected, attempts, map[string]any{
			"error": "Unknown channel: " + job.Channel,
		}); err != nil {
			logger.Error("worker: failed to update status", "error", err)
		}
		msg.Ack(false)
		return
	}

	resp, err := gw.Send(ctx, notification.SubscriberID, notification.Message)
	if err != nil {
		if err == gateway.ErrTemporary {
			logger.Warn("worker: temporary failure", "attempts", attempts, "max_retries", c.maxRetries)
			if attempts < c.maxRetries {
				// Update attempt count but keep status queued
				if err := c.store.UpdateStatus(ctx, notification.ID, domain.StatusQueued, attempts, nil); err != nil {
					logger.Error("worker: failed to update attempts", "error", err)
				}
				msg.Nack(false, true) // requeue
				return
			}

			logger.Error("worker: max retries exhausted")
			if err := c.store.UpdateStatus(ctx, notification.ID, domain.StatusRejected, attempts, map[string]any{
				"error": "Max retries exhausted: " + err.Error(),
			}); err != nil {
				logger.Error("worker: failed to reject", "error", err)
			}
			msg.Ack(false)
			return
		}

		// Unexpected error
		logger.Error("worker: unexpected gateway error", "error", err)
		if err := c.store.UpdateStatus(ctx, notification.ID, domain.StatusRejected, attempts, map[string]any{
			"error": err.Error(),
		}); err != nil {
			logger.Error("worker: failed to update status", "error", err)
		}
		msg.Ack(false)
		return
	}

	// Handle response
	if resp.Success {
		gwResp := map[string]any{
			"success":             resp.Success,
			"status":              resp.Status,
			"provider_message_id": resp.ProviderMessageID,
		}
		if err := c.store.UpdateStatus(ctx, notification.ID, domain.StatusDelivered, attempts, gwResp); err != nil {
			logger.Error("worker: failed to mark delivered", "error", err)
		}
		msg.Ack(false)
		logger.Info("worker: notification delivered")
	} else {
		gwResp := map[string]any{
			"success": false,
			"status":  resp.Status,
			"error":   resp.ErrorMessage,
		}
		if resp.Raw != nil {
			for k, v := range resp.Raw {
				gwResp[k] = v
			}
		}
		if err := c.store.UpdateStatus(ctx, notification.ID, domain.StatusRejected, attempts, gwResp); err != nil {
			logger.Error("worker: failed to reject", "error", err)
		}
		msg.Ack(false)
		logger.Warn("worker: notification rejected", "reason", resp.ErrorMessage)
	}
}
