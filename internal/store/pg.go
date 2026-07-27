package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/asvikov/smlogistics/internal/domain"
)

type PGStore struct {
	pool *pgxpool.Pool
}

func NewPGStore(ctx context.Context, dsn string) (*PGStore, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("store: parse config: %w", err)
	}
	cfg.MaxConns = 20

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("store: connect: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("store: ping: %w", err)
	}

	return &PGStore{pool: pool}, nil
}

// Close pool.
func (s *PGStore) Close() {
	s.pool.Close()
}

// health checks.
func (s *PGStore) Pool() *pgxpool.Pool {
	return s.pool
}

func (s *PGStore) BulkUpsertNotifications(ctx context.Context, notifications []domain.Notification) (int, error) {
	if len(notifications) == 0 {
		return 0, nil
	}

	rows := make([][]any, len(notifications))
	for i, n := range notifications {
		rows[i] = []any{
			n.SubscriberID,
			n.Channel,
			n.Message,
			n.Status,
			int(n.Priority),
			n.IdempotencyKey,
			n.BatchID,
			n.Attempts,
		}
	}

	_, err := s.pool.CopyFrom(
		ctx,
		pgx.Identifier{"notifications"},
		[]string{"subscriber_id", "channel", "message", "status", "priority", "idempotency_key", "batch_id", "attempts"},
		pgx.CopyFromRows(rows),
	)
	if err != nil {
		// On conflict (duplicate idempotency_key), fall back to individual upserts
		return s.fallbackUpsert(ctx, notifications)
	}

	return len(notifications), nil
}

func (s *PGStore) fallbackUpsert(ctx context.Context, notifications []domain.Notification) (int, error) {
	query := `
		INSERT INTO notifications (subscriber_id, channel, message, status, priority, idempotency_key, batch_id, attempts, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW(), NOW())
		ON CONFLICT (idempotency_key) DO UPDATE SET
			message = EXCLUDED.message,
			priority = EXCLUDED.priority,
			updated_at = NOW()`

	accepted := 0
	for _, n := range notifications {
		tag, err := s.pool.Exec(ctx, query,
			n.SubscriberID, n.Channel, n.Message, n.Status,
			int(n.Priority), n.IdempotencyKey, n.BatchID, n.Attempts,
		)
		if err != nil {
			continue // skip duplicates silently
		}
		if tag.RowsAffected() > 0 {
			accepted++
		}
	}
	return accepted, nil
}

func (s *PGStore) GetQueuedByBatch(ctx context.Context, batchID string, limit int) ([]domain.Notification, error) {
	query := `
		SELECT id, subscriber_id, channel, message, status, priority, idempotency_key, batch_id,
		       attempts, gateway_response, created_at, updated_at
		FROM notifications
		WHERE batch_id = $1 AND status = 'queued'
		ORDER BY priority DESC, id ASC
		LIMIT $2`

	return s.queryNotifications(ctx, query, batchID, limit)
}

func (s *PGStore) GetByID(ctx context.Context, id int64) (*domain.Notification, error) {
	query := `
		SELECT id, subscriber_id, channel, message, status, priority, idempotency_key, batch_id,
		       attempts, gateway_response, created_at, updated_at
		FROM notifications
		WHERE id = $1`

	rows, err := s.pool.Query(ctx, query, id)
	if err != nil {
		return nil, fmt.Errorf("store: get by id: %w", err)
	}
	defer rows.Close()

	notifications, err := scanNotifications(rows)
	if err != nil {
		return nil, err
	}
	if len(notifications) == 0 {
		return nil, fmt.Errorf("store: notification %d not found", id)
	}
	return &notifications[0], nil
}

// GetBySubscriber returns paginated notifications for a subscriber with optional filters.
func (s *PGStore) GetBySubscriber(ctx context.Context, subscriberID string, channel *domain.Channel, status *domain.Status, limit, offset int) ([]domain.Notification, int, error) {
	args := []any{subscriberID}
	argIdx := 2

	whereClause := "WHERE subscriber_id = $1"

	if channel != nil {
		whereClause += fmt.Sprintf(" AND channel = $%d", argIdx)
		args = append(args, string(*channel))
		argIdx++
	}
	if status != nil {
		whereClause += fmt.Sprintf(" AND status = $%d", argIdx)
		args = append(args, string(*status))
		argIdx++
	}

	// Count total
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM notifications %s", whereClause)
	var total int
	if err := s.pool.QueryRow(ctx, countQuery, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("store: count subscribers: %w", err)
	}

	// Fetch page
	args = append(args, limit, offset)
	dataQuery := fmt.Sprintf(`
		SELECT id, subscriber_id, channel, message, status, priority, idempotency_key, batch_id,
		       attempts, gateway_response, created_at, updated_at
		FROM notifications %s
		ORDER BY created_at DESC
		LIMIT $%d OFFSET $%d`, whereClause, argIdx, argIdx+1)

	notifications, err := s.queryNotifications(ctx, dataQuery, args...)
	if err != nil {
		return nil, 0, err
	}

	return notifications, total, nil
}

func (s *PGStore) UpdateStatus(ctx context.Context, id int64, status domain.Status, attempts int, gatewayResponse map[string]any) error {
	query := `
		UPDATE notifications
		SET status = $2, attempts = $3, gateway_response = $4, updated_at = NOW()
		WHERE id = $1`

	_, err := s.pool.Exec(ctx, query, id, string(status), attempts, gatewayResponse)
	if err != nil {
		return fmt.Errorf("store: update status: %w", err)
	}
	return nil
}

func (s *PGStore) queryNotifications(ctx context.Context, query string, args ...any) ([]domain.Notification, error) {
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("store: query: %w", err)
	}
	defer rows.Close()
	return scanNotifications(rows)
}

func scanNotifications(rows pgx.Rows) ([]domain.Notification, error) {
	var notifications []domain.Notification
	for rows.Next() {
		var n domain.Notification
		var gwResp map[string]any
		err := rows.Scan(
			&n.ID, &n.SubscriberID, &n.Channel, &n.Message, &n.Status,
			&n.Priority, &n.IdempotencyKey, &n.BatchID, &n.Attempts,
			&gwResp, &n.CreatedAt, &n.UpdatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("store: scan: %w", err)
		}
		n.GatewayResponse = gwResp
		notifications = append(notifications, n)
	}
	return notifications, rows.Err()
}
