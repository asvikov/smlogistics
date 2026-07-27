package service

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/asvikov/smlogistics/internal/config"
)

type IdempotencyService struct {
	rdb    *redis.Client
	ttl    time.Duration
	prefix string
}

func NewIdempotencyService(cfg *config.Config) (*IdempotencyService, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr(),
		Password: cfg.RedisPassword,
		DB:       0,
	})

	// Verify connection
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("idempotency: redis connect: %w", err)
	}

	return &IdempotencyService{
		rdb:    rdb,
		ttl:    time.Duration(cfg.IdempotencyTTL) * time.Second,
		prefix: "idempotency:",
	}, nil
}

// Close shuts down the Redis connection.
func (s *IdempotencyService) Close() error {
	return s.rdb.Close()
}

// CheckOrCreate tries to set the key with NX (only if not exists).
// Returns true if the key was set, false if already exists (duplicate).
func (s *IdempotencyService) CheckOrCreate(ctx context.Context, key string) (bool, error) {
	fullKey := s.prefix + key
	ok, err := s.rdb.SetNX(ctx, fullKey, "1", s.ttl).Result()
	if err != nil {
		return false, fmt.Errorf("idempotency: check: %w", err)
	}
	return ok, nil
}

// removes an idempotency
func (s *IdempotencyService) Release(ctx context.Context, key string) error {
	fullKey := s.prefix + key
	if err := s.rdb.Del(ctx, fullKey).Err(); err != nil {
		return fmt.Errorf("idempotency: release: %w", err)
	}
	return nil
}
