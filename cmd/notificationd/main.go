package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/asvikov/smlogistics/internal/api"
	"github.com/asvikov/smlogistics/internal/config"
	applog "github.com/asvikov/smlogistics/internal/log"
	"github.com/asvikov/smlogistics/internal/service"
	"github.com/asvikov/smlogistics/internal/store"
	"github.com/asvikov/smlogistics/internal/worker"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "usage: notificationd {serve|work}\n")
		os.Exit(1)
	}

	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load config: %v\n", err)
		os.Exit(1)
	}

	logger := applog.New(cfg.AppDebug)

	switch os.Args[1] {
	case "serve":
		runAPIServer(cfg, logger)
	case "work":
		runWorker(cfg, logger)
	default:
		fmt.Fprintf(os.Stderr, "usage: notificationd {serve|work}\n")
		os.Exit(1)
	}
}

func runAPIServer(cfg *config.Config, logger *slog.Logger) {
	ctx := context.Background()

	pgStore, err := store.NewPGStore(ctx, cfg.DSN())
	if err != nil {
		logger.Error("failed to connect to PostgreSQL", "error", err)
		os.Exit(1)
	}
	defer pgStore.Close()

	// RabbitMQ connection
	amqpConn, err := amqp.Dial(cfg.RabbitMQURL())
	if err != nil {
		logger.Error("failed to connect to RabbitMQ", "error", err)
		os.Exit(1)
	}
	defer amqpConn.Close()

	amqpCh, err := amqpConn.Channel()
	if err != nil {
		logger.Error("failed to open AMQP channel", "error", err)
		os.Exit(1)
	}
	defer amqpCh.Close()

	// Declare queue (idempotent)
	// Queue is created in docker/rabbitmq/definitions.json
	amqpCh.QueueDeclare(
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

	idempotencySvc, err := service.NewIdempotencyService(cfg)
	if err != nil {
		logger.Error("failed to create idempotency service", "error", err)
		os.Exit(1)
	}
	defer idempotencySvc.Close()

	dispatchSvc := service.NewDispatchService(pgStore, amqpCh, cfg.RabbitMQQueue, logger)

	// Router
	router := api.NewRouter(dispatchSvc, idempotencySvc, pgStore, logger)

	addr := fmt.Sprintf(":%d", cfg.HTTPPort)
	srv := &http.Server{
		Addr:         addr,
		Handler:      router,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Graceful shutdown
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
		<-sig
		logger.Info("api: shutting down...")
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		srv.Shutdown(ctx)
	}()

	logger.Info("api: listening", "addr", addr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Error("api: failed", "error", err)
		os.Exit(1)
	}
	logger.Info("api: stopped")
}

func runWorker(cfg *config.Config, logger *slog.Logger) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pgStore, err := store.NewPGStore(ctx, cfg.DSN())
	if err != nil {
		logger.Error("failed to connect to PostgreSQL", "error", err)
		os.Exit(1)
	}
	defer pgStore.Close()

	consumer, err := worker.NewConsumer(cfg, pgStore, logger)
	if err != nil {
		logger.Error("failed to create consumer", "error", err)
		os.Exit(1)
	}
	defer consumer.Close()

	logger.Info("worker: starting")
	if err := consumer.Run(ctx); err != nil {
		logger.Error("worker: failed", "error", err)
		os.Exit(1)
	}
	logger.Info("worker: stopped")
}
