-- 001_create_notifications.sql

CREATE TABLE IF NOT EXISTS notifications (
    id              BIGSERIAL PRIMARY KEY,
    subscriber_id   VARCHAR(64)  NOT NULL,
    channel         VARCHAR(20)  NOT NULL,
    message         TEXT         NOT NULL,
    status          VARCHAR(20)  NOT NULL DEFAULT 'queued',
    priority        INTEGER      NOT NULL DEFAULT 5,
    idempotency_key VARCHAR(64)  NOT NULL,
    batch_id        VARCHAR(64),
    attempts        INTEGER      NOT NULL DEFAULT 0,
    gateway_response JSONB,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_subscriber_status ON notifications (subscriber_id, status);
CREATE INDEX IF NOT EXISTS idx_batch_status      ON notifications (batch_id, status);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'uniq_idempotency_key'
    ) THEN
        ALTER TABLE notifications ADD CONSTRAINT uniq_idempotency_key UNIQUE (idempotency_key);
    END IF;
END $$;
