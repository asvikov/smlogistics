#!/bin/sh

set -e

if [ "$1" = "migrate" ]; then
    echo "Running database migrations..."
    migrate -path migrations -database "postgres://${DB_USERNAME}:${DB_PASSWORD}@${DB_HOST:-db}:${DB_PORT:-5432}/${DB_DATABASE}?sslmode=disable" up
    echo "Migrations completed successfully."
    exit 0
fi

echo "Starting application..."
exec ./notificationd "$@"