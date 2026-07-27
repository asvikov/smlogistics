package config

import (
	"fmt"

	"github.com/caarlos0/env/v11"
)

type Config struct {
	AppEnv   string `env:"APP_ENV"   envDefault:"local"`
	AppDebug bool   `env:"APP_DEBUG" envDefault:"true"`

	DBHost     string `env:"DB_HOST"     envDefault:"db"`
	DBPort     int    `env:"DB_PORT"     envDefault:"5432"`
	DBDatabase string `env:"DB_DATABASE" envDefault:"notifications"`
	DBUsername string `env:"DB_USERNAME" envDefault:"notify_user"`
	DBPassword string `env:"DB_PASSWORD" envDefault:"secret"`

	RedisHost     string `env:"REDIS_HOST"     envDefault:"redis"`
	RedisPort     int    `env:"REDIS_PORT"     envDefault:"6379"`
	RedisPassword string `env:"REDIS_PASSWORD" envDefault:""`

	RabbitMQHost        string `env:"RABBITMQ_HOST"     envDefault:"rabbitmq"`
	RabbitMQPort        int    `env:"RABBITMQ_PORT"     envDefault:"5672"`
	RabbitMQVHost       string `env:"RABBITMQ_VHOST"    envDefault:"/notifications"`
	RabbitMQUser        string `env:"RABBITMQ_USER"     envDefault:"notify_user"`
	RabbitMQPassword    string `env:"RABBITMQ_PASSWORD" envDefault:"secret"`
	RabbitMQQueue       string `env:"RABBITMQ_QUEUE"    envDefault:"notifications_queue"`
	RabbitMQDurable     bool   `env:"RABBITMQ_DURABLE"  envDefault:"true"`
	RabbitMQAutoDelete  bool   `env:"RABBITMQ_AUTODELETE"  envDefault:"false"`
	RabbitMQExclusive   bool   `env:"RABBITMQ_EXCLUSIVE"  envDefault:"false"`
	RabbitMQNoWait      bool   `env:"RABBITMQ_NO_WAIT"  envDefault:"false"`
	RabbitMQMaxPriority int    `env:"RABBITMQ_MAX_PRIORITY"  envDefault:"10"`
	RabbitMQQueueType   string `env:"RABBITMQ_QUEUE_TYPE"  envDefault:"classic"`

	HTTPPort             int `env:"HTTP_PORT"             envDefault:"8080"`
	IdempotencyTTL       int `env:"IDEMPOTENCY_TTL_SECONDS" envDefault:"3600"`
	MockSMSFailureRate   int `env:"MOCK_SMS_FAILURE_RATE"  envDefault:"10"`
	MockEmailFailureRate int `env:"MOCK_EMAIL_FAILURE_RATE" envDefault:"5"`
	MaxRetries           int `env:"MAX_RETRIES"            envDefault:"3"`
}

func Load() (*Config, error) {
	cfg := &Config{}
	if err := env.Parse(cfg); err != nil {
		return nil, fmt.Errorf("config: parse env: %w", err)
	}
	return cfg, nil
}

// PostgreSQL connection string.
func (c *Config) DSN() string {
	return fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s?sslmode=disable",
		c.DBUsername, c.DBPassword, c.DBHost, c.DBPort, c.DBDatabase,
	)
}

// Redis address string.
func (c *Config) RedisAddr() string {
	return fmt.Sprintf("%s:%d", c.RedisHost, c.RedisPort)
}

// RabbitMQ URL
func (c *Config) RabbitMQURL() string {
	return fmt.Sprintf(
		"amqp://%s:%s@%s:%d/%s",
		c.RabbitMQUser, c.RabbitMQPassword,
		c.RabbitMQHost, c.RabbitMQPort,
		c.RabbitMQVHost,
	)
}
