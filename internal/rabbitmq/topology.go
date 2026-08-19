package rabbitmq

import (
	"context"
	"fmt"
	"net"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	deadLetterExchangeSuffix   = ".dead-letter"
	deadLetterQueueSuffix      = ".dead-letter"
	deadLetterRoutingKeySuffix = ".dead-letter"
)

// DeadLetterExchangeName returns the exchange used for failed deliveries.
func DeadLetterExchangeName(cfg Config) string {
	return cfg.Exchange + deadLetterExchangeSuffix
}

// DeadLetterQueueName returns the queue used for failed deliveries.
func DeadLetterQueueName(cfg Config) string {
	return cfg.Queue + deadLetterQueueSuffix
}

// DeadLetterRoutingKey returns the routing key used for failed deliveries.
func DeadLetterRoutingKey(cfg Config) string {
	return cfg.RoutingKey + deadLetterRoutingKeySuffix
}

func dial(ctx context.Context, connectionURL string) (*amqp.Connection, error) {
	dialer := &net.Dialer{Timeout: connectionTimeout}
	return amqp.DialConfig(connectionURL, amqp.Config{
		Heartbeat: heartbeatTimeout,
		Locale:    "en_US",
		Dial: func(network, address string) (net.Conn, error) {
			return dialer.DialContext(ctx, network, address)
		},
	})
}

func declareTopology(channel *amqp.Channel, cfg Config) error {
	deadLetterExchange := DeadLetterExchangeName(cfg)
	deadLetterQueue := DeadLetterQueueName(cfg)
	deadLetterRoutingKey := DeadLetterRoutingKey(cfg)

	if err := channel.ExchangeDeclare(deadLetterExchange, "direct", true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare RabbitMQ dead-letter exchange %q: %w", deadLetterExchange, err)
	}
	if _, err := channel.QueueDeclare(deadLetterQueue, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare RabbitMQ dead-letter queue %q: %w", deadLetterQueue, err)
	}
	if err := channel.QueueBind(deadLetterQueue, deadLetterRoutingKey, deadLetterExchange, false, nil); err != nil {
		return fmt.Errorf("bind RabbitMQ dead-letter queue %q: %w", deadLetterQueue, err)
	}

	if err := channel.ExchangeDeclare(cfg.Exchange, "direct", true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare RabbitMQ exchange %q: %w", cfg.Exchange, err)
	}
	queueArguments := amqp.Table{
		"x-dead-letter-exchange":    deadLetterExchange,
		"x-dead-letter-routing-key": deadLetterRoutingKey,
	}
	if _, err := channel.QueueDeclare(cfg.Queue, true, false, false, false, queueArguments); err != nil {
		return fmt.Errorf("declare RabbitMQ queue %q: %w", cfg.Queue, err)
	}
	if err := channel.QueueBind(cfg.Queue, cfg.RoutingKey, cfg.Exchange, false, nil); err != nil {
		return fmt.Errorf("bind RabbitMQ queue %q: %w", cfg.Queue, err)
	}
	return nil
}
