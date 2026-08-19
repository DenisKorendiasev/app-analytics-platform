package rabbitmq

import (
	"context"
	"fmt"
	"net"

	amqp "github.com/rabbitmq/amqp091-go"
)

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
	if err := channel.ExchangeDeclare(cfg.Exchange, "direct", true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare RabbitMQ exchange %q: %w", cfg.Exchange, err)
	}
	if _, err := channel.QueueDeclare(cfg.Queue, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare RabbitMQ queue %q: %w", cfg.Queue, err)
	}
	if err := channel.QueueBind(cfg.Queue, cfg.RoutingKey, cfg.Exchange, false, nil); err != nil {
		return fmt.Errorf("bind RabbitMQ queue %q: %w", cfg.Queue, err)
	}
	return nil
}
