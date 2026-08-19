// Package rabbitmq provides RabbitMQ infrastructure for publishing messages.
package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	connectionTimeout = 5 * time.Second
	heartbeatTimeout  = 10 * time.Second
)

// Config contains RabbitMQ topology and connection settings.
type Config struct {
	URL        string
	Exchange   string
	Queue      string
	RoutingKey string
}

// Publisher owns a RabbitMQ connection and publishing channel.
type Publisher struct {
	connection *amqp.Connection
	channel    *amqp.Channel
	exchange   string
	routingKey string

	mu     sync.Mutex
	closed bool
}

// NewPublisher connects to RabbitMQ and declares the required durable topology.
func NewPublisher(ctx context.Context, cfg Config) (*Publisher, error) {
	dialer := &net.Dialer{Timeout: connectionTimeout}
	connection, err := amqp.DialConfig(cfg.URL, amqp.Config{
		Heartbeat: heartbeatTimeout,
		Locale:    "en_US",
		Dial: func(network, address string) (net.Conn, error) {
			return dialer.DialContext(ctx, network, address)
		},
	})
	if err != nil {
		return nil, fmt.Errorf("connect to RabbitMQ: %w", err)
	}

	channel, err := connection.Channel()
	if err != nil {
		return nil, errors.Join(
			fmt.Errorf("open RabbitMQ channel: %w", err),
			closeConnection(connection),
		)
	}

	if err := channel.ExchangeDeclare(cfg.Exchange, "direct", true, false, false, false, nil); err != nil {
		return nil, errors.Join(
			fmt.Errorf("declare RabbitMQ exchange %q: %w", cfg.Exchange, err),
			closeChannelAndConnection(channel, connection),
		)
	}
	if _, err := channel.QueueDeclare(cfg.Queue, true, false, false, false, nil); err != nil {
		return nil, errors.Join(
			fmt.Errorf("declare RabbitMQ queue %q: %w", cfg.Queue, err),
			closeChannelAndConnection(channel, connection),
		)
	}
	if err := channel.QueueBind(cfg.Queue, cfg.RoutingKey, cfg.Exchange, false, nil); err != nil {
		return nil, errors.Join(
			fmt.Errorf("bind RabbitMQ queue %q: %w", cfg.Queue, err),
			closeChannelAndConnection(channel, connection),
		)
	}

	return &Publisher{
		connection: connection,
		channel:    channel,
		exchange:   cfg.Exchange,
		routingKey: cfg.RoutingKey,
	}, nil
}

// Publish sends a persistent JSON message to the configured exchange.
func (p *Publisher) Publish(ctx context.Context, payload []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return errors.New("RabbitMQ publisher is closed")
	}
	if err := p.channel.PublishWithContext(ctx, p.exchange, p.routingKey, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now().UTC(),
		Body:         payload,
	}); err != nil {
		return fmt.Errorf("publish RabbitMQ message: %w", err)
	}
	return nil
}

// Close closes the publishing channel followed by the RabbitMQ connection.
func (p *Publisher) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil
	}
	p.closed = true
	return closeChannelAndConnection(p.channel, p.connection)
}

func closeChannelAndConnection(channel *amqp.Channel, connection *amqp.Connection) error {
	var result error
	if err := channel.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
		result = errors.Join(result, fmt.Errorf("close RabbitMQ channel: %w", err))
	}
	return errors.Join(result, closeConnection(connection))
}

func closeConnection(connection *amqp.Connection) error {
	if err := connection.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
		return fmt.Errorf("close RabbitMQ connection: %w", err)
	}
	return nil
}
