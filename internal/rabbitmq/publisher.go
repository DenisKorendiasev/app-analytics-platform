// Package rabbitmq provides RabbitMQ infrastructure for publishing and consuming events.
package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/DenisKorendiasev/app-analytics-platform/internal/event"
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

var _ event.Publisher = (*Publisher)(nil)

// NewPublisher connects to RabbitMQ and declares the required durable topology.
func NewPublisher(ctx context.Context, cfg Config) (*Publisher, error) {
	connection, err := dial(ctx, cfg.URL)
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

	if err := declareTopology(channel, cfg); err != nil {
		return nil, errors.Join(err, closeChannelAndConnection(channel, connection))
	}
	if err := channel.Confirm(false); err != nil {
		return nil, errors.Join(
			fmt.Errorf("enable RabbitMQ publisher confirms: %w", err),
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

// Publish sends a persistent JSON event to the configured exchange.
func (p *Publisher) Publish(ctx context.Context, applicationEvent event.Event) error {
	payload, err := json.Marshal(applicationEvent)
	if err != nil {
		return fmt.Errorf("marshal RabbitMQ event: %w", err)
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return errors.New("RabbitMQ publisher is closed")
	}
	confirmation, err := p.channel.PublishWithDeferredConfirmWithContext(ctx, p.exchange, p.routingKey, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		MessageId:    applicationEvent.EventID.String(),
		Timestamp:    time.Now().UTC(),
		Body:         payload,
	})
	if err != nil {
		return fmt.Errorf("publish RabbitMQ message: %w", err)
	}
	if confirmation == nil {
		return errors.New("RabbitMQ publisher confirmation is unavailable")
	}
	acknowledged, err := confirmation.WaitContext(ctx)
	if err != nil {
		return fmt.Errorf("wait for RabbitMQ publisher confirmation: %w", err)
	}
	if !acknowledged {
		return errors.New("RabbitMQ negatively acknowledged published message")
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
