package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/DenisKorendiasev/app-analytics-platform/internal/event"
	"github.com/DenisKorendiasev/app-analytics-platform/internal/worker"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	workerConsumerTag = "app-analytics-worker"
)

// Consumer owns a RabbitMQ connection, channel, and event subscription.
type Consumer struct {
	connection *amqp.Connection
	channel    *amqp.Channel
	deliveries <-chan amqp.Delivery

	mu      sync.Mutex
	stopped bool
	closed  bool
}

var _ worker.Consumer = (*Consumer)(nil)

// NewConsumer connects to RabbitMQ and subscribes to the configured queue.
func NewConsumer(ctx context.Context, cfg Config) (*Consumer, error) {
	connection, err := dial(ctx, cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("connect RabbitMQ consumer: %w", err)
	}

	channel, err := connection.Channel()
	if err != nil {
		return nil, errors.Join(
			fmt.Errorf("open RabbitMQ consumer channel: %w", err),
			closeConnection(connection),
		)
	}
	if err := declareTopology(channel, cfg); err != nil {
		return nil, errors.Join(err, closeChannelAndConnection(channel, connection))
	}
	if err := channel.Qos(worker.BatchSize, 0, false); err != nil {
		return nil, errors.Join(
			fmt.Errorf("set RabbitMQ consumer prefetch: %w", err),
			closeChannelAndConnection(channel, connection),
		)
	}
	deliveries, err := channel.Consume(cfg.Queue, workerConsumerTag, false, false, false, false, nil)
	if err != nil {
		return nil, errors.Join(
			fmt.Errorf("subscribe RabbitMQ queue %q: %w", cfg.Queue, err),
			closeChannelAndConnection(channel, connection),
		)
	}

	return &Consumer{
		connection: connection,
		channel:    channel,
		deliveries: deliveries,
	}, nil
}

// Receive waits for and decodes the next RabbitMQ event delivery.
func (c *Consumer) Receive(ctx context.Context) (worker.Delivery, error) {
	for {
		if err := ctx.Err(); err != nil {
			return worker.Delivery{}, err
		}
		select {
		case <-ctx.Done():
			return worker.Delivery{}, ctx.Err()
		case delivery, ok := <-c.deliveries:
			if !ok {
				return worker.Delivery{}, errors.New("RabbitMQ delivery channel is closed")
			}

			var applicationEvent event.Event
			if err := json.Unmarshal(delivery.Body, &applicationEvent); err != nil {
				if rejectError := rejectDelivery(delivery); rejectError != nil {
					return worker.Delivery{}, errors.Join(
						fmt.Errorf("decode RabbitMQ event: %w", err),
						rejectError,
					)
				}
				continue
			}
			if err := event.Validate(applicationEvent); err != nil {
				if rejectError := rejectDelivery(delivery); rejectError != nil {
					return worker.Delivery{}, errors.Join(
						fmt.Errorf("validate RabbitMQ event: %w", err),
						rejectError,
					)
				}
				continue
			}
			return worker.Delivery{
				Event: applicationEvent,
				Ack: func() error {
					if err := delivery.Ack(false); err != nil {
						return fmt.Errorf("ack RabbitMQ delivery: %w", err)
					}
					return nil
				},
				Reject: func() error {
					return rejectDelivery(delivery)
				},
			}, nil
		}
	}
}

func rejectDelivery(delivery amqp.Delivery) error {
	if err := delivery.Reject(false); err != nil {
		return fmt.Errorf("reject RabbitMQ delivery without requeue: %w", err)
	}
	return nil
}

// Stop cancels the subscription so RabbitMQ sends no new deliveries.
func (c *Consumer) Stop() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.stopped || c.closed {
		return nil
	}
	c.stopped = true
	if err := c.channel.Cancel(workerConsumerTag, false); err != nil && !errors.Is(err, amqp.ErrClosed) {
		return fmt.Errorf("cancel RabbitMQ consumer: %w", err)
	}
	return nil
}

// Close stops consumption and closes the channel followed by the connection.
func (c *Consumer) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}
	c.closed = true

	var result error
	if !c.stopped {
		c.stopped = true
		if err := c.channel.Cancel(workerConsumerTag, false); err != nil && !errors.Is(err, amqp.ErrClosed) {
			result = errors.Join(result, fmt.Errorf("cancel RabbitMQ consumer: %w", err))
		}
	}
	return errors.Join(result, closeChannelAndConnection(c.channel, c.connection))
}
