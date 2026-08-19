package event

import "context"

// Publisher sends accepted events for asynchronous processing.
type Publisher interface {
	Publish(ctx context.Context, event Event) error
}
