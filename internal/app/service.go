package app

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Service implements App business operations.
type Service struct {
	repository Repository
	newID      func() uuid.UUID
	now        func() time.Time
}

// NewService creates an App service.
func NewService(repository Repository) *Service {
	return &Service{
		repository: repository,
		newID:      uuid.New,
		now:        time.Now,
	}
}

// Create validates input and persists a new application.
func (s *Service) Create(ctx context.Context, name, publisher, category string) (App, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return App{}, ErrNameRequired
	}

	publisher = strings.TrimSpace(publisher)
	if publisher == "" {
		return App{}, ErrPublisherRequired
	}

	category = strings.TrimSpace(category)
	if category == "" {
		return App{}, ErrCategoryRequired
	}

	application := App{
		ID:        s.newID(),
		Name:      name,
		Publisher: publisher,
		Category:  category,
		CreatedAt: s.now().UTC(),
	}
	if err := s.repository.Create(ctx, application); err != nil {
		return App{}, fmt.Errorf("create app: %w", err)
	}

	return application, nil
}

// GetByID returns an application by ID.
func (s *Service) GetByID(ctx context.Context, id uuid.UUID) (App, error) {
	application, err := s.repository.GetByID(ctx, id)
	if err != nil {
		return App{}, fmt.Errorf("get app by ID: %w", err)
	}
	return application, nil
}
