package app

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestServiceCreate(t *testing.T) {
	fixedID := uuid.MustParse("b8edbe8d-4fa6-42fd-a351-9a98d17d8b83")
	fixedTime := time.Date(2026, time.August, 18, 10, 0, 0, 0, time.FixedZone("test", 3*60*60))
	var created App
	repository := &fakeRepository{
		create: func(_ context.Context, application App) error {
			created = application
			return nil
		},
	}
	service := NewService(repository)
	service.newID = func() uuid.UUID { return fixedID }
	service.now = func() time.Time { return fixedTime }

	application, err := service.Create(context.Background(), " Spotify ", " Spotify AB ", " music ")
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	if application.ID != fixedID {
		t.Errorf("ID = %s, want %s", application.ID, fixedID)
	}
	if application.Name != "Spotify" {
		t.Errorf("Name = %q, want Spotify", application.Name)
	}
	if application.Publisher != "Spotify AB" {
		t.Errorf("Publisher = %q, want Spotify AB", application.Publisher)
	}
	if application.Category != "music" {
		t.Errorf("Category = %q, want music", application.Category)
	}
	if !application.CreatedAt.Equal(fixedTime) || application.CreatedAt.Location() != time.UTC {
		t.Errorf("CreatedAt = %v, want %v in UTC", application.CreatedAt, fixedTime.UTC())
	}
	if created != application {
		t.Errorf("repository received %+v, want %+v", created, application)
	}
}

func TestServiceCreateValidation(t *testing.T) {
	tests := []struct {
		name      string
		appName   string
		publisher string
		category  string
		wantError error
	}{
		{name: "empty name", appName: "  ", publisher: "Publisher", category: "music", wantError: ErrNameRequired},
		{name: "empty publisher", appName: "App", publisher: "  ", category: "music", wantError: ErrPublisherRequired},
		{name: "empty category", appName: "App", publisher: "Publisher", category: "  ", wantError: ErrCategoryRequired},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repository := &fakeRepository{
				create: func(_ context.Context, _ App) error {
					t.Fatal("repository Create() called for invalid input")
					return nil
				},
			}
			service := NewService(repository)

			_, err := service.Create(context.Background(), tt.appName, tt.publisher, tt.category)
			if !errors.Is(err, tt.wantError) {
				t.Fatalf("Create() error = %v, want %v", err, tt.wantError)
			}
		})
	}
}

func TestServiceCreateRepositoryError(t *testing.T) {
	repositoryError := errors.New("repository unavailable")
	repository := &fakeRepository{
		create: func(_ context.Context, _ App) error { return repositoryError },
	}
	service := NewService(repository)

	_, err := service.Create(context.Background(), "App", "Publisher", "music")
	if !errors.Is(err, repositoryError) {
		t.Fatalf("Create() error = %v, want wrapped repository error", err)
	}
}

func TestServiceGetByIDNotFound(t *testing.T) {
	id := uuid.MustParse("b8edbe8d-4fa6-42fd-a351-9a98d17d8b83")
	repository := &fakeRepository{
		getByID: func(_ context.Context, gotID uuid.UUID) (App, error) {
			if gotID != id {
				t.Errorf("GetByID() ID = %s, want %s", gotID, id)
			}
			return App{}, ErrNotFound
		},
	}
	service := NewService(repository)

	_, err := service.GetByID(context.Background(), id)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetByID() error = %v, want ErrNotFound", err)
	}
}

type fakeRepository struct {
	create  func(context.Context, App) error
	getByID func(context.Context, uuid.UUID) (App, error)
	exists  func(context.Context, uuid.UUID) (bool, error)
}

func (r *fakeRepository) Create(ctx context.Context, application App) error {
	if r.create == nil {
		return nil
	}
	return r.create(ctx, application)
}

func (r *fakeRepository) GetByID(ctx context.Context, id uuid.UUID) (App, error) {
	if r.getByID == nil {
		return App{}, nil
	}
	return r.getByID(ctx, id)
}

func (r *fakeRepository) Exists(ctx context.Context, id uuid.UUID) (bool, error) {
	if r.exists == nil {
		return false, nil
	}
	return r.exists(ctx, id)
}
