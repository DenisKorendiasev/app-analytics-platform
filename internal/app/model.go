// Package app contains the application domain model and business logic.
package app

import (
	"time"

	"github.com/google/uuid"
)

// App represents a mobile application registered in the platform.
type App struct {
	ID        uuid.UUID
	Name      string
	Publisher string
	Category  string
	CreatedAt time.Time
}
