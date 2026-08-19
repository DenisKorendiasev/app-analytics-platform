package app

import (
	"time"

	"github.com/google/uuid"
)

type createAppRequest struct {
	Name      string `json:"name"`
	Publisher string `json:"publisher"`
	Category  string `json:"category"`
}

type appResponse struct {
	ID        uuid.UUID `json:"id"`
	Name      string    `json:"name"`
	Publisher string    `json:"publisher"`
	Category  string    `json:"category"`
	CreatedAt time.Time `json:"created_at"`
}

type errorResponse struct {
	Error string `json:"error"`
}

func newAppResponse(application App) appResponse {
	return appResponse(application)
}
