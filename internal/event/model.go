// Package event contains the event ingestion domain model and business logic.
package event

import (
	"time"

	"github.com/google/uuid"
)

// Type identifies the kind of application event.
type Type string

const (
	TypeInstall  Type = "install"
	TypeSession  Type = "session"
	TypePurchase Type = "purchase"
)

// Platform identifies the mobile platform that produced an event.
type Platform string

const (
	PlatformAndroid Platform = "android"
	PlatformIOS     Platform = "ios"
)

// Event is an application event ready to be published for processing.
type Event struct {
	EventID      uuid.UUID `json:"event_id"`
	AppID        uuid.UUID `json:"app_id"`
	EventType    Type      `json:"event_type"`
	Country      string    `json:"country"`
	Platform     Platform  `json:"platform"`
	RevenueCents int64     `json:"revenue_cents"`
	Timestamp    time.Time `json:"timestamp"`
}

// Input contains client-provided event fields.
type Input struct {
	AppID        uuid.UUID
	EventType    Type
	Country      string
	Platform     Platform
	RevenueCents int64
	Timestamp    time.Time
}
