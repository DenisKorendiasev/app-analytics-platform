package event

import "errors"

var (
	ErrAppIDRequired          = errors.New("app id is required")
	ErrEventTypeInvalid       = errors.New("event type must be install, session, or purchase")
	ErrCountryRequired        = errors.New("country is required")
	ErrPlatformInvalid        = errors.New("platform must be android or ios")
	ErrPurchaseRevenueInvalid = errors.New("purchase revenue cents must be non-negative")
	ErrTimestampRequired      = errors.New("timestamp is required")
	ErrAppNotFound            = errors.New("app not found")
)
