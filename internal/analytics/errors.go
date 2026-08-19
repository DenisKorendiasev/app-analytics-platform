package analytics

import "errors"

var (
	// ErrAppIDRequired indicates that the statistics application ID is empty.
	ErrAppIDRequired = errors.New("app id is required")
	// ErrFromInvalid indicates that from is not an ISO calendar date.
	ErrFromInvalid = errors.New("from must be a date in YYYY-MM-DD format")
	// ErrToInvalid indicates that to is not an ISO calendar date.
	ErrToInvalid = errors.New("to must be a date in YYYY-MM-DD format")
	// ErrDateRangeInvalid indicates that from is later than to.
	ErrDateRangeInvalid = errors.New("from must not be after to")
	// ErrPlatformInvalid indicates that platform is not supported.
	ErrPlatformInvalid = errors.New("platform must be android or ios")
)
