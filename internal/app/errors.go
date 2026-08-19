package app

import "errors"

var (
	// ErrNotFound indicates that an application does not exist.
	ErrNotFound = errors.New("app not found")
	// ErrNameRequired indicates that an application name is missing.
	ErrNameRequired = errors.New("app name is required")
	// ErrPublisherRequired indicates that an application publisher is missing.
	ErrPublisherRequired = errors.New("app publisher is required")
	// ErrCategoryRequired indicates that an application category is missing.
	ErrCategoryRequired = errors.New("app category is required")
)
