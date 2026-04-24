package provider

import (
	"bytes"
	"context"
)

// FileChange represents a file that has changed
type FileChange struct {
	RelativePath string // Path relative to the local root
	AbsolutePath string // Full local path
	IsDelete     bool   // True if this is a delete operation
}

// Provider defines the interface for sync targets
type Provider interface {
	// Name returns the provider name for logging
	Name() string

	// Init initializes the provider with its configuration
	Init(ctx context.Context) error

	// Close cleans up provider resources
	Close() error

	// Sync uploads changed files to the target
	Sync(ctx context.Context, tarData *bytes.Buffer) error

	// Delete removes files from the target
	Delete(ctx context.Context, relativePaths []string) error

	// Targets returns the list of sync targets (for logging)
	Targets(ctx context.Context) ([]string, error)

	// RunCommand executes a command on the target (optional, can be no-op)
	RunCommand(ctx context.Context, command string) error
}

// Config holds common configuration for all providers
type Config struct {
	RemotePath string // Remote path to sync to
}
