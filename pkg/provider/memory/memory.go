// Package memory provides an in-memory provider for testing purposes.
package memory

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/doodlesbykumbi/sink/pkg/provider"
)

// Config holds Memory-specific configuration
type Config struct {
	provider.Config
}

// Provider implements the provider.Provider interface for in-memory storage (testing)
type Provider struct {
	config Config

	mu       sync.RWMutex
	files    map[string][]byte // path -> content
	commands []string          // executed commands
}

// Compile-time check that Provider implements provider.Provider
var _ provider.Provider = (*Provider)(nil)

// New creates a new Memory provider
func New(cfg Config) *Provider {
	return &Provider{
		config:   cfg,
		files:    make(map[string][]byte),
		commands: make([]string, 0),
	}
}

func (p *Provider) Name() string {
	return "memory"
}

func (p *Provider) Init(ctx context.Context) error {
	return nil
}

func (p *Provider) Close() error {
	return nil
}

func (p *Provider) Targets(ctx context.Context) ([]string, error) {
	return []string{"memory://" + p.config.RemotePath}, nil
}

func (p *Provider) Sync(ctx context.Context, tarData *bytes.Buffer) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	tr := tar.NewReader(tarData)
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read tar: %w", err)
		}

		if header.Typeflag != tar.TypeReg {
			continue
		}

		content, err := io.ReadAll(tr)
		if err != nil {
			return fmt.Errorf("failed to read file content: %w", err)
		}

		path := p.config.RemotePath + "/" + header.Name
		p.files[path] = content
	}

	return nil
}

func (p *Provider) Delete(ctx context.Context, relativePaths []string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, relPath := range relativePaths {
		path := p.config.RemotePath + "/" + relPath
		delete(p.files, path)
	}

	return nil
}

func (p *Provider) RunCommand(ctx context.Context, command string) error {
	if command == "" {
		return nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	p.commands = append(p.commands, command)
	return nil
}

// GetFile returns the content of a file (for testing)
func (p *Provider) GetFile(path string) ([]byte, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	content, ok := p.files[path]
	return content, ok
}

// GetFiles returns all files (for testing)
func (p *Provider) GetFiles() map[string][]byte {
	p.mu.RLock()
	defer p.mu.RUnlock()

	result := make(map[string][]byte, len(p.files))
	for k, v := range p.files {
		result[k] = v
	}
	return result
}

// GetCommands returns all executed commands (for testing)
func (p *Provider) GetCommands() []string {
	p.mu.RLock()
	defer p.mu.RUnlock()

	result := make([]string, len(p.commands))
	copy(result, p.commands)
	return result
}

// FileCount returns the number of synced files (for testing)
func (p *Provider) FileCount() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.files)
}

// Reset clears all synced files and commands (for testing)
func (p *Provider) Reset() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.files = make(map[string][]byte)
	p.commands = make([]string, 0)
}
