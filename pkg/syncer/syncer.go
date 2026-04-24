// Package syncer provides file watching and syncing functionality.
package syncer

import (
	"archive/tar"
	"bufio"
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/doodlesbykumbi/sink/pkg/provider"
	"github.com/fsnotify/fsnotify"
	"github.com/go-git/go-git/v5/plumbing/format/gitignore"
)

// Config holds the syncer configuration.
type Config struct {
	LocalPath   string
	RemotePath  string
	Debounce    time.Duration
	Command     string
	NoGitignore bool
}

// Syncer watches local files and syncs them to a provider.
type Syncer struct {
	config   Config
	provider provider.Provider
	watcher  *fsnotify.Watcher
	ignorer  gitignore.Matcher

	mu            sync.Mutex
	pendingFiles  map[string]struct{}
	pendingDelete []string
	debounceTimer *time.Timer
}

// New creates a new Syncer with the given provider.
func New(cfg Config, p provider.Provider) (*Syncer, error) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, fmt.Errorf("failed to create file watcher: %w", err)
	}

	var ignorer gitignore.Matcher
	if !cfg.NoGitignore {
		ignorer = loadIgnorePatterns(cfg.LocalPath)
	}

	return &Syncer{
		config:       cfg,
		provider:     p,
		watcher:      watcher,
		ignorer:      ignorer,
		pendingFiles: make(map[string]struct{}),
	}, nil
}

// Close releases resources.
func (s *Syncer) Close() error {
	if s.watcher != nil {
		s.watcher.Close()
	}
	return nil
}

// Run starts watching and syncing files.
func (s *Syncer) Run(ctx context.Context) error {
	if err := s.addRecursiveWatch(s.config.LocalPath); err != nil {
		return fmt.Errorf("failed to set up watches: %w", err)
	}

	targets, _ := s.provider.Targets(ctx)
	log.Printf("Provider: %s", s.provider.Name())
	log.Printf("Watching: %s", s.config.LocalPath)
	log.Printf("Targets: %v", targets)

	log.Println("Performing initial full sync...")
	if err := s.fullSync(ctx); err != nil {
		log.Printf("Warning: initial sync failed: %v", err)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case event, ok := <-s.watcher.Events:
			if !ok {
				return nil
			}
			s.handleEvent(ctx, event)

		case err, ok := <-s.watcher.Errors:
			if !ok {
				return nil
			}
			log.Printf("Watcher error: %v", err)
		}
	}
}

func loadIgnorePatterns(basePath string) gitignore.Matcher {
	var patterns []gitignore.Pattern

	patterns = append(patterns, gitignore.ParsePattern(".git", nil))

	syncignorePath := filepath.Join(basePath, ".syncignore")
	if p, err := loadIgnoreFile(syncignorePath); err == nil {
		patterns = append(patterns, p...)
		log.Printf("Loaded ignore patterns from .syncignore")
	}

	gitignorePath := filepath.Join(basePath, ".gitignore")
	if p, err := loadIgnoreFile(gitignorePath); err == nil {
		patterns = append(patterns, p...)
		log.Printf("Loaded ignore patterns from .gitignore")
	}

	if len(patterns) == 0 {
		return nil
	}

	return gitignore.NewMatcher(patterns)
}

func loadIgnoreFile(path string) ([]gitignore.Pattern, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var patterns []gitignore.Pattern
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		patterns = append(patterns, gitignore.ParsePattern(line, nil))
	}

	return patterns, scanner.Err()
}

func (s *Syncer) addRecursiveWatch(path string) error {
	return filepath.Walk(path, func(walkPath string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			if strings.HasPrefix(info.Name(), ".") && walkPath != path {
				return filepath.SkipDir
			}
			if s.shouldIgnore(walkPath, true) {
				log.Printf("Ignoring directory: %s", walkPath)
				return filepath.SkipDir
			}
			if err := s.watcher.Add(walkPath); err != nil {
				return fmt.Errorf("failed to watch %s: %w", walkPath, err)
			}
			log.Printf("Watching: %s", walkPath)
		}
		return nil
	})
}

func (s *Syncer) shouldIgnore(path string, isDir bool) bool {
	if s.ignorer == nil {
		return false
	}

	relPath, err := filepath.Rel(s.config.LocalPath, path)
	if err != nil {
		return false
	}

	parts := strings.Split(relPath, string(filepath.Separator))
	return s.ignorer.Match(parts, isDir)
}

func (s *Syncer) handleEvent(ctx context.Context, event fsnotify.Event) {
	if strings.HasPrefix(filepath.Base(event.Name), ".") {
		return
	}

	info, statErr := os.Stat(event.Name)
	isDir := statErr == nil && info.IsDir()
	if s.shouldIgnore(event.Name, isDir) {
		return
	}

	if event.Has(fsnotify.Create) {
		if info, err := os.Stat(event.Name); err == nil && info.IsDir() {
			if err := s.addRecursiveWatch(event.Name); err != nil {
				log.Printf("Failed to watch new directory %s: %v", event.Name, err)
			}
		}
	}

	if event.Has(fsnotify.Write) || event.Has(fsnotify.Create) {
		s.queueFile(ctx, event.Name)
	}

	if event.Has(fsnotify.Remove) {
		s.queueDelete(ctx, event.Name)
	}
}

func (s *Syncer) queueFile(ctx context.Context, path string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.pendingFiles[path] = struct{}{}
	s.resetDebounceTimer(ctx)
}

func (s *Syncer) queueDelete(ctx context.Context, path string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	relPath, err := filepath.Rel(s.config.LocalPath, path)
	if err != nil {
		return
	}

	s.pendingDelete = append(s.pendingDelete, relPath)
	s.resetDebounceTimer(ctx)
}

func (s *Syncer) resetDebounceTimer(ctx context.Context) {
	if s.debounceTimer != nil {
		s.debounceTimer.Stop()
	}
	s.debounceTimer = time.AfterFunc(s.config.Debounce, func() {
		s.flushPending(ctx)
	})
}

func (s *Syncer) flushPending(ctx context.Context) {
	s.mu.Lock()
	files := make([]string, 0, len(s.pendingFiles))
	for f := range s.pendingFiles {
		files = append(files, f)
	}
	s.pendingFiles = make(map[string]struct{})

	deletes := s.pendingDelete
	s.pendingDelete = nil
	s.mu.Unlock()

	if len(deletes) > 0 {
		log.Printf("Deleting %d file(s)...", len(deletes))
		if err := s.provider.Delete(ctx, deletes); err != nil {
			log.Printf("Delete error: %v", err)
		}
	}

	if len(files) > 0 {
		log.Printf("Syncing %d file(s)...", len(files))
		if err := s.syncFiles(ctx, files); err != nil {
			log.Printf("Sync error: %v", err)
			return
		}

		if s.config.Command != "" {
			s.provider.RunCommand(ctx, s.config.Command)
		}
	}
}

func (s *Syncer) syncFiles(ctx context.Context, files []string) error {
	tarBuf, err := s.CreateTar(files)
	if err != nil {
		return fmt.Errorf("failed to create tar: %w", err)
	}

	return s.provider.Sync(ctx, tarBuf)
}

// CreateTar creates a tar archive from the given files.
func (s *Syncer) CreateTar(files []string) (*bytes.Buffer, error) {
	buf := new(bytes.Buffer)
	tw := tar.NewWriter(buf)
	defer tw.Close()

	for _, file := range files {
		info, err := os.Stat(file)
		if err != nil {
			log.Printf("Skipping %s: %v", file, err)
			continue
		}

		if info.IsDir() {
			continue
		}

		relPath, err := filepath.Rel(s.config.LocalPath, file)
		if err != nil {
			log.Printf("Skipping %s: cannot get relative path: %v", file, err)
			continue
		}

		content, err := os.ReadFile(file)
		if err != nil {
			log.Printf("Skipping %s: %v", file, err)
			continue
		}

		header := &tar.Header{
			Name:    relPath,
			Size:    int64(len(content)),
			Mode:    int64(info.Mode()),
			ModTime: info.ModTime(),
		}

		if err := tw.WriteHeader(header); err != nil {
			return nil, err
		}

		if _, err := tw.Write(content); err != nil {
			return nil, err
		}
	}

	return buf, nil
}

func (s *Syncer) fullSync(ctx context.Context) error {
	var files []string
	err := filepath.Walk(s.config.LocalPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if strings.HasPrefix(info.Name(), ".") && path != s.config.LocalPath {
			if info.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if s.shouldIgnore(path, info.IsDir()) {
			if info.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if !info.IsDir() {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		return err
	}

	if len(files) == 0 {
		log.Println("No files to sync")
		return nil
	}

	return s.syncFiles(ctx, files)
}
