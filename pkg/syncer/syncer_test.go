package syncer

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/doodlesbykumbi/sink/pkg/provider"
	"github.com/doodlesbykumbi/sink/pkg/provider/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNew(t *testing.T) {
	tmpDir := t.TempDir()

	p := memory.New(memory.Config{
		Config: provider.Config{RemotePath: "/app"},
	})
	require.NoError(t, p.Init(context.Background()))
	defer p.Close()

	s, err := New(Config{
		LocalPath:  tmpDir,
		RemotePath: "/app",
		Debounce:   100 * time.Millisecond,
	}, p)

	require.NoError(t, err)
	require.NotNil(t, s)
	defer s.Close()
}

func TestCreateTar(t *testing.T) {
	tmpDir := t.TempDir()

	// Create test files
	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "main.go"), []byte("package main"), 0644))
	require.NoError(t, os.MkdirAll(filepath.Join(tmpDir, "pkg"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "pkg", "util.go"), []byte("package pkg"), 0644))

	p := memory.New(memory.Config{
		Config: provider.Config{RemotePath: "/app"},
	})
	require.NoError(t, p.Init(context.Background()))
	defer p.Close()

	s, err := New(Config{
		LocalPath:  tmpDir,
		RemotePath: "/app",
		Debounce:   100 * time.Millisecond,
	}, p)
	require.NoError(t, err)
	defer s.Close()

	// Create tar from files
	tarBuf, err := s.CreateTar([]string{
		filepath.Join(tmpDir, "main.go"),
		filepath.Join(tmpDir, "pkg", "util.go"),
	})

	require.NoError(t, err)
	assert.Greater(t, tarBuf.Len(), 0)

	// Sync to provider and verify
	require.NoError(t, p.Sync(context.Background(), tarBuf))
	assert.Equal(t, 2, p.FileCount())

	content, ok := p.GetFile("/app/main.go")
	require.True(t, ok)
	assert.Equal(t, "package main", string(content))

	content, ok = p.GetFile("/app/pkg/util.go")
	require.True(t, ok)
	assert.Equal(t, "package pkg", string(content))
}

func TestShouldIgnore(t *testing.T) {
	tmpDir := t.TempDir()

	// Create .gitignore
	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, ".gitignore"), []byte("*.log\nvendor/\n"), 0644))

	p := memory.New(memory.Config{
		Config: provider.Config{RemotePath: "/app"},
	})
	require.NoError(t, p.Init(context.Background()))
	defer p.Close()

	s, err := New(Config{
		LocalPath:   tmpDir,
		RemotePath:  "/app",
		Debounce:    100 * time.Millisecond,
		NoGitignore: false,
	}, p)
	require.NoError(t, err)
	defer s.Close()

	// Test ignored patterns
	assert.True(t, s.shouldIgnore(filepath.Join(tmpDir, "debug.log"), false))
	assert.True(t, s.shouldIgnore(filepath.Join(tmpDir, "vendor"), true))
	assert.True(t, s.shouldIgnore(filepath.Join(tmpDir, ".git"), true))

	// Test allowed patterns
	assert.False(t, s.shouldIgnore(filepath.Join(tmpDir, "main.go"), false))
	assert.False(t, s.shouldIgnore(filepath.Join(tmpDir, "pkg"), true))
}

func TestShouldIgnore_Disabled(t *testing.T) {
	tmpDir := t.TempDir()

	// Create .gitignore (should be ignored when NoGitignore is true)
	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, ".gitignore"), []byte("*.log\n"), 0644))

	p := memory.New(memory.Config{
		Config: provider.Config{RemotePath: "/app"},
	})
	require.NoError(t, p.Init(context.Background()))
	defer p.Close()

	s, err := New(Config{
		LocalPath:   tmpDir,
		RemotePath:  "/app",
		Debounce:    100 * time.Millisecond,
		NoGitignore: true,
	}, p)
	require.NoError(t, err)
	defer s.Close()

	// With NoGitignore, nothing should be ignored
	assert.False(t, s.shouldIgnore(filepath.Join(tmpDir, "debug.log"), false))
}

func TestLoadIgnorePatterns(t *testing.T) {
	tmpDir := t.TempDir()

	// Always returns a matcher (at minimum for .git)
	matcher := loadIgnorePatterns(tmpDir)
	assert.NotNil(t, matcher)

	// Create .syncignore
	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, ".syncignore"), []byte("*.tmp\n"), 0644))

	matcher = loadIgnorePatterns(tmpDir)
	assert.NotNil(t, matcher)
}

func TestRun_SyncsFilesOnChange(t *testing.T) {
	tmpDir := t.TempDir()

	// Create initial file
	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "initial.go"), []byte("package initial"), 0644))

	p := memory.New(memory.Config{
		Config: provider.Config{RemotePath: "/app"},
	})
	require.NoError(t, p.Init(context.Background()))
	defer p.Close()

	s, err := New(Config{
		LocalPath:   tmpDir,
		RemotePath:  "/app",
		Debounce:    50 * time.Millisecond, // Short debounce for testing
		NoGitignore: true,
	}, p)
	require.NoError(t, err)
	defer s.Close()

	// Run syncer in background
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- s.Run(ctx)
	}()

	// Wait for initial sync
	time.Sleep(100 * time.Millisecond)

	// Verify initial file was synced
	assert.Equal(t, 1, p.FileCount())
	content, ok := p.GetFile("/app/initial.go")
	require.True(t, ok)
	assert.Equal(t, "package initial", string(content))

	// Create a new file
	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "new.go"), []byte("package new"), 0644))

	// Wait for debounce + processing
	time.Sleep(150 * time.Millisecond)

	// Verify new file was synced
	assert.Equal(t, 2, p.FileCount())
	content, ok = p.GetFile("/app/new.go")
	require.True(t, ok)
	assert.Equal(t, "package new", string(content))

	// Modify existing file
	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "new.go"), []byte("package new // updated"), 0644))

	// Wait for debounce + processing
	time.Sleep(150 * time.Millisecond)

	// Verify modification was synced
	content, ok = p.GetFile("/app/new.go")
	require.True(t, ok)
	assert.Equal(t, "package new // updated", string(content))

	// Cancel and verify clean shutdown
	cancel()
	select {
	case err := <-errCh:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("syncer did not shut down in time")
	}
}

func TestRun_DeletesFiles(t *testing.T) {
	tmpDir := t.TempDir()

	// Create initial files
	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "keep.go"), []byte("keep"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "delete.go"), []byte("delete"), 0644))

	p := memory.New(memory.Config{
		Config: provider.Config{RemotePath: "/app"},
	})
	require.NoError(t, p.Init(context.Background()))
	defer p.Close()

	s, err := New(Config{
		LocalPath:   tmpDir,
		RemotePath:  "/app",
		Debounce:    50 * time.Millisecond,
		NoGitignore: true,
	}, p)
	require.NoError(t, err)
	defer s.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go s.Run(ctx)

	// Wait for initial sync
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, 2, p.FileCount())

	// Delete a file
	require.NoError(t, os.Remove(filepath.Join(tmpDir, "delete.go")))

	// Wait for debounce + processing
	time.Sleep(150 * time.Millisecond)

	// Verify deletion was synced
	assert.Equal(t, 1, p.FileCount())
	_, ok := p.GetFile("/app/delete.go")
	assert.False(t, ok)
	_, ok = p.GetFile("/app/keep.go")
	assert.True(t, ok)

	cancel()
}

func TestRun_RunsCommandAfterSync(t *testing.T) {
	tmpDir := t.TempDir()

	p := memory.New(memory.Config{
		Config: provider.Config{RemotePath: "/app"},
	})
	require.NoError(t, p.Init(context.Background()))
	defer p.Close()

	s, err := New(Config{
		LocalPath:   tmpDir,
		RemotePath:  "/app",
		Debounce:    50 * time.Millisecond,
		Command:     "go build",
		NoGitignore: true,
	}, p)
	require.NoError(t, err)
	defer s.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go s.Run(ctx)

	// Wait for initial sync (no files, no command)
	time.Sleep(100 * time.Millisecond)
	assert.Empty(t, p.GetCommands())

	// Create a file to trigger sync + command
	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "main.go"), []byte("package main"), 0644))

	// Wait for debounce + processing
	time.Sleep(150 * time.Millisecond)

	// Verify command was run after sync
	assert.Equal(t, []string{"go build"}, p.GetCommands())

	cancel()
}
