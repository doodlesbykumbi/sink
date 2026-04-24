package memory

import (
	"archive/tar"
	"bytes"
	"context"
	"testing"

	"github.com/doodlesbykumbi/sink/pkg/provider"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemoryProvider_Name(t *testing.T) {
	p := New(Config{})
	assert.Equal(t, "memory", p.Name())
}

func TestMemoryProvider_InitClose(t *testing.T) {
	p := New(Config{})
	require.NoError(t, p.Init(context.Background()))
	require.NoError(t, p.Close())
}

func TestMemoryProvider_Targets(t *testing.T) {
	p := New(Config{Config: provider.Config{RemotePath: "/app"}})

	targets, err := p.Targets(context.Background())
	require.NoError(t, err)
	assert.Equal(t, []string{"memory:///app"}, targets)
}

func TestMemoryProvider_Sync(t *testing.T) {
	p := New(Config{Config: provider.Config{RemotePath: "/app"}})

	tarBuf := createTestTar(t, map[string]string{
		"main.go":     "package main",
		"lib/util.go": "package lib",
	})

	require.NoError(t, p.Sync(context.Background(), tarBuf))
	assert.Equal(t, 2, p.FileCount())

	content, ok := p.GetFile("/app/main.go")
	require.True(t, ok, "main.go should exist")
	assert.Equal(t, "package main", string(content))

	content, ok = p.GetFile("/app/lib/util.go")
	require.True(t, ok, "lib/util.go should exist")
	assert.Equal(t, "package lib", string(content))
}

func TestMemoryProvider_Delete(t *testing.T) {
	p := New(Config{Config: provider.Config{RemotePath: "/app"}})

	tarBuf := createTestTar(t, map[string]string{
		"main.go": "package main",
		"test.go": "package main_test",
	})

	require.NoError(t, p.Sync(context.Background(), tarBuf))
	assert.Equal(t, 2, p.FileCount())

	require.NoError(t, p.Delete(context.Background(), []string{"test.go"}))
	assert.Equal(t, 1, p.FileCount())

	_, ok := p.GetFile("/app/test.go")
	assert.False(t, ok, "test.go should be deleted")

	_, ok = p.GetFile("/app/main.go")
	assert.True(t, ok, "main.go should still exist")
}

func TestMemoryProvider_RunCommand(t *testing.T) {
	p := New(Config{})

	require.NoError(t, p.RunCommand(context.Background(), ""))
	assert.Empty(t, p.GetCommands())

	require.NoError(t, p.RunCommand(context.Background(), "go build"))
	require.NoError(t, p.RunCommand(context.Background(), "go test"))

	assert.Equal(t, []string{"go build", "go test"}, p.GetCommands())
}

func TestMemoryProvider_Reset(t *testing.T) {
	p := New(Config{Config: provider.Config{RemotePath: "/app"}})

	tarBuf := createTestTar(t, map[string]string{"main.go": "package main"})
	_ = p.Sync(context.Background(), tarBuf)
	_ = p.RunCommand(context.Background(), "go build")

	assert.Equal(t, 1, p.FileCount())
	assert.Len(t, p.GetCommands(), 1)

	p.Reset()

	assert.Equal(t, 0, p.FileCount())
	assert.Empty(t, p.GetCommands())
}

func createTestTar(t *testing.T, files map[string]string) *bytes.Buffer {
	t.Helper()

	buf := new(bytes.Buffer)
	tw := tar.NewWriter(buf)

	for name, content := range files {
		header := &tar.Header{
			Name: name,
			Size: int64(len(content)),
			Mode: 0644,
		}

		if err := tw.WriteHeader(header); err != nil {
			t.Fatalf("failed to write tar header: %v", err)
		}

		if _, err := tw.Write([]byte(content)); err != nil {
			t.Fatalf("failed to write tar content: %v", err)
		}
	}

	if err := tw.Close(); err != nil {
		t.Fatalf("failed to close tar writer: %v", err)
	}

	return buf
}
