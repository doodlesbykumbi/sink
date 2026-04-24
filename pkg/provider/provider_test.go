package provider_test

import (
	"archive/tar"
	"bytes"
	"context"
	"testing"

	"github.com/doodlesbykumbi/sink/pkg/provider"
	"github.com/doodlesbykumbi/sink/pkg/provider/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProvider(t *testing.T) {
	p := memory.New(memory.Config{
		Config: provider.Config{RemotePath: "/app"},
	})

	// Test Name
	assert.Equal(t, "memory", p.Name())

	// Test Init/Close
	require.NoError(t, p.Init(context.Background()))
	defer p.Close()

	// Test Targets
	targets, err := p.Targets(context.Background())
	require.NoError(t, err)
	assert.Equal(t, []string{"memory:///app"}, targets)

	// Test Sync
	tarBuf := createTar(t, map[string]string{
		"main.go": "package main",
		"util.go": "package util",
	})
	require.NoError(t, p.Sync(context.Background(), tarBuf))
	assert.Equal(t, 2, p.FileCount())

	content, ok := p.GetFile("/app/main.go")
	require.True(t, ok)
	assert.Equal(t, "package main", string(content))

	// Test Delete
	require.NoError(t, p.Delete(context.Background(), []string{"util.go"}))
	assert.Equal(t, 1, p.FileCount())

	_, ok = p.GetFile("/app/util.go")
	assert.False(t, ok)

	// Test RunCommand
	require.NoError(t, p.RunCommand(context.Background(), "go build"))
	require.NoError(t, p.RunCommand(context.Background(), "go test"))
	assert.Equal(t, []string{"go build", "go test"}, p.GetCommands())

	// Empty command is no-op
	require.NoError(t, p.RunCommand(context.Background(), ""))
	assert.Len(t, p.GetCommands(), 2)
}

func TestConfig(t *testing.T) {
	cfg := provider.Config{RemotePath: "/app"}
	assert.Equal(t, "/app", cfg.RemotePath)
}

func createTar(t *testing.T, files map[string]string) *bytes.Buffer {
	t.Helper()
	buf := new(bytes.Buffer)
	tw := tar.NewWriter(buf)

	for name, content := range files {
		err := tw.WriteHeader(&tar.Header{
			Name: name,
			Size: int64(len(content)),
			Mode: 0644,
		})
		require.NoError(t, err)
		_, err = tw.Write([]byte(content))
		require.NoError(t, err)
	}

	require.NoError(t, tw.Close())
	return buf
}
