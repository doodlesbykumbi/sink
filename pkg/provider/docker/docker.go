package docker

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/doodlesbykumbi/sink/pkg/provider"
)

// Config holds Docker-specific configuration
type Config struct {
	provider.Config
	Container  string   // Container name or ID
	Containers []string // Multiple containers for multi-target sync
	Host       string   // Docker host (optional, uses DOCKER_HOST env if empty)
}

// Provider implements the provider.Provider interface for Docker using CLI
type Provider struct {
	config     Config
	dockerArgs []string // Base docker command arguments
}

// Compile-time check that Provider implements provider.Provider
var _ provider.Provider = (*Provider)(nil)

// New creates a new Docker provider
func New(cfg Config) *Provider {
	return &Provider{config: cfg}
}

func (p *Provider) Name() string {
	return "docker"
}

func (p *Provider) Init(ctx context.Context) error {
	if p.config.Host != "" {
		p.dockerArgs = []string{"-H", p.config.Host}
	}
	cmd := exec.CommandContext(ctx, "docker", append(p.dockerArgs, "info")...)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to connect to docker: %w", err)
	}
	log.Printf("Connected to Docker daemon")
	return nil
}

func (p *Provider) Close() error {
	return nil
}

func (p *Provider) getContainers() []string {
	containers := p.config.Containers
	if len(containers) == 0 && p.config.Container != "" {
		containers = []string{p.config.Container}
	}
	return containers
}

func (p *Provider) Targets(ctx context.Context) ([]string, error) {
	return p.getContainers(), nil
}

func (p *Provider) Sync(ctx context.Context, tarData *bytes.Buffer) error {
	containers := p.getContainers()
	if len(containers) == 0 {
		return fmt.Errorf("no containers configured")
	}
	for _, containerID := range containers {
		if err := p.syncToContainer(ctx, containerID, tarData); err != nil {
			log.Printf("Failed to sync to container %s: %v", containerID, err)
		} else {
			log.Printf("Synced to container %s", containerID)
		}
	}
	return nil
}

func (p *Provider) syncToContainer(ctx context.Context, containerID string, tarData *bytes.Buffer) error {
	mkdirArgs := append(p.dockerArgs, "exec", containerID, "mkdir", "-p", p.config.RemotePath)
	if err := exec.CommandContext(ctx, "docker", mkdirArgs...).Run(); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}
	cpArgs := append(p.dockerArgs, "cp", "-", fmt.Sprintf("%s:%s", containerID, p.config.RemotePath))
	cmd := exec.CommandContext(ctx, "docker", cpArgs...)
	cmd.Stdin = tarData
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("docker cp failed: %w, stderr: %s", err, stderr.String())
	}
	return nil
}

func (p *Provider) Delete(ctx context.Context, relativePaths []string) error {
	containers := p.getContainers()
	for _, containerID := range containers {
		for _, relPath := range relativePaths {
			remotePath := filepath.Join(p.config.RemotePath, relPath)
			if err := p.deleteInContainer(ctx, containerID, remotePath); err != nil {
				log.Printf("Failed to delete %s in container %s: %v", remotePath, containerID, err)
			}
		}
	}
	return nil
}

func (p *Provider) deleteInContainer(ctx context.Context, containerID, remotePath string) error {
	args := append(p.dockerArgs, "exec", containerID, "rm", "-rf", remotePath)
	return exec.CommandContext(ctx, "docker", args...).Run()
}

func (p *Provider) RunCommand(ctx context.Context, command string) error {
	if command == "" {
		return nil
	}
	containers := p.getContainers()
	for _, containerID := range containers {
		log.Printf("Running command in container %s: %s", containerID, command)
		if err := p.runCommandInContainer(ctx, containerID, command); err != nil {
			log.Printf("Command failed in container %s: %v", containerID, err)
		}
	}
	return nil
}

func (p *Provider) runCommandInContainer(ctx context.Context, containerID, command string) error {
	args := append(p.dockerArgs, "exec", "-w", p.config.RemotePath, containerID, "sh", "-c", command)
	cmd := exec.CommandContext(ctx, "docker", args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if stdout.Len() > 0 {
		log.Printf("[%s stdout] %s", containerID, strings.TrimSpace(stdout.String()))
	}
	if stderr.Len() > 0 {
		log.Printf("[%s stderr] %s", containerID, strings.TrimSpace(stderr.String()))
	}
	return err
}
