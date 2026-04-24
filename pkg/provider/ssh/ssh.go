package ssh

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"

	"github.com/doodlesbykumbi/sink/pkg/provider"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"
)

// Config holds SSH-specific configuration
type Config struct {
	provider.Config
	Host           string   // hostname:port or just hostname (defaults to port 22)
	User           string   // SSH username
	KeyFile        string   // Path to private key file (optional if using agent)
	Password       string   // Password (optional, prefer key-based auth)
	Hosts          []string // Multiple hosts for multi-target sync
	UseAgent       bool     // Use SSH agent for authentication
	KnownHostsFile string   // Path to known_hosts file (optional)
}

// Provider implements the provider.Provider interface for SSH
type Provider struct {
	config  Config
	clients []*ssh.Client
}

// Compile-time check that Provider implements provider.Provider
var _ provider.Provider = (*Provider)(nil)

// New creates a new SSH provider
func New(cfg Config) *Provider {
	return &Provider{config: cfg}
}

func (p *Provider) Name() string {
	return "ssh"
}

func (p *Provider) Init(ctx context.Context) error {
	hosts := p.config.Hosts
	if len(hosts) == 0 && p.config.Host != "" {
		hosts = []string{p.config.Host}
	}

	if len(hosts) == 0 {
		return fmt.Errorf("no SSH hosts configured")
	}

	authMethods, err := p.getAuthMethods()
	if err != nil {
		return fmt.Errorf("failed to get auth methods: %w", err)
	}

	sshConfig := &ssh.ClientConfig{
		User:            p.config.User,
		Auth:            authMethods,
		HostKeyCallback: ssh.InsecureIgnoreHostKey(), // TODO: proper host key verification
	}

	for _, host := range hosts {
		// Add default port if not specified
		if !strings.Contains(host, ":") {
			host = host + ":22"
		}

		client, err := ssh.Dial("tcp", host, sshConfig)
		if err != nil {
			return fmt.Errorf("failed to connect to %s: %w", host, err)
		}
		p.clients = append(p.clients, client)
		log.Printf("Connected to SSH host: %s", host)
	}

	return nil
}

func (p *Provider) getAuthMethods() ([]ssh.AuthMethod, error) {
	var methods []ssh.AuthMethod

	// Try SSH agent first
	if p.config.UseAgent {
		if agentConn, err := net.Dial("unix", os.Getenv("SSH_AUTH_SOCK")); err == nil {
			methods = append(methods, ssh.PublicKeysCallback(agent.NewClient(agentConn).Signers))
		}
	}

	// Try key file
	if p.config.KeyFile != "" {
		key, err := os.ReadFile(p.config.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read key file: %w", err)
		}

		signer, err := ssh.ParsePrivateKey(key)
		if err != nil {
			return nil, fmt.Errorf("failed to parse private key: %w", err)
		}
		methods = append(methods, ssh.PublicKeys(signer))
	}

	// Try default key locations
	if p.config.KeyFile == "" {
		for _, keyPath := range []string{
			filepath.Join(os.Getenv("HOME"), ".ssh", "id_rsa"),
			filepath.Join(os.Getenv("HOME"), ".ssh", "id_ed25519"),
		} {
			if key, err := os.ReadFile(keyPath); err == nil {
				if signer, err := ssh.ParsePrivateKey(key); err == nil {
					methods = append(methods, ssh.PublicKeys(signer))
					break
				}
			}
		}
	}

	// Password auth as fallback
	if p.config.Password != "" {
		methods = append(methods, ssh.Password(p.config.Password))
	}

	if len(methods) == 0 {
		return nil, fmt.Errorf("no authentication methods available")
	}

	return methods, nil
}

func (p *Provider) Close() error {
	for _, client := range p.clients {
		client.Close()
	}
	return nil
}

func (p *Provider) Targets(ctx context.Context) ([]string, error) {
	targets := make([]string, len(p.clients))
	for i, client := range p.clients {
		targets[i] = client.RemoteAddr().String()
	}
	return targets, nil
}

func (p *Provider) Sync(ctx context.Context, tarData *bytes.Buffer) error {
	for _, client := range p.clients {
		if err := p.syncToClient(client, tarData); err != nil {
			log.Printf("Failed to sync to %s: %v", client.RemoteAddr(), err)
		} else {
			log.Printf("Synced to %s", client.RemoteAddr())
		}
	}
	return nil
}

func (p *Provider) syncToClient(client *ssh.Client, tarData *bytes.Buffer) error {
	session, err := client.NewSession()
	if err != nil {
		return fmt.Errorf("failed to create session: %w", err)
	}
	defer session.Close()

	// Pipe tar data to remote tar extract
	session.Stdin = bytes.NewReader(tarData.Bytes())

	var stderr bytes.Buffer
	session.Stderr = &stderr

	cmd := fmt.Sprintf("mkdir -p %s && tar -xf - -C %s", p.config.RemotePath, p.config.RemotePath)
	if err := session.Run(cmd); err != nil {
		return fmt.Errorf("tar extract failed: %w, stderr: %s", err, stderr.String())
	}

	return nil
}

func (p *Provider) Delete(ctx context.Context, relativePaths []string) error {
	for _, client := range p.clients {
		for _, relPath := range relativePaths {
			remotePath := filepath.Join(p.config.RemotePath, relPath)
			if err := p.deleteOnClient(client, remotePath); err != nil {
				log.Printf("Failed to delete %s on %s: %v", remotePath, client.RemoteAddr(), err)
			}
		}
	}
	return nil
}

func (p *Provider) deleteOnClient(client *ssh.Client, remotePath string) error {
	session, err := client.NewSession()
	if err != nil {
		return err
	}
	defer session.Close()

	return session.Run(fmt.Sprintf("rm -rf %s", remotePath))
}

func (p *Provider) RunCommand(ctx context.Context, command string) error {
	if command == "" {
		return nil
	}

	for _, client := range p.clients {
		log.Printf("Running command on %s: %s", client.RemoteAddr(), command)
		if err := p.runCommandOnClient(client, command); err != nil {
			log.Printf("Command failed on %s: %v", client.RemoteAddr(), err)
		}
	}
	return nil
}

func (p *Provider) runCommandOnClient(client *ssh.Client, command string) error {
	session, err := client.NewSession()
	if err != nil {
		return err
	}
	defer session.Close()

	var stdout, stderr bytes.Buffer
	session.Stdout = &stdout
	session.Stderr = &stderr

	// Change to remote directory before running command
	fullCmd := fmt.Sprintf("cd %s && %s", p.config.RemotePath, command)
	err = session.Run(fullCmd)

	if stdout.Len() > 0 {
		log.Printf("[%s stdout] %s", client.RemoteAddr(), strings.TrimSpace(stdout.String()))
	}
	if stderr.Len() > 0 {
		log.Printf("[%s stderr] %s", client.RemoteAddr(), strings.TrimSpace(stderr.String()))
	}

	return err
}

// CopyFile copies a single file via SCP (utility function)
func (p *Provider) CopyFile(client *ssh.Client, localPath, remotePath string) error {
	session, err := client.NewSession()
	if err != nil {
		return err
	}
	defer session.Close()

	file, err := os.Open(localPath)
	if err != nil {
		return err
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return err
	}

	go func() {
		w, _ := session.StdinPipe()
		defer w.Close()
		fmt.Fprintf(w, "C0644 %d %s\n", stat.Size(), filepath.Base(remotePath))
		io.Copy(w, file)
		fmt.Fprint(w, "\x00")
	}()

	return session.Run(fmt.Sprintf("scp -t %s", filepath.Dir(remotePath)))
}
