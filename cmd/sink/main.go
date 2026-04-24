package main

import (
	"context"
	"flag"
	"log"
	"path/filepath"
	"strings"
	"time"

	"github.com/doodlesbykumbi/sink/pkg/provider"
	"github.com/doodlesbykumbi/sink/pkg/provider/docker"
	"github.com/doodlesbykumbi/sink/pkg/provider/kubernetes"
	"github.com/doodlesbykumbi/sink/pkg/provider/s3"
	"github.com/doodlesbykumbi/sink/pkg/provider/ssh"
	"github.com/doodlesbykumbi/sink/pkg/syncer"
)

func main() {
	var (
		// Common flags
		localPath   = flag.String("local", ".", "Local directory to watch")
		remotePath  = flag.String("remote", "/app", "Remote path to sync to")
		debounce    = flag.Duration("debounce", 500*time.Millisecond, "Debounce duration for file changes")
		command     = flag.String("command", "", "Command to run after sync (provider-dependent)")
		noGitignore = flag.Bool("no-gitignore", false, "Disable .gitignore/.syncignore support")
		providerStr = flag.String("provider", "kubernetes", "Sync provider: kubernetes, ssh, s3, docker")

		// Kubernetes flags
		k8sNamespace = flag.String("namespace", "default", "Kubernetes namespace")
		k8sSelector  = flag.String("selector", "", "Kubernetes label selector (e.g., app=myapp)")
		k8sContainer = flag.String("container", "", "Kubernetes container name (optional)")
		k8sContext   = flag.String("context", "", "Kubernetes context (optional)")

		// SSH flags
		sshHost  = flag.String("ssh-host", "", "SSH host (hostname:port)")
		sshUser  = flag.String("ssh-user", "", "SSH username")
		sshKey   = flag.String("ssh-key", "", "SSH private key file")
		sshHosts = flag.String("ssh-hosts", "", "Multiple SSH hosts (comma-separated)")

		// S3 flags
		s3Bucket   = flag.String("s3-bucket", "", "S3 bucket name")
		s3Prefix   = flag.String("s3-prefix", "", "S3 key prefix")
		s3Region   = flag.String("s3-region", "", "AWS region")
		s3Endpoint = flag.String("s3-endpoint", "", "S3 endpoint (for MinIO/LocalStack)")

		// Docker flags
		dockerContainer  = flag.String("docker-container", "", "Docker container name/ID")
		dockerContainers = flag.String("docker-containers", "", "Multiple containers (comma-separated)")
	)

	flag.Parse()

	// Resolve to absolute path
	absPath, err := filepath.Abs(*localPath)
	if err != nil {
		log.Fatalf("Failed to resolve local path: %v", err)
	}

	// Create provider
	p, err := createProvider(*providerStr, *remotePath, providerOptions{
		k8sNamespace:     *k8sNamespace,
		k8sSelector:      *k8sSelector,
		k8sContainer:     *k8sContainer,
		k8sContext:       *k8sContext,
		sshHost:          *sshHost,
		sshUser:          *sshUser,
		sshKey:           *sshKey,
		sshHosts:         *sshHosts,
		s3Bucket:         *s3Bucket,
		s3Prefix:         *s3Prefix,
		s3Region:         *s3Region,
		s3Endpoint:       *s3Endpoint,
		dockerContainer:  *dockerContainer,
		dockerContainers: *dockerContainers,
	})
	if err != nil {
		log.Fatalf("Failed to create provider: %v", err)
	}

	if err := p.Init(context.Background()); err != nil {
		log.Fatalf("Failed to initialize provider: %v", err)
	}
	defer p.Close()

	// Create syncer
	s, err := syncer.New(syncer.Config{
		LocalPath:   absPath,
		RemotePath:  *remotePath,
		Debounce:    *debounce,
		Command:     *command,
		NoGitignore: *noGitignore,
	}, p)
	if err != nil {
		log.Fatalf("Failed to create syncer: %v", err)
	}
	defer s.Close()

	if err := s.Run(context.Background()); err != nil {
		log.Fatalf("Syncer error: %v", err)
	}
}

type providerOptions struct {
	k8sNamespace, k8sSelector, k8sContainer, k8sContext string
	sshHost, sshUser, sshKey, sshHosts                  string
	s3Bucket, s3Prefix, s3Region, s3Endpoint            string
	dockerContainer, dockerContainers                   string
}

func createProvider(name, remotePath string, opts providerOptions) (provider.Provider, error) {
	switch name {
	case "kubernetes", "k8s":
		if opts.k8sSelector == "" {
			log.Fatal("kubernetes provider requires -selector flag")
		}
		return kubernetes.New(kubernetes.Config{
			Config:        provider.Config{RemotePath: remotePath},
			Namespace:     opts.k8sNamespace,
			LabelSelector: opts.k8sSelector,
			Container:     opts.k8sContainer,
			Context:       opts.k8sContext,
		}), nil

	case "ssh":
		if opts.sshHost == "" && opts.sshHosts == "" {
			log.Fatal("ssh provider requires -ssh-host or -ssh-hosts flag")
		}
		var hosts []string
		if opts.sshHosts != "" {
			hosts = strings.Split(opts.sshHosts, ",")
			for i := range hosts {
				hosts[i] = strings.TrimSpace(hosts[i])
			}
		}
		return ssh.New(ssh.Config{
			Config:   provider.Config{RemotePath: remotePath},
			Host:     opts.sshHost,
			Hosts:    hosts,
			User:     opts.sshUser,
			KeyFile:  opts.sshKey,
			UseAgent: true,
		}), nil

	case "s3":
		if opts.s3Bucket == "" {
			log.Fatal("s3 provider requires -s3-bucket flag")
		}
		return s3.New(s3.Config{
			Config:   provider.Config{RemotePath: remotePath},
			Bucket:   opts.s3Bucket,
			Prefix:   opts.s3Prefix,
			Region:   opts.s3Region,
			Endpoint: opts.s3Endpoint,
		}), nil

	case "docker":
		if opts.dockerContainer == "" && opts.dockerContainers == "" {
			log.Fatal("docker provider requires -docker-container or -docker-containers flag")
		}
		var containers []string
		if opts.dockerContainers != "" {
			containers = strings.Split(opts.dockerContainers, ",")
			for i := range containers {
				containers[i] = strings.TrimSpace(containers[i])
			}
		}
		return docker.New(docker.Config{
			Config:     provider.Config{RemotePath: remotePath},
			Container:  opts.dockerContainer,
			Containers: containers,
		}), nil

	default:
		log.Fatalf("unknown provider: %s (supported: kubernetes, ssh, s3, docker)", name)
		return nil, nil
	}
}
