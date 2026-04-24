# sink

A multi-provider file sync tool that watches local files and syncs them to various targets in real-time.

## Install

```bash
go install github.com/doodlesbykumbi/sink@latest
```

Or build from source:
```bash
git clone https://github.com/doodlesbykumbi/sink.git
cd sink
go build -o sink .
```

## Supported Providers

| Provider | Target | Use Case |
|----------|--------|----------|
| `kubernetes` | K8s Pods | Dev containers in clusters |
| `ssh` | Remote hosts | EC2, GCE, any SSH server |
| `s3` | S3 buckets | Static sites, Lambda layers |
| `docker` | Local containers | Local Docker development |

## Features

- **Multi-provider**: Sync to Kubernetes, SSH hosts, S3, or Docker containers
- **Recursive file watching**: Watches directories recursively using `fsnotify`
- **Incremental sync**: Only syncs changed files via tar streaming
- **Debouncing**: Batches rapid file changes to avoid excessive syncs
- **Delete support**: Removes files from targets when deleted locally
- **Multi-target sync**: Syncs to all matching pods/hosts/containers
- **Gitignore support**: Respects `.gitignore` and `.syncignore` patterns
- **Command execution**: Run commands after sync (Kubernetes, SSH, Docker)

## Prerequisites

- Go 1.22+
- Provider-specific requirements:
  - **Kubernetes**: `tar` in container, valid kubeconfig
  - **SSH**: SSH key or agent, `tar` on remote host
  - **S3**: AWS credentials (env, profile, or IAM role)
  - **Docker**: Docker CLI installed, daemon running

## Usage

### Kubernetes (default)

```bash
sink \
  -provider kubernetes \
  -local ./my-code \
  -namespace default \
  -selector app=myapp \
  -remote /app
```

### SSH (EC2, GCE, any host)

```bash
sink \
  -provider ssh \
  -local ./my-code \
  -ssh-host ec2-user@10.0.1.50 \
  -ssh-key ~/.ssh/id_rsa \
  -remote /home/ec2-user/app
```

Multiple hosts:
```bash
sink \
  -provider ssh \
  -ssh-hosts "10.0.1.50,10.0.1.51,10.0.1.52" \
  -ssh-user ubuntu \
  -remote /app
```

### S3 (static sites, Lambda layers)

```bash
sink \
  -provider s3 \
  -local ./dist \
  -s3-bucket my-website \
  -s3-prefix static/v1 \
  -s3-region us-east-1
```

With custom endpoint (MinIO/LocalStack):
```bash
sink \
  -provider s3 \
  -s3-bucket test \
  -s3-endpoint http://localhost:9000
```

### Docker (local containers)

```bash
sink \
  -provider docker \
  -local ./src \
  -docker-container myapp-dev \
  -remote /app \
  -command "npm run build"
```

## Flags

### Common Flags

| Flag | Default | Description |
|------|---------|-------------|
| `-provider` | `kubernetes` | Provider: kubernetes, ssh, s3, docker |
| `-local` | `.` | Local directory to watch |
| `-remote` | `/app` | Remote path to sync to |
| `-debounce` | `500ms` | Debounce duration for file changes |
| `-command` | (none) | Command to run after sync |
| `-no-gitignore` | `false` | Disable .gitignore/.syncignore support |

### Kubernetes Flags

| Flag | Default | Description |
|------|---------|-------------|
| `-selector` | (required) | Label selector (e.g., `app=myapp`) |
| `-namespace` | `default` | Kubernetes namespace |
| `-container` | (first) | Container name |
| `-context` | (current) | Kubectl context |

### SSH Flags

| Flag | Default | Description |
|------|---------|-------------|
| `-ssh-host` | (required) | SSH host (hostname:port) |
| `-ssh-hosts` | | Multiple hosts (comma-separated) |
| `-ssh-user` | | SSH username |
| `-ssh-key` | | Private key file |

### S3 Flags

| Flag | Default | Description |
|------|---------|-------------|
| `-s3-bucket` | (required) | S3 bucket name |
| `-s3-prefix` | | Key prefix (folder) |
| `-s3-region` | | AWS region |
| `-s3-endpoint` | | Custom endpoint (MinIO) |

### Docker Flags

| Flag | Default | Description |
|------|---------|-------------|
| `-docker-container` | (required) | Container name/ID |
| `-docker-containers` | | Multiple containers (comma-separated) |

## Ignore Patterns

By default, the tool respects ignore patterns from:

1. **`.syncignore`** (takes precedence) - project-specific sync ignores
2. **`.gitignore`** - standard git ignores
3. **`.git/`** - always ignored

Create a `.syncignore` file to customize what gets synced:

```gitignore
# Don't sync build artifacts
bin/
*.exe

# Don't sync test files
*_test.go

# Don't sync vendor (use go mod in container)
vendor/
```

## Quick Start Demo

1. **Deploy the sample pod (has reflex for hot-reload):**
   ```bash
   kubectl apply -f deploy/sample-pod.yaml
   ```

2. **Create a test directory with a Go file:**
   ```bash
   mkdir -p /tmp/sync-test
   cat > /tmp/sync-test/main.go << 'EOF'
   package main

   import (
       "fmt"
       "time"
   )

   func main() {
       for {
           fmt.Println("Hello from the pod!")
           time.Sleep(2 * time.Second)
       }
   }
   EOF
   ```

3. **Start the syncer (no -command needed, reflex handles restart):**
   ```bash
   sink \
     -local /tmp/sync-test \
     -namespace live-sync-demo \
     -selector app=demo-app
   ```

4. **Watch the pod logs:**
   ```bash
   kubectl logs -f -n live-sync-demo -l app=demo-app
   ```

5. **Edit the file - reflex auto-restarts the process:**
   ```bash
   cat > /tmp/sync-test/main.go << 'EOF'
   package main

   import (
       "fmt"
       "time"
   )

   func main() {
       for {
           fmt.Println("Updated message!")
           time.Sleep(2 * time.Second)
       }
   }
   EOF
   ```

## Architecture

```
                                    ┌─────────────────────────────┐
                                    │      Kubernetes Pods        │
                                ┌──▶│  (tar extract via exec)     │
                                │   └─────────────────────────────┘
                                │
┌─────────────────┐             │   ┌─────────────────────────────┐
│  Local Machine  │  Provider   │   │      SSH Hosts              │
│                 │─────────────┼──▶│  (tar stream via SSH)       │
│  sink           │  Interface  │   └─────────────────────────────┘
│    watches      │             │
│    local files  │             │   ┌─────────────────────────────┐
│    debounces    │             ├──▶│      S3 Buckets             │
│    creates tar  │             │   │  (PutObject per file)       │
└─────────────────┘             │   └─────────────────────────────┘
                                │
                                │   ┌─────────────────────────────┐
                                └──▶│      Docker Containers      │
                                    │  (docker cp)                │
                                    └─────────────────────────────┘
```

## Provider Interface

All providers implement:
```go
type Provider interface {
    Name() string
    Init(ctx context.Context) error
    Close() error
    Sync(ctx context.Context, tarData *bytes.Buffer) error
    Delete(ctx context.Context, relativePaths []string) error
    Targets(ctx context.Context) ([]string, error)
    RunCommand(ctx context.Context, command string) error
}
```

## How It Works

1. **File watching**: `fsnotify` watches directories recursively
2. **Ignore patterns**: Loads `.syncignore` then `.gitignore` patterns
3. **Change batching**: Changes are debounced to batch rapid edits
4. **Tar creation**: Changed files are packaged into a tar archive
5. **Provider sync**: The selected provider transfers files to the target
6. **Command execution**: Optional post-sync command (provider-dependent)

## Provider Details

| Provider | Sync Method | Command Support | Multi-target |
|----------|-------------|-----------------|--------------|
| Kubernetes | `exec tar -xf` | ✅ via exec | ✅ all matching pods |
| SSH | SSH session + tar | ✅ via SSH | ✅ multiple hosts |
| S3 | PutObject API | ❌ N/A | Single bucket |
| Docker | `docker cp` | ✅ via exec | ✅ multiple containers |

## Limitations

- **Kubernetes/SSH/Docker**: Target must have `tar` installed
- **Kubernetes/SSH/Docker**: Target must have `sh` for command execution
- **S3**: No command execution (object storage only)
- Remote path must be writable
- No support for symlinks
- Hidden files (starting with `.`) are skipped
