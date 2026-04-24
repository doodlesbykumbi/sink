package s3

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/doodlesbykumbi/sink/pkg/provider"
)

// Config holds S3-specific configuration
type Config struct {
	provider.Config
	Bucket          string // S3 bucket name
	Prefix          string // Key prefix (folder path in bucket)
	Region          string // AWS region
	Endpoint        string // Custom endpoint (for MinIO, LocalStack, etc.)
	AccessKeyID     string // AWS access key (optional, uses default chain if empty)
	SecretAccessKey string // AWS secret key (optional)
	Profile         string // AWS profile name (optional)
}

// Provider implements the provider.Provider interface for S3
type Provider struct {
	config Config
	client *s3.Client
}

// Compile-time check that Provider implements provider.Provider
var _ provider.Provider = (*Provider)(nil)

// New creates a new S3 provider
func New(cfg Config) *Provider {
	return &Provider{config: cfg}
}

func (p *Provider) Name() string {
	return "s3"
}

func (p *Provider) Init(ctx context.Context) error {
	if p.config.Bucket == "" {
		return fmt.Errorf("S3 bucket is required")
	}

	opts := []func(*config.LoadOptions) error{}

	if p.config.Region != "" {
		opts = append(opts, config.WithRegion(p.config.Region))
	}

	if p.config.Profile != "" {
		opts = append(opts, config.WithSharedConfigProfile(p.config.Profile))
	}

	if p.config.AccessKeyID != "" && p.config.SecretAccessKey != "" {
		opts = append(opts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(
				p.config.AccessKeyID,
				p.config.SecretAccessKey,
				"",
			),
		))
	}

	cfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return fmt.Errorf("failed to load AWS config: %w", err)
	}

	s3Opts := []func(*s3.Options){}
	if p.config.Endpoint != "" {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(p.config.Endpoint)
			o.UsePathStyle = true // Required for MinIO/LocalStack
		})
	}

	p.client = s3.NewFromConfig(cfg, s3Opts...)

	// Verify bucket access
	_, err = p.client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(p.config.Bucket),
	})
	if err != nil {
		return fmt.Errorf("failed to access bucket %s: %w", p.config.Bucket, err)
	}

	log.Printf("Connected to S3 bucket: %s", p.config.Bucket)
	return nil
}

func (p *Provider) Close() error {
	return nil
}

func (p *Provider) Targets(ctx context.Context) ([]string, error) {
	target := fmt.Sprintf("s3://%s", p.config.Bucket)
	if p.config.Prefix != "" {
		target = fmt.Sprintf("%s/%s", target, p.config.Prefix)
	}
	return []string{target}, nil
}

func (p *Provider) Sync(ctx context.Context, tarData *bytes.Buffer) error {
	// Extract tar and upload each file
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

		// Build S3 key
		key := header.Name
		if p.config.Prefix != "" {
			key = filepath.Join(p.config.Prefix, header.Name)
		}
		// Normalize path separators for S3
		key = strings.ReplaceAll(key, "\\", "/")

		// Read file content
		content, err := io.ReadAll(tr)
		if err != nil {
			log.Printf("Failed to read %s from tar: %v", header.Name, err)
			continue
		}

		// Upload to S3
		_, err = p.client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(p.config.Bucket),
			Key:    aws.String(key),
			Body:   bytes.NewReader(content),
		})
		if err != nil {
			log.Printf("Failed to upload %s: %v", key, err)
		} else {
			log.Printf("Uploaded: s3://%s/%s", p.config.Bucket, key)
		}
	}

	return nil
}

func (p *Provider) Delete(ctx context.Context, relativePaths []string) error {
	for _, relPath := range relativePaths {
		key := relPath
		if p.config.Prefix != "" {
			key = filepath.Join(p.config.Prefix, relPath)
		}
		key = strings.ReplaceAll(key, "\\", "/")

		_, err := p.client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(p.config.Bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			log.Printf("Failed to delete s3://%s/%s: %v", p.config.Bucket, key, err)
		} else {
			log.Printf("Deleted: s3://%s/%s", p.config.Bucket, key)
		}
	}
	return nil
}

func (p *Provider) RunCommand(ctx context.Context, command string) error {
	// S3 doesn't support running commands - this is a no-op
	if command != "" {
		log.Printf("Warning: S3 provider does not support running commands")
	}
	return nil
}
