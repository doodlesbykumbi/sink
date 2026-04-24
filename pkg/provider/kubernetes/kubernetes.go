package kubernetes

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"path/filepath"
	"strings"

	"github.com/doodlesbykumbi/sink/pkg/provider"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/remotecommand"
)

// Config holds Kubernetes-specific configuration
type Config struct {
	provider.Config
	Namespace     string
	LabelSelector string
	Container     string
	Kubeconfig    string // Optional path to kubeconfig file
	Context       string // Optional kubectl context to use
}

// Provider implements the provider.Provider interface for Kubernetes
type Provider struct {
	config     Config
	client     *kubernetes.Clientset
	restConfig *rest.Config
}

// Compile-time check that Provider implements provider.Provider
var _ provider.Provider = (*Provider)(nil)

// New creates a new Kubernetes provider
func New(cfg Config) *Provider {
	return &Provider{config: cfg}
}

func (p *Provider) Name() string {
	return "kubernetes"
}

func (p *Provider) Init(ctx context.Context) error {
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	if p.config.Kubeconfig != "" {
		loadingRules.ExplicitPath = p.config.Kubeconfig
	}

	configOverrides := &clientcmd.ConfigOverrides{}
	if p.config.Context != "" {
		configOverrides.CurrentContext = p.config.Context
	}

	kubeConfig := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, configOverrides)

	restConfig, err := kubeConfig.ClientConfig()
	if err != nil {
		return fmt.Errorf("failed to load kubeconfig: %w", err)
	}

	client, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return fmt.Errorf("failed to create kubernetes client: %w", err)
	}

	p.restConfig = restConfig
	p.client = client

	return nil
}

func (p *Provider) Close() error {
	return nil
}

func (p *Provider) Targets(ctx context.Context) ([]string, error) {
	pods, err := p.getReadyPods(ctx)
	if err != nil {
		return nil, err
	}

	targets := make([]string, len(pods))
	for i, pod := range pods {
		targets[i] = fmt.Sprintf("%s/%s", pod.Namespace, pod.Name)
	}
	return targets, nil
}

func (p *Provider) getReadyPods(ctx context.Context) ([]corev1.Pod, error) {
	podList, err := p.client.CoreV1().Pods(p.config.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: p.config.LabelSelector,
	})
	if err != nil {
		return nil, err
	}

	var readyPods []corev1.Pod
	for _, pod := range podList.Items {
		if pod.Status.Phase == corev1.PodRunning {
			for _, cond := range pod.Status.Conditions {
				if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
					readyPods = append(readyPods, pod)
					break
				}
			}
		}
	}
	return readyPods, nil
}

func (p *Provider) Sync(ctx context.Context, tarData *bytes.Buffer) error {
	pods, err := p.getReadyPods(ctx)
	if err != nil {
		return fmt.Errorf("failed to get pods: %w", err)
	}

	if len(pods) == 0 {
		return fmt.Errorf("no ready pods found with selector '%s'", p.config.LabelSelector)
	}

	for _, pod := range pods {
		container := p.getContainer(pod)
		if err := p.execTarExtract(ctx, pod.Name, container, tarData); err != nil {
			log.Printf("Failed to sync to pod %s: %v", pod.Name, err)
		} else {
			log.Printf("Synced to pod %s", pod.Name)
		}
	}

	return nil
}

func (p *Provider) Delete(ctx context.Context, relativePaths []string) error {
	pods, err := p.getReadyPods(ctx)
	if err != nil {
		return fmt.Errorf("failed to get pods: %w", err)
	}

	for _, pod := range pods {
		container := p.getContainer(pod)
		for _, relPath := range relativePaths {
			remotePath := filepath.Join(p.config.RemotePath, relPath)
			if err := p.execDelete(ctx, pod.Name, container, remotePath); err != nil {
				log.Printf("Failed to delete %s on pod %s: %v", remotePath, pod.Name, err)
			}
		}
	}

	return nil
}

func (p *Provider) RunCommand(ctx context.Context, command string) error {
	if command == "" {
		return nil
	}

	pods, err := p.getReadyPods(ctx)
	if err != nil {
		return fmt.Errorf("failed to get pods: %w", err)
	}

	for _, pod := range pods {
		container := p.getContainer(pod)
		log.Printf("Running command in pod %s: %s", pod.Name, command)
		if err := p.execCommand(ctx, pod.Name, container, command); err != nil {
			log.Printf("Command failed in pod %s: %v", pod.Name, err)
		}
	}

	return nil
}

func (p *Provider) getContainer(pod corev1.Pod) string {
	if p.config.Container != "" {
		return p.config.Container
	}
	if len(pod.Spec.Containers) > 0 {
		return pod.Spec.Containers[0].Name
	}
	return ""
}

func (p *Provider) execTarExtract(ctx context.Context, podName, container string, tarData *bytes.Buffer) error {
	req := p.client.CoreV1().RESTClient().
		Post().
		Resource("pods").
		Name(podName).
		Namespace(p.config.Namespace).
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: container,
			Command:   []string{"tar", "-xf", "-", "-C", p.config.RemotePath},
			Stdin:     true,
			Stdout:    true,
			Stderr:    true,
		}, scheme.ParameterCodec)

	exec, err := remotecommand.NewSPDYExecutor(p.restConfig, "POST", req.URL())
	if err != nil {
		return fmt.Errorf("failed to create executor: %w", err)
	}

	var stdout, stderr bytes.Buffer
	err = exec.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdin:  bytes.NewReader(tarData.Bytes()),
		Stdout: &stdout,
		Stderr: &stderr,
	})

	if err != nil {
		return fmt.Errorf("exec failed: %w, stderr: %s", err, stderr.String())
	}

	return nil
}

func (p *Provider) execDelete(ctx context.Context, podName, container, remotePath string) error {
	req := p.client.CoreV1().RESTClient().
		Post().
		Resource("pods").
		Name(podName).
		Namespace(p.config.Namespace).
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: container,
			Command:   []string{"rm", "-rf", remotePath},
			Stdout:    true,
			Stderr:    true,
		}, scheme.ParameterCodec)

	exec, err := remotecommand.NewSPDYExecutor(p.restConfig, "POST", req.URL())
	if err != nil {
		return err
	}

	var stdout, stderr bytes.Buffer
	return exec.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdout: &stdout,
		Stderr: &stderr,
	})
}

func (p *Provider) execCommand(ctx context.Context, podName, container, command string) error {
	req := p.client.CoreV1().RESTClient().
		Post().
		Resource("pods").
		Name(podName).
		Namespace(p.config.Namespace).
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: container,
			Command:   []string{"sh", "-c", command},
			Stdout:    true,
			Stderr:    true,
		}, scheme.ParameterCodec)

	exec, err := remotecommand.NewSPDYExecutor(p.restConfig, "POST", req.URL())
	if err != nil {
		return fmt.Errorf("failed to create executor: %w", err)
	}

	var stdout, stderr bytes.Buffer
	err = exec.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdout: &stdout,
		Stderr: &stderr,
	})

	if stdout.Len() > 0 {
		log.Printf("[%s stdout] %s", podName, strings.TrimSpace(stdout.String()))
	}
	if stderr.Len() > 0 {
		log.Printf("[%s stderr] %s", podName, strings.TrimSpace(stderr.String()))
	}

	if err != nil {
		return fmt.Errorf("command failed: %w", err)
	}
	return nil
}
