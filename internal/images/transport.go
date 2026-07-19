package images

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
)

const DefaultPlatform = "linux/arm64"

var (
	clusterNamePattern = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$`)
	imagePattern       = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._/@:+-]*$`)
	platformPattern    = regexp.MustCompile(`^[a-z0-9]+/[a-z0-9]+(?:/[A-Za-z0-9._-]+)?$`)
	peerPattern        = regexp.MustCompile(`^[A-Za-z0-9._-]+(?:@[A-Za-z0-9._:-]+)?$`)
)

type Options struct {
	Cluster         string
	Images          []string
	Peers           []string
	Platform        string
	Pull            bool
	ContainerBinary string
	SSHBinary       string
	Stdout          io.Writer
	Stderr          io.Writer
}

type Result struct {
	Images       []string
	Targets      []string
	ArchiveBytes int64
}

type commandRunner interface {
	Run(context.Context, io.Reader, io.Writer, io.Writer, string, ...string) error
}

type execRunner struct{}

func (execRunner) Run(ctx context.Context, stdin io.Reader, stdout, stderr io.Writer, binary string, arguments ...string) error {
	command := exec.CommandContext(ctx, binary, arguments...)
	command.Stdin = stdin
	command.Stdout = stdout
	command.Stderr = stderr
	return command.Run()
}

type Manager struct {
	runner commandRunner
}

func NewManager() *Manager {
	return &Manager{runner: execRunner{}}
}

func (m *Manager) Transfer(ctx context.Context, options Options) (Result, error) {
	options, err := normalize(options)
	if err != nil {
		return Result{}, err
	}
	result := Result{Images: append([]string(nil), options.Images...)}
	for _, image := range options.Images {
		if !options.Pull {
			continue
		}
		if err := m.runner.Run(ctx, nil, options.Stdout, options.Stderr, options.ContainerBinary,
			"image", "pull", "--platform", options.Platform, "--progress", "plain", image); err != nil {
			return result, fmt.Errorf("pull image %q: %w", image, err)
		}
	}

	temporaryDirectory, err := os.MkdirTemp("", "apc-image-sync-")
	if err != nil {
		return result, fmt.Errorf("create private image staging directory: %w", err)
	}
	defer func() {
		_ = os.Remove(filepath.Join(temporaryDirectory, "images.tar"))
		_ = os.Remove(temporaryDirectory)
	}()
	archive := filepath.Join(temporaryDirectory, "images.tar")
	saveArguments := []string{"image", "save", "--platform", options.Platform, "--output", archive}
	saveArguments = append(saveArguments, options.Images...)
	if err := m.runner.Run(ctx, nil, options.Stdout, options.Stderr, options.ContainerBinary, saveArguments...); err != nil {
		return result, fmt.Errorf("export OCI image archive: %w", err)
	}
	archiveInfo, err := os.Stat(archive)
	if err != nil {
		return result, fmt.Errorf("inspect OCI image archive: %w", err)
	}
	if !archiveInfo.Mode().IsRegular() || archiveInfo.Size() == 0 {
		return result, fmt.Errorf("OCI image archive is empty or not a regular file")
	}
	result.ArchiveBytes = archiveInfo.Size()

	serverContainer := "apc-k3s-" + options.Cluster + "-server"
	if err := m.importLocal(ctx, options, archive, serverContainer); err != nil {
		return result, err
	}
	result.Targets = append(result.Targets, serverContainer)

	agentContainer := "apc-k3s-" + options.Cluster + "-agent"
	for _, peer := range options.Peers {
		if err := m.importRemote(ctx, options, archive, peer, agentContainer); err != nil {
			return result, err
		}
		result.Targets = append(result.Targets, peer+"/"+agentContainer)
	}
	return result, nil
}

func (m *Manager) importLocal(ctx context.Context, options Options, archive, containerName string) error {
	file, err := os.Open(archive)
	if err != nil {
		return fmt.Errorf("open OCI image archive: %w", err)
	}
	defer file.Close()
	if err := m.runner.Run(ctx, file, options.Stdout, options.Stderr, options.ContainerBinary,
		"exec", "-i", containerName, "ctr", "-n", "k8s.io", "images", "import", "--platform", options.Platform, "-"); err != nil {
		return fmt.Errorf("import images into local K3s node: %w", err)
	}
	for _, image := range options.Images {
		var output bytes.Buffer
		arguments := []string{"exec", containerName, "ctr", "-n", "k8s.io", "images", "list", "-q", "name==" + image}
		if err := m.runner.Run(ctx, nil, &output, options.Stderr, options.ContainerBinary, arguments...); err != nil {
			return fmt.Errorf("verify image %q on local K3s node: %w", image, err)
		}
		if strings.TrimSpace(output.String()) != image {
			return fmt.Errorf("image %q was not found on local K3s node after import", image)
		}
		fmt.Fprintf(options.Stdout, "verified %s on %s\n", image, containerName)
	}
	return nil
}

func (m *Manager) importRemote(ctx context.Context, options Options, archive, peer, containerName string) error {
	file, err := os.Open(archive)
	if err != nil {
		return fmt.Errorf("open OCI image archive: %w", err)
	}
	defer file.Close()
	remoteImport := []string{
		"-o", "BatchMode=yes", peer,
		"/usr/local/bin/container", "exec", "-i", containerName,
		"ctr", "-n", "k8s.io", "images", "import", "--platform", options.Platform, "-",
	}
	if err := m.runner.Run(ctx, file, options.Stdout, options.Stderr, options.SSHBinary, remoteImport...); err != nil {
		return fmt.Errorf("stream images to %s: %w", peer, err)
	}
	for _, image := range options.Images {
		var output bytes.Buffer
		remoteCheck := []string{
			"-o", "BatchMode=yes", peer,
			"/usr/local/bin/container", "exec", containerName,
			"ctr", "-n", "k8s.io", "images", "list", "-q", "name==" + image,
		}
		if err := m.runner.Run(ctx, nil, &output, options.Stderr, options.SSHBinary, remoteCheck...); err != nil {
			return fmt.Errorf("verify image %q on %s: %w", image, peer, err)
		}
		if strings.TrimSpace(output.String()) != image {
			return fmt.Errorf("image %q was not found on %s after import", image, peer)
		}
		fmt.Fprintf(options.Stdout, "verified %s on %s\n", image, peer)
	}
	return nil
}

func normalize(options Options) (Options, error) {
	if !clusterNamePattern.MatchString(options.Cluster) {
		return Options{}, fmt.Errorf("cluster name must be a lowercase DNS label")
	}
	if len(options.Images) == 0 {
		return Options{}, fmt.Errorf("at least one image reference is required")
	}
	seenImages := make(map[string]struct{}, len(options.Images))
	images := make([]string, 0, len(options.Images))
	for _, image := range options.Images {
		if !imagePattern.MatchString(image) || strings.HasPrefix(image, "-") {
			return Options{}, fmt.Errorf("invalid image reference %q", image)
		}
		if _, exists := seenImages[image]; exists {
			continue
		}
		seenImages[image] = struct{}{}
		images = append(images, image)
	}
	options.Images = images
	if options.Platform == "" {
		options.Platform = DefaultPlatform
	}
	if !platformPattern.MatchString(options.Platform) {
		return Options{}, fmt.Errorf("platform must use os/architecture[/variant] format")
	}
	for _, peer := range options.Peers {
		if !peerPattern.MatchString(peer) || strings.HasPrefix(peer, "-") {
			return Options{}, fmt.Errorf("invalid SSH peer %q", peer)
		}
	}
	if options.ContainerBinary == "" {
		options.ContainerBinary = "/usr/local/bin/container"
	}
	if options.SSHBinary == "" {
		options.SSHBinary = "/usr/bin/ssh"
	}
	if options.Stdout == nil {
		options.Stdout = io.Discard
	}
	if options.Stderr == nil {
		options.Stderr = io.Discard
	}
	return options, nil
}
