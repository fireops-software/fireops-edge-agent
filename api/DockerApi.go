package api

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/uoul/go-common/async"
	"github.com/uoul/go-common/log"

	"github.com/fireops-software/fireops-edge-agent/domain"
	appError "github.com/fireops-software/fireops-edge-agent/error"
)

const (
	LABEL = "label"
)

type DockerApi struct {
	docker *client.Client
	logger log.ILogger

	namespace string
}

// GetContainerLogs implements IDockerApi.
func (d *DockerApi) GetContainerLogs(ctx context.Context, containerId string, len uint) chan async.ActionResult[[]domain.ContainerLogEntry] {
	r := make(chan async.ActionResult[[]domain.ContainerLogEntry], 1)
	go func() {
		reader, err := d.docker.ContainerLogs(ctx, containerId, container.LogsOptions{
			ShowStdout: true,
			ShowStderr: true,
			Timestamps: false,
			Follow:     false,
			Tail:       fmt.Sprintf("%d", len),
		})
		if err != nil {
			r <- async.NewErrorActionResult[[]domain.ContainerLogEntry](
				appError.NewErrDockerApi("failed to create log reader - %v", err),
			)
			return
		}
		defer reader.Close()
		logEntries, err := parseDockerLogs(reader)
		if err != nil {
			r <- async.NewErrorActionResult[[]domain.ContainerLogEntry](
				appError.NewErrDockerApi("failed to parse container logs"),
			)
			return
		}
		r <- async.ActionResult[[]domain.ContainerLogEntry]{
			Result: logEntries,
			Error:  nil,
		}
	}()
	return r
}

// CreateNetwork implements IDockerApi.
func (d *DockerApi) CreateNetwork(ctx context.Context, name string, driver string) chan async.ActionResult[network.CreateResponse] {
	r := make(chan async.ActionResult[network.CreateResponse], 1)
	go func() {
		n, err := d.docker.NetworkCreate(ctx, name, network.CreateOptions{Driver: driver, Labels: map[string]string{d.namespace: ""}})
		if err != nil {
			r <- async.NewErrorActionResult[network.CreateResponse](appError.NewErrDockerApi("failed to create network - %v", err))
			return
		}
		r <- async.ActionResult[network.CreateResponse]{
			Result: n,
			Error:  nil,
		}
	}()
	return r
}

// PruneNetworks implements IDockerApi.
func (d *DockerApi) PruneNetworks(ctx context.Context) chan async.ActionResult[network.PruneReport] {
	r := make(chan async.ActionResult[network.PruneReport], 1)
	go func() {
		filterArgs := filters.NewArgs()
		filterArgs.Add(LABEL, d.namespace)
		report, err := d.docker.NetworksPrune(ctx, filterArgs)
		if err != nil {
			r <- async.NewErrorActionResult[network.PruneReport](appError.NewErrDockerApi("failed to prune networks"))
			return
		}
		r <- async.ActionResult[network.PruneReport]{
			Result: report,
			Error:  nil,
		}
	}()
	return r
}

// ListImages implements IDockerApi.
func (d *DockerApi) PruneImages(ctx context.Context, all bool) chan async.ActionResult[image.PruneReport] {
	r := make(chan async.ActionResult[image.PruneReport], 1)
	go func() {
		filterArgs := filters.NewArgs()
		if all {
			filterArgs.Add("dangling", "false")
		}
		report, err := d.docker.ImagesPrune(ctx, filterArgs)
		if err != nil {
			r <- async.NewErrorActionResult[image.PruneReport](appError.NewErrDockerApi("failed to prune images"))
			return
		}
		r <- async.ActionResult[image.PruneReport]{
			Result: report,
			Error:  nil,
		}
	}()
	return r
}

// ListContainers implements IDockerApi.
func (d *DockerApi) ListContainers(ctx context.Context) chan async.ActionResult[[]container.Summary] {
	r := make(chan async.ActionResult[[]container.Summary], 1)
	go func() {
		containers, err := d.docker.ContainerList(ctx, container.ListOptions{All: true, Filters: filters.NewArgs(filters.Arg(LABEL, d.namespace))})
		if err != nil {
			r <- async.NewErrorActionResult[[]container.Summary](appError.NewErrDockerApi("failed to list containers - %v", err))
			return
		}
		r <- async.ActionResult[[]container.Summary]{
			Result: containers,
			Error:  nil,
		}
	}()
	return r
}

// CreateContainer implements IDockerApi.
func (d *DockerApi) CreateContainer(ctx context.Context, img string, name string, networkIds []string, portForwards map[HostPort]ContainerPort, env []string) chan async.ActionResult[container.CreateResponse] {
	r := make(chan async.ActionResult[container.CreateResponse], 1)
	go func() {
		if err := d.pullImage(ctx, img); err != nil {
			r <- async.NewErrorActionResult[container.CreateResponse](err)
			return
		}
		portBindings, exposedPorts := createPortBindings(portForwards)
		resp, err := d.docker.ContainerCreate(ctx,
			&container.Config{
				Hostname:     name,
				Domainname:   name,
				Env:          env,
				Image:        img,
				ExposedPorts: exposedPorts,
				Labels: map[string]string{
					d.namespace: "",
				},
			},
			&container.HostConfig{
				PortBindings: portBindings,
				//RestartPolicy: container.RestartPolicy{Name: container.RestartPolicyUnlessStopped},
			},
			&network.NetworkingConfig{}, v1.DescriptorEmptyJSON.Platform, name)
		if err != nil {
			r <- async.NewErrorActionResult[container.CreateResponse](appError.NewErrDockerApi("failed to create container - %v", err))
			return
		}
		// Connect to network
		if len(networkIds) > 0 {
			for _, netId := range networkIds {
				if err := d.docker.NetworkConnect(ctx, netId, resp.ID, &network.EndpointSettings{}); err != nil {
					r <- async.NewErrorActionResult[container.CreateResponse](appError.NewErrDockerApi("failed connect container %s to network %s - %v", resp.ID, netId, err))
					return
				}
			}
		}
		r <- async.ActionResult[container.CreateResponse]{
			Result: resp,
			Error:  nil,
		}
	}()
	return r
}

// RemoveContainer implements IDockerApi.
func (d *DockerApi) RemoveContainer(ctx context.Context, containerId string) chan async.ActionResult[any] {
	r := make(chan async.ActionResult[any], 1)
	go func() {
		r <- async.ActionResult[any]{
			Result: nil,
			Error:  d.docker.ContainerRemove(ctx, containerId, container.RemoveOptions{Force: true}),
		}
	}()
	return r
}

// StartContainer implements IDockerApi.
func (d *DockerApi) StartContainer(ctx context.Context, containerId string) chan async.ActionResult[any] {
	r := make(chan async.ActionResult[any], 1)
	go func() {
		r <- async.ActionResult[any]{
			Result: nil,
			Error:  d.docker.ContainerStart(ctx, containerId, container.StartOptions{}),
		}
	}()
	return r
}

// Close implements IDockerApi
func (d *DockerApi) Close() error {
	return d.docker.Close()
}

func createPortBindings(portForwards map[HostPort]ContainerPort) (nat.PortMap, nat.PortSet) {
	portMap := nat.PortMap{}
	portSet := nat.PortSet{}
	for hostPort, containerPort := range portForwards {
		cp, _ := nat.NewPort("tcp", containerPort.ToString())
		portMap[cp] = []nat.PortBinding{
			{
				HostIP:   "0.0.0.0",
				HostPort: hostPort.ToString(),
			},
		}
		portSet[cp] = struct{}{}
	}
	return portMap, portSet
}

func (d *DockerApi) pullImage(ctx context.Context, img string) error {
	// Pull image
	d.logger.Debugf("Pulling image %s...", img)
	reader, err := d.docker.ImagePull(ctx, img, image.PullOptions{})
	if err != nil {
		return appError.NewErrDockerApi("failed to pull image - %v", err)
	}
	defer reader.Close()
	if _, err := io.ReadAll(reader); err != nil {
		return appError.NewErrDockerApi("failed to read image - %v", err)
	}
	return nil
}

func parseDockerLogs(reader io.Reader) ([]domain.ContainerLogEntry, error) {
	entries := []domain.ContainerLogEntry{}
	for {
		// Read the 8-byte header
		header := make([]byte, 8)
		_, err := io.ReadFull(reader, header)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		// Extract stream type and message size
		streamType := header[0]
		messageSize := binary.BigEndian.Uint32(header[4:8])
		// Read the actual log message
		message := make([]byte, messageSize)
		_, err = io.ReadFull(reader, message)
		if err != nil {
			return nil, err
		}
		// Process based on stream type
		switch streamType {
		case 1: // stdout
			entries = append(entries, domain.ContainerLogEntry{
				Stream:  "stdout",
				Message: string(message),
			})
		case 2: // stderr
			entries = append(entries, domain.ContainerLogEntry{
				Stream:  "stderr",
				Message: string(message),
			})
		}
	}
	return entries, nil
}

func NewDockerApi(logger log.ILogger, namespace string, opts ...func(*DockerApi)) (IDockerApi, error) {
	dockerClient, err := client.NewClientWithOpts(
		client.FromEnv,
		client.WithAPIVersionNegotiation(),
	)
	if err != nil {
		return nil, appError.NewErrDockerApi("failed to create docker client - %v", err)
	}
	d := &DockerApi{
		docker:    dockerClient,
		namespace: namespace,
		logger:    logger,
	}
	for _, o := range opts {
		o(d)
	}
	return d, nil
}
