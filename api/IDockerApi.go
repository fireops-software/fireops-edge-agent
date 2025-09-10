package api

import (
	"context"
	"fmt"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/network"
	"github.com/fireops-software/fireops-edge-agent/domain"
	"github.com/uoul/go-common/async"
)

type HostPort uint16

func (p HostPort) ToString() string {
	return fmt.Sprintf("%v", p)
}

type ContainerPort uint16

func (p ContainerPort) ToString() string {
	return fmt.Sprintf("%v", p)
}

type IDockerApi interface {
	Close() error
	// Container management
	ListContainers(ctx context.Context) chan async.ActionResult[[]container.Summary]
	CreateContainer(ctx context.Context, image string, name string, networkIds []string, portForwards map[HostPort]ContainerPort, env []string) chan async.ActionResult[container.CreateResponse]
	StartContainer(ctx context.Context, containerId string) chan async.ActionResult[any]
	RemoveContainer(ctx context.Context, containerId string) chan async.ActionResult[any]
	GetContainerLogs(ctx context.Context, containerId string, len uint) chan async.ActionResult[[]domain.ContainerLogEntry]
	// Image management
	PruneImages(ctx context.Context, all bool) chan async.ActionResult[image.PruneReport]
	// Networks
	CreateNetwork(ctx context.Context, name string, driver string) chan async.ActionResult[network.CreateResponse]
	PruneNetworks(ctx context.Context) chan async.ActionResult[network.PruneReport]
}
