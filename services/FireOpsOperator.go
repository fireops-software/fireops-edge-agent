package services

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/network"
	"github.com/gorilla/websocket"
	"github.com/uoul/go-common/collections"
	"github.com/uoul/go-common/log"

	"github.com/fireops-software/fireops-edge-agent/api"
	"github.com/fireops-software/fireops-edge-agent/domain"
	appError "github.com/fireops-software/fireops-edge-agent/error"
)

const (
	WS_BUFFER_SIZE = 1024

	MESSAGE_TYPE_GET_CONTAINERS     = msgType("GetContainers")
	MESSAGE_TYPE_INSTALL            = msgType("Install")
	MESSAGE_TYPE_DESTROY            = msgType("Destroy")
	MESSAGE_TYPE_GET_CONTAINER_LOGS = msgType("GetContainerLogs")
	MESSAGE_TYPE_GET_AGENT_VERSION  = msgType("GetAgentVersion")
)

// ---------------------------------------------------------------------------
// Service type
// ---------------------------------------------------------------------------
type FireOpsOperator struct {
	ctx          context.Context
	logger       log.ILogger
	dockerApi    api.IDockerApi
	fireOpsApi   url.URL
	fireOpsToken string

	fireopsNetwork string
	retryInterval  time.Duration
	agentVersion   string

	wsMux sync.Mutex
}

// ---------------------------------------------------------------------------
// Message Types
// ---------------------------------------------------------------------------
type msgType string

type wsRequest[T any] struct {
	MsgId   string
	MsgType msgType
	Body    T
}

type wsResponse[T any] struct {
	MsgId   string
	MsgType msgType
	Error   error
	Body    T
}

type getDeploymentRequest struct{}
type getDeploymentResponse []container.Summary

type installRequest []struct {
	ServiceName  string
	Image        string
	PortForwards map[uint16]uint16 // map[hostPort]containerPort
	Environment  map[string]string
}
type installResponse []container.Summary

type destroyRequest struct{}
type destroyResponse struct{}

type getAgentVersionRequest struct{}
type getAgentVersionResponse struct {
	Version string
}

type getLogsRequest struct {
	ContainerId string
	Len         uint
}
type getLogsResponse struct {
	LogEntries []domain.ContainerLogEntry
}

// ---------------------------------------------------------------------------
// Private
// ---------------------------------------------------------------------------

func (f *FireOpsOperator) run() error {
	// Create Websocket Config
	ws, _, err := websocket.DefaultDialer.DialContext(f.ctx, f.fireOpsApi.String(), http.Header{"Authorization": []string{f.fireOpsToken}})
	if err != nil {
		return appError.NewErrFireOpsApi("failed to create websocket - %v", err)
	}
	defer func() {
		// Send close message to client
		closeMessage := websocket.FormatCloseMessage(websocket.CloseNormalClosure, "fireops-edge-agent close connection")
		ws.WriteMessage(websocket.CloseMessage, closeMessage)
		// Close the connection
		ws.Close()
	}()
	// Handle incomming messages
	for {
		// Read Incomming Message
		raw := wsRequest[json.RawMessage]{}
		if err := ws.ReadJSON(&raw); err != nil {
			return err
		}
		// Route message based on message type
		switch raw.MsgType {
		case MESSAGE_TYPE_GET_CONTAINERS:
			f.logger.Tracef("New incomming %s request %v", MESSAGE_TYPE_GET_CONTAINERS, string(raw.Body))
			if err := routeMsg(ws, f.sendMessage, raw, f.handleGetDeploymentRequest); err != nil {
				return err
			}
		case MESSAGE_TYPE_INSTALL:
			f.logger.Tracef("New incomming %s request %v", MESSAGE_TYPE_INSTALL, string(raw.Body))
			if err := routeMsg(ws, f.sendMessage, raw, f.handleInstallRequest); err != nil {
				return err
			}
		case MESSAGE_TYPE_DESTROY:
			f.logger.Tracef("New incomming %s request %v", MESSAGE_TYPE_DESTROY, string(raw.Body))
			if err := routeMsg(ws, f.sendMessage, raw, f.handleDestroyRequest); err != nil {
				return err
			}
		case MESSAGE_TYPE_GET_CONTAINER_LOGS:
			f.logger.Tracef("New incomming %s request %v", MESSAGE_TYPE_GET_CONTAINER_LOGS, string(raw.Body))
			if err := routeMsg(ws, f.sendMessage, raw, f.handleGetLogsRequest); err != nil {
				return err
			}
		case MESSAGE_TYPE_GET_AGENT_VERSION:
			f.logger.Tracef("New incomming %s request %v", MESSAGE_TYPE_GET_AGENT_VERSION, string(raw.Body))
			if err := routeMsg(ws, f.sendMessage, raw, f.handleGetAgentVersionRequest); err != nil {
				return err
			}
		default:
			if err := f.sendMessage(ws, wsResponse[any]{MsgId: raw.MsgId, MsgType: raw.MsgType, Error: appError.NewErrUnsupportedMsgType("type %s is not supported", raw.MsgType), Body: nil}); err != nil {
				return err
			}
		}
	}
}

func (f *FireOpsOperator) sendMessage(ws *websocket.Conn, msg any) error {
	f.wsMux.Lock()
	defer f.wsMux.Unlock()
	return ws.WriteJSON(msg)
}

func routeMsg[I, O any](ws *websocket.Conn, sendFunc func(*websocket.Conn, any) error, msg wsRequest[json.RawMessage], handler func(wsRequest[I]) wsResponse[O]) error {
	body := *new(I)
	err := json.Unmarshal(msg.Body, &body)
	if err != nil {
		if err := sendFunc(ws, wsResponse[O]{MsgId: msg.MsgId, MsgType: msg.MsgType, Error: err, Body: *new(O)}); err != nil {
			return err
		}
	} else {
		if err := sendFunc(ws, handler(wsRequest[I]{MsgId: msg.MsgId, MsgType: msg.MsgType, Body: body})); err != nil {
			return err
		}
	}
	return nil
}

func (f *FireOpsOperator) cleanDocker(ctx context.Context) error {
	f.logger.Tracef("Listing all containers...")
	containers := <-f.dockerApi.ListContainers(ctx)
	if containers.Error != nil {
		return containers.Error
	}
	f.logger.Tracef("List of all containers runnning in context: %v", containers.Result)
	// Remove all containers
	for _, container := range containers.Result {
		f.logger.Tracef("Removing container %v...", container.Names)
		remove := <-f.dockerApi.RemoveContainer(ctx, container.ID)
		if remove.Error != nil {
			return remove.Error
		}
		f.logger.Tracef("Container %v removed", container.Names)
	}
	// Prune images
	f.logger.Tracef("Prune images...")
	imagePrune := <-f.dockerApi.PruneImages(ctx, true)
	if imagePrune.Error != nil {
		return imagePrune.Error
	}
	f.logger.Tracef("Images pruned %v", imagePrune.Result)
	// Prune networks
	f.logger.Tracef("Prune networks...")
	netPrune := <-f.dockerApi.PruneNetworks(ctx)
	if netPrune.Error != nil {
		return netPrune.Error
	}
	f.logger.Tracef("Networks pruned %v", netPrune.Result)
	// Return success
	return nil
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------
func (f *FireOpsOperator) handleGetDeploymentRequest(msg wsRequest[getDeploymentRequest]) wsResponse[getDeploymentResponse] {
	// Create Context with timeout for Docker api call
	ctx, cancel := context.WithTimeout(f.ctx, 10*time.Second)
	defer cancel()
	// Get Running containers from docker api
	f.logger.Tracef("Listing all containers...")
	containers := <-f.dockerApi.ListContainers(ctx)
	f.logger.Tracef("List of all containers runnning in context: %v", containers.Result)
	// Return current state
	return wsResponse[getDeploymentResponse]{
		MsgId:   msg.MsgId,
		MsgType: msg.MsgType,
		Error:   containers.Error,
		Body:    containers.Result,
	}
}

func (f *FireOpsOperator) handleInstallRequest(msg wsRequest[installRequest]) wsResponse[installResponse] {
	// Create Context with timeout for Docker api call
	ctx, cancel := context.WithTimeout(f.ctx, 60*time.Second)
	defer cancel()
	// Clean docker
	if err := f.cleanDocker(ctx); err != nil {
		return wsResponse[installResponse]{
			MsgId:   msg.MsgId,
			MsgType: msg.MsgType,
			Error:   err,
		}
	}
	// Create Network
	f.logger.Tracef("Creating docker network %s...", f.fireopsNetwork)
	dockerNet := <-f.dockerApi.CreateNetwork(ctx, f.fireopsNetwork, network.NetworkBridge)
	if dockerNet.Error != nil {
		return wsResponse[installResponse]{
			MsgId:   msg.MsgId,
			MsgType: msg.MsgType,
			Error:   dockerNet.Error,
		}
	}
	f.logger.Tracef("Nework %s created with id %s", f.fireopsNetwork, dockerNet.Result.ID)
	// Install services
	for _, service := range msg.Body {
		env := []string{}
		for k, v := range service.Environment {
			env = append(env, fmt.Sprintf("%s=%s", k, v))
		}
		// Create container
		f.logger.Tracef("Creating docker container %s...", service.ServiceName)
		create := <-f.dockerApi.CreateContainer(
			ctx,
			service.Image,
			service.ServiceName,
			[]string{dockerNet.Result.ID},
			collections.MapMap(
				service.PortForwards,
				func(hostPort, containerPort uint16) (api.HostPort, api.ContainerPort) {
					return api.HostPort(hostPort), api.ContainerPort(containerPort)
				}),
			env,
		)
		if create.Error != nil {
			return wsResponse[installResponse]{
				MsgId:   msg.MsgId,
				MsgType: msg.MsgType,
				Error:   create.Error,
			}
		}
		f.logger.Tracef("Container %s created with id %s", service.ServiceName, create.Result.ID)
		// Start container
		f.logger.Tracef("Starting docker container: %s...", service.ServiceName)
		start := <-f.dockerApi.StartContainer(ctx, create.Result.ID)
		if start.Error != nil {
			return wsResponse[installResponse]{
				MsgId:   msg.MsgId,
				MsgType: msg.MsgType,
				Error:   start.Error,
			}
		}
		f.logger.Tracef("Container %s started", service.ServiceName)
	}
	// Get containers
	f.logger.Tracef("List running containers...")
	containers := <-f.dockerApi.ListContainers(ctx)
	if containers.Error != nil {
		return wsResponse[installResponse]{
			MsgId:   msg.MsgId,
			MsgType: msg.MsgType,
			Error:   containers.Error,
		}
	}
	f.logger.Tracef("Running containers: %v", containers.Result)
	// Return result
	return wsResponse[installResponse]{
		MsgId:   msg.MsgId,
		MsgType: msg.MsgType,
		Error:   nil,
		Body:    containers.Result,
	}
}

func (f *FireOpsOperator) handleDestroyRequest(msg wsRequest[destroyRequest]) wsResponse[destroyResponse] {
	// Create Context with timeout for Docker api call
	ctx, cancel := context.WithTimeout(f.ctx, 30*time.Second)
	defer cancel()
	return wsResponse[destroyResponse]{
		MsgId:   msg.MsgId,
		MsgType: msg.MsgType,
		Error:   f.cleanDocker(ctx),
	}
}

func (f *FireOpsOperator) handleGetLogsRequest(msg wsRequest[getLogsRequest]) wsResponse[getLogsResponse] {
	// Create Context with timeout for Docker api call
	ctx, cancel := context.WithTimeout(f.ctx, 30*time.Second)
	defer cancel()
	// Get logs of container
	logEntries := <-f.dockerApi.GetContainerLogs(ctx, msg.Body.ContainerId, msg.Body.Len)
	return wsResponse[getLogsResponse]{
		MsgId:   msg.MsgId,
		MsgType: msg.MsgType,
		Error:   logEntries.Error,
		Body: getLogsResponse{
			LogEntries: logEntries.Result,
		},
	}
}

func (f *FireOpsOperator) handleGetAgentVersionRequest(msg wsRequest[getAgentVersionRequest]) wsResponse[getAgentVersionResponse] {
	// Return version
	return wsResponse[getAgentVersionResponse]{
		MsgId:   msg.MsgId,
		MsgType: msg.MsgType,
		Error:   nil,
		Body: getAgentVersionResponse{
			Version: f.agentVersion,
		},
	}
}

// ---------------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------------

func WithFireOpsOperatorNetwork(network string) func(*FireOpsOperator) {
	return func(foo *FireOpsOperator) {
		foo.fireopsNetwork = network
	}
}

func WithFireOpsOperatorVersion(version string) func(*FireOpsOperator) {
	return func(foo *FireOpsOperator) {
		foo.agentVersion = version
	}
}

// ---------------------------------------------------------------------------
// Constructor
// ---------------------------------------------------------------------------
func NewFireOpsOperator(ctx context.Context, logger log.ILogger, dockerApi api.IDockerApi, fireOpsApi url.URL, fireOpsToken string, opts ...func(*FireOpsOperator)) *FireOpsOperator {
	f := &FireOpsOperator{
		ctx:          ctx,
		logger:       logger,
		dockerApi:    dockerApi,
		fireOpsApi:   fireOpsApi,
		fireOpsToken: fireOpsToken,

		fireopsNetwork: "fireops",
		retryInterval:  10 * time.Second,
		wsMux:          sync.Mutex{},
		agentVersion:   "",
	}
	for _, o := range opts {
		o(f)
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				err := f.run()
				if err == nil {
					return
				}
				f.logger.Error(err.Error())
				time.Sleep(f.retryInterval)
			}
		}
	}()
	return f
}
