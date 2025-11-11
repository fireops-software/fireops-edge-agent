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
	MESSAGE_TYPE_INSTALL            = msgType("InstallContainers")
	MESSAGE_TYPE_DESTROY            = msgType("DestroyContainers")
	MESSAGE_TYPE_GET_CONTAINER_LOGS = msgType("GetContainerLogs")
	MESSAGE_TYPE_GET_AGENT_VERSION  = msgType("GetAgentVersion")
)

// ---------------------------------------------------------------------------
// Service type
// ---------------------------------------------------------------------------
type FireOpsOperator struct {
	ctx           context.Context
	logger        log.ILogger
	dockerApi     api.IDockerApi
	fireOpsApi    url.URL
	fireOpsApiKey string

	fireopsNetwork string
	retryInterval  time.Duration
	maxReqDuration time.Duration
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
type getLogsResponse []domain.ContainerLogEntry

// ---------------------------------------------------------------------------
// Private
// ---------------------------------------------------------------------------

func (f *FireOpsOperator) run() error {
	// Check if Docker is running
	rCtx, rCtxCancel := context.WithTimeout(f.ctx, 5*time.Second)
	containers := <-f.dockerApi.ListContainers(rCtx)
	rCtxCancel()
	if containers.Error != nil {
		return containers.Error
	}
	// Create connection context
	ctx, cancel := context.WithCancel(f.ctx)
	defer cancel()
	// Create Websocket Config
	ws, _, err := websocket.DefaultDialer.DialContext(ctx, f.fireOpsApi.String(), http.Header{"Api-Key": []string{f.fireOpsApiKey}})
	if err != nil {
		return appError.NewErrFireOpsApi("failed to create websocket - %v", err)
	}
	f.logger.Infof("Successfully connected to Websocket interface %s -> If you are currently installing fireops-edge for the first time, please follow the instructions on the website", f.fireOpsApi.String())
	defer func() {
		// Cancel context
		cancel()
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
		go func() {
			// Copy raw data into request
			req := raw
			f.logger.Debugf("New incomming %s request with id %s: %v", req.MsgType, req.MsgId, string(req.Body))
			switch req.MsgType {
			case MESSAGE_TYPE_GET_CONTAINERS:
				if err := routeMsg(ctx, ws, f.sendMessage, req, f.handleGetDeploymentRequest); err != nil {
					f.logger.Errorf("Request with id %s failed - %v", req.MsgId, err.Error())
				}
			case MESSAGE_TYPE_INSTALL:
				if err := routeMsg(ctx, ws, f.sendMessage, req, f.handleInstallRequest); err != nil {
					f.logger.Errorf("Request with id %s failed - %v", req.MsgId, err.Error())
				}
			case MESSAGE_TYPE_DESTROY:
				if err := routeMsg(ctx, ws, f.sendMessage, req, f.handleDestroyRequest); err != nil {
					f.logger.Errorf("Request with id %s failed - %v", req.MsgId, err.Error())
				}
			case MESSAGE_TYPE_GET_CONTAINER_LOGS:
				if err := routeMsg(ctx, ws, f.sendMessage, req, f.handleGetLogsRequest); err != nil {
					f.logger.Errorf("Request with id %s failed - %v", req.MsgId, err.Error())
				}
			case MESSAGE_TYPE_GET_AGENT_VERSION:
				if err := routeMsg(ctx, ws, f.sendMessage, req, f.handleGetAgentVersionRequest); err != nil {
					f.logger.Errorf("Request with id %s failed - %v", req.MsgId, err.Error())
				}
			default:
				if err := f.sendMessage(ws, wsResponse[any]{MsgId: req.MsgId, MsgType: req.MsgType, Error: appError.NewErrUnsupportedMsgType("type %s is not supported", req.MsgType), Body: nil}); err != nil {
					f.logger.Errorf("Request with id %s failed - %v", req.MsgId, err.Error())
				}
			}
			f.logger.Debugf("Finished request with id %s", req.MsgId)
		}()
	}
}

func (f *FireOpsOperator) sendMessage(ws *websocket.Conn, msg any) error {
	f.wsMux.Lock()
	defer f.wsMux.Unlock()
	return ws.WriteJSON(msg)
}

func routeMsg[I, O any](ctx context.Context, ws *websocket.Conn, sendFunc func(*websocket.Conn, any) error, msg wsRequest[json.RawMessage], handler func(context.Context, wsRequest[I]) wsResponse[O]) error {
	body := *new(I)
	err := json.Unmarshal(msg.Body, &body)
	if err != nil {
		if err := sendFunc(ws, wsResponse[O]{MsgId: msg.MsgId, MsgType: msg.MsgType, Error: err, Body: *new(O)}); err != nil {
			return err
		}
	} else {
		if err := sendFunc(ws, handler(ctx, wsRequest[I]{MsgId: msg.MsgId, MsgType: msg.MsgType, Body: body})); err != nil {
			return err
		}
	}
	return nil
}

func (f *FireOpsOperator) cleanRunningConfig(ctx context.Context) error {
	f.logger.Debugf("Listing all containers...")
	containers := <-f.dockerApi.ListContainers(ctx)
	if containers.Error != nil {
		return containers.Error
	}
	f.logger.Debugf("List of all containers runnning in context: %v", containers.Result)
	// Remove all containers
	for _, container := range containers.Result {
		f.logger.Debugf("Removing container %v...", container.Names)
		remove := <-f.dockerApi.RemoveContainer(ctx, container.ID)
		if remove.Error != nil {
			return remove.Error
		}
		f.logger.Debugf("Container %v removed", container.Names)
	}
	// Prune networks
	f.logger.Debugf("Prune networks...")
	netPrune := <-f.dockerApi.PruneNetworks(ctx)
	if netPrune.Error != nil {
		return netPrune.Error
	}
	f.logger.Debugf("Networks pruned %v", netPrune.Result)
	// Return success
	return nil
}

func (f *FireOpsOperator) cleanDocker(ctx context.Context) error {
	// Clean fireops stuff
	if err := f.cleanRunningConfig(ctx); err != nil {
		return err
	}
	// Prune images
	f.logger.Debugf("Prune images...")
	imagePrune := <-f.dockerApi.PruneImages(ctx, true)
	if imagePrune.Error != nil {
		return imagePrune.Error
	}
	f.logger.Debugf("Images pruned %v", imagePrune.Result)
	// Return success
	return nil
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------
func (f *FireOpsOperator) handleGetDeploymentRequest(ctx context.Context, msg wsRequest[getDeploymentRequest]) wsResponse[getDeploymentResponse] {
	// Get Running containers from docker api
	f.logger.Debugf("Listing all containers...")
	containers := <-f.dockerApi.ListContainers(ctx)
	if containers.Error != nil {
		f.logger.Error(containers.Error.Error())
	}
	f.logger.Debugf("List of all containers runnning in context: %v", containers.Result)
	// Return current state
	return wsResponse[getDeploymentResponse]{
		MsgId:   msg.MsgId,
		MsgType: msg.MsgType,
		Error:   containers.Error,
		Body:    containers.Result,
	}
}

func (f *FireOpsOperator) handleInstallRequest(ctx context.Context, msg wsRequest[installRequest]) wsResponse[installResponse] {
	// Clean docker
	if err := f.cleanRunningConfig(ctx); err != nil {
		f.logger.Error(err.Error())
		return wsResponse[installResponse]{
			MsgId:   msg.MsgId,
			MsgType: msg.MsgType,
			Error:   err,
		}
	}
	// Create Network
	f.logger.Debugf("Creating docker network %s...", f.fireopsNetwork)
	dockerNet := <-f.dockerApi.CreateNetwork(ctx, f.fireopsNetwork, network.NetworkBridge)
	if dockerNet.Error != nil {
		f.logger.Error(dockerNet.Error.Error())
		return wsResponse[installResponse]{
			MsgId:   msg.MsgId,
			MsgType: msg.MsgType,
			Error:   dockerNet.Error,
		}
	}
	f.logger.Debugf("Nework %s created with id %s", f.fireopsNetwork, dockerNet.Result.ID)
	// Install services
	for _, service := range msg.Body {
		env := []string{}
		for k, v := range service.Environment {
			env = append(env, fmt.Sprintf("%s=%s", k, v))
		}
		// Create container
		f.logger.Debugf("Creating docker container %s...", service.ServiceName)
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
			f.logger.Error(create.Error.Error())
			return wsResponse[installResponse]{
				MsgId:   msg.MsgId,
				MsgType: msg.MsgType,
				Error:   create.Error,
			}
		}
		f.logger.Debugf("Container %s created with id %s", service.ServiceName, create.Result.ID)
		// Start container
		f.logger.Debugf("Starting docker container: %s...", service.ServiceName)
		start := <-f.dockerApi.StartContainer(ctx, create.Result.ID)
		if start.Error != nil {
			f.logger.Error(start.Error.Error())
			return wsResponse[installResponse]{
				MsgId:   msg.MsgId,
				MsgType: msg.MsgType,
				Error:   start.Error,
			}
		}
		f.logger.Debugf("Container %s started", service.ServiceName)
	}
	// Get containers
	f.logger.Debugf("List running containers...")
	containers := <-f.dockerApi.ListContainers(ctx)
	if containers.Error != nil {
		f.logger.Error(containers.Error.Error())
		return wsResponse[installResponse]{
			MsgId:   msg.MsgId,
			MsgType: msg.MsgType,
			Error:   containers.Error,
		}
	}
	f.logger.Debugf("Running containers: %v", containers.Result)
	// Return result
	return wsResponse[installResponse]{
		MsgId:   msg.MsgId,
		MsgType: msg.MsgType,
		Error:   nil,
		Body:    containers.Result,
	}
}

func (f *FireOpsOperator) handleDestroyRequest(ctx context.Context, msg wsRequest[destroyRequest]) wsResponse[destroyResponse] {
	err := f.cleanDocker(ctx)
	if err != nil {
		f.logger.Error(err.Error())
	}
	return wsResponse[destroyResponse]{
		MsgId:   msg.MsgId,
		MsgType: msg.MsgType,
		Error:   err,
	}
}

func (f *FireOpsOperator) handleGetLogsRequest(ctx context.Context, msg wsRequest[getLogsRequest]) wsResponse[getLogsResponse] {
	// Get logs of container
	logEntries := <-f.dockerApi.GetContainerLogs(ctx, msg.Body.ContainerId, msg.Body.Len)
	if logEntries.Error != nil {
		f.logger.Error(logEntries.Error.Error())
	}
	return wsResponse[getLogsResponse]{
		MsgId:   msg.MsgId,
		MsgType: msg.MsgType,
		Error:   logEntries.Error,
		Body:    logEntries.Result,
	}
}

func (f *FireOpsOperator) handleGetAgentVersionRequest(ctx context.Context, msg wsRequest[getAgentVersionRequest]) wsResponse[getAgentVersionResponse] {
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
func NewFireOpsOperator(ctx context.Context, logger log.ILogger, dockerApi api.IDockerApi, fireOpsApi url.URL, fireOpsApiKey string, opts ...func(*FireOpsOperator)) *FireOpsOperator {
	f := &FireOpsOperator{
		ctx:           ctx,
		logger:        logger,
		dockerApi:     dockerApi,
		fireOpsApi:    fireOpsApi,
		fireOpsApiKey: fireOpsApiKey,

		fireopsNetwork: "fireops",
		retryInterval:  30 * time.Second,
		maxReqDuration: time.Hour,
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
