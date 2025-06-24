package services

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"sync"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/network"
	"github.com/uoul/go-common/collections"
	"github.com/uoul/go-common/log"
	"golang.org/x/net/websocket"

	"github.com/fireops-software/fireops-edge-agent/api"
	appError "github.com/fireops-software/fireops-edge-agent/error"
)

const (
	WS_BUFFER_SIZE = 1024

	MESSAGE_TYPE_GET     = msgType("Get")
	MESSAGE_TYPE_INSTALL = msgType("Install")
	MESSAGE_TYPE_DESTROY = msgType("Destroy")
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

type destroyRequest []struct{}
type destroyResponse []struct{}

// ---------------------------------------------------------------------------
// Private
// ---------------------------------------------------------------------------

func (f *FireOpsOperator) run() error {
	// Create Websocket Config
	config, err := websocket.NewConfig(f.fireOpsApi.String(), originOfWsUrl(f.fireOpsApi))
	if err != nil {
		return appError.NewErrFireOpsApi("failed to create websocket config - %v", err)
	}
	// Set Headers
	if len(f.fireOpsToken) > 0 {
		config.Header.Add("Authorization", fmt.Sprintf("Bearer %s", f.fireOpsToken))
	}
	// Dial Websocket
	ws, err := websocket.DialConfig(config)
	if err != nil {
		return appError.NewErrFireOpsApi("failed to create websocket - %v", err)
	}
	defer ws.Close()
	// Handle incomming messages
	for {
		// Read incomming message
		data, err := f.readMessage(ws)
		if err != nil {
			return err
		}
		// Deserialize incomming message
		raw := wsRequest[any]{}
		if err := json.Unmarshal(data, &raw); err != nil {
			return err
		}
		// Route message based on message type
		switch raw.MsgType {
		case MESSAGE_TYPE_GET:
			err := routeMsg(ws, f.sendMessage, raw, f.handleGetDeploymentRequest)
			if err != nil {
				return err
			}
		case MESSAGE_TYPE_INSTALL:
			err := routeMsg(ws, f.sendMessage, raw, f.handleInstallRequest)
			if err != nil {
				return err
			}
		case MESSAGE_TYPE_DESTROY:
			err := routeMsg(ws, f.sendMessage, raw, f.handleDestroyRequest)
			if err != nil {
				return err
			}
		default:
			if err := f.sendMessage(ws, wsResponse[any]{MsgId: raw.MsgId, MsgType: raw.MsgType, Error: appError.NewErrUnsupportedMsgType("type %s is not supported", raw.MsgType), Body: nil}); err != nil {
				return err
			}
		}
	}
}

func (f *FireOpsOperator) readMessage(ws net.Conn) ([]byte, error) {
	buffer := make([]byte, WS_BUFFER_SIZE)
	n := WS_BUFFER_SIZE
	data := []byte{}
	for n >= WS_BUFFER_SIZE {
		n, err := ws.Read(buffer)
		if err != nil {
			return nil, err
		}
		data = append(data, buffer[:n]...)
	}
	return data, nil
}

func (f *FireOpsOperator) sendMessage(ws net.Conn, msg any) error {
	f.wsMux.Lock()
	defer f.wsMux.Unlock()
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	_, err = ws.Write(data)
	return err
}

func routeMsg[I, O any](ws net.Conn, sendFunc func(net.Conn, any) error, msg wsRequest[any], handler func(wsRequest[I]) wsResponse[O]) error {
	req, err := convertMsg[I](&msg)
	if err != nil {
		if err := sendFunc(ws, wsResponse[O]{MsgId: msg.MsgId, MsgType: msg.MsgType, Error: err, Body: *new(O)}); err != nil {
			return err
		}
	} else {
		if err := sendFunc(ws, handler(*req)); err != nil {
			return err
		}
	}
	return nil
}

func convertMsg[T any](msg *wsRequest[any]) (*wsRequest[T], error) {
	b, ok := msg.Body.(T)
	if !ok {
		return nil, appError.NewErrUnsupportedMsgType("body does not match message of type %s", msg.MsgType)
	}
	return &wsRequest[T]{
		MsgId:   msg.MsgId,
		MsgType: msg.MsgType,
		Body:    b,
	}, nil
}

func (f *FireOpsOperator) cleanDocker(ctx context.Context) error {
	containers := <-f.dockerApi.ListContainers(ctx)
	if containers.Error != nil {
		return containers.Error
	}
	// Remove all containers
	for _, container := range containers.Result {
		remove := <-f.dockerApi.RemoveContainer(ctx, container.ID)
		if remove.Error != nil {
			return remove.Error
		}
	}
	// Prune images
	imagePrune := <-f.dockerApi.PruneImages(ctx, true)
	if imagePrune.Error != nil {
		return imagePrune.Error
	}
	// Prune networks
	netPrune := <-f.dockerApi.PruneNetworks(ctx)
	if netPrune.Error != nil {
		return netPrune.Error
	}
	// Return success
	return nil
}

func originOfWsUrl(wsUrl url.URL) string {
	scheme := "http"
	if wsUrl.Scheme == "wss" {
		scheme = "https"
	}
	return fmt.Sprintf("%s://%s", scheme, wsUrl.Host)
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------
func (f *FireOpsOperator) handleGetDeploymentRequest(msg wsRequest[getDeploymentRequest]) wsResponse[getDeploymentResponse] {
	// Create Context with timeout for Docker api call
	ctx, cancel := context.WithTimeout(f.ctx, 10*time.Second)
	defer cancel()
	// Get Running containers from docker api
	containers := <-f.dockerApi.ListContainers(ctx)
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
	dockerNet := <-f.dockerApi.CreateNetwork(ctx, f.fireopsNetwork, network.NetworkBridge)
	if dockerNet.Error != nil {
		return wsResponse[installResponse]{
			MsgId:   msg.MsgId,
			MsgType: msg.MsgType,
			Error:   dockerNet.Error,
		}
	}
	// Install services
	for _, service := range msg.Body {
		env := []string{}
		for k, v := range service.Environment {
			env = append(env, fmt.Sprintf("%s=%s", k, v))
		}
		// Create container
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
		// Start container
		start := <-f.dockerApi.StartContainer(ctx, create.Result.ID)
		if start.Error != nil {
			return wsResponse[installResponse]{
				MsgId:   msg.MsgId,
				MsgType: msg.MsgType,
				Error:   start.Error,
			}
		}
	}
	// Get containers
	containers := <-f.dockerApi.ListContainers(ctx)
	if containers.Error != nil {
		return wsResponse[installResponse]{
			MsgId:   msg.MsgId,
			MsgType: msg.MsgType,
			Error:   containers.Error,
		}
	}
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

// ---------------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------------

func WithFireOpsOperatorNetwork(network string) func(*FireOpsOperator) {
	return func(foo *FireOpsOperator) {
		foo.fireopsNetwork = network
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
