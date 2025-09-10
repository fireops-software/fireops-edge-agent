package main

import (
	"context"
	"flag"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"syscall"

	"github.com/fireops-software/fireops-edge-agent/api"
	"github.com/fireops-software/fireops-edge-agent/services"
	"github.com/uoul/go-common/log"
)

const (
	VERSION = "{VERSION}"
)

func main() {
	// Flags
	logLvl := flag.String("logLvl", "INFO", "OFF,FATAL,ERROR,WARNING,INFO,DEBUG,TRACE")
	namespace := flag.String("namespace", "fireops", "namespace(label) for docker resources")
	fireOpsApiStr := flag.String("api", "", "Api endpoint for fireops websocket connection (required)")
	fireOpsToken := flag.String("token", "", "Api token for fireops api (required)")
	flag.Parse()

	// Check flags
	if len(*fireOpsApiStr) <= 0 {
		panic(fmt.Sprintf("Required flag \"-api\" not provided (%s --help)", os.Args[0]))
	}
	if len(*fireOpsToken) <= 0 {
		panic(fmt.Sprintf("Required flag \"-token\" not provided (%s --help)", os.Args[0]))
	}
	fireOpsApi, err := url.Parse(*fireOpsApiStr)
	if err != nil {
		panic(fmt.Sprintf("Api must be a valid url - %v", err))
	}

	// Create AppCtx
	appCtx, cancelAppCtx := context.WithCancel(context.Background())

	// Create logger
	logger := log.NewConsoleLogger(
		log.StringToLogLevel(
			*logLvl,
			log.INFO,
		),
	)

	// Create DockerApi
	docker, err := api.NewDockerApi(*namespace)
	if err != nil {
		logger.Fatalf("failed to create docker api - %v", err)
		return
	}
	defer docker.Close()

	// Create FireOpsOperator
	services.NewFireOpsOperator(
		appCtx,
		logger,
		docker,
		*fireOpsApi,
		*fireOpsToken,
		services.WithFireOpsOperatorVersion(VERSION),
		services.WithFireOpsOperatorNetwork(*namespace),
	)

	// Wait until stop
	osSig := make(chan os.Signal, 1)
	signal.Notify(osSig, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	<-osSig
	cancelAppCtx()
	logger.Info("Shutting down...")
}
