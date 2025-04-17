// Copyright (C) NHR@FAU, University Erlangen-Nuremberg.
// All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.
package main

import (
	"encoding/json"
	"flag"
	"os"
	"os/signal"
	"sync"
	"syscall"

	api "github.com/ClusterCockpit/cc-event-store/internal/api"
	storage "github.com/ClusterCockpit/cc-event-store/internal/storage"
	cfg "github.com/ClusterCockpit/cc-lib/ccConfig"
	cclog "github.com/ClusterCockpit/cc-lib/ccLogger"
	lp "github.com/ClusterCockpit/cc-lib/ccMessage"

	"github.com/ClusterCockpit/cc-lib/receivers"
)

var (
	storageEngine                storage.StorageManager
	receiveManager               receivers.ReceiveManager
	ShutdownWG                   sync.WaitGroup
	myRouter                     Router
	myApi                        api.API
	flagVersion, flagLogDateTime bool
	flagConfigFile, flagLogLevel string
)

type CentralConfig struct {
	ReceiverConfigFile string `json:"receivers"`
	StorageConfigFile  string `json:"storage"`
	ApiConfigFile      string `json:"api"`
}

func LoadCentralConfiguration(file string, config *CentralConfig) error {
	configFile, err := os.Open(file)
	if err != nil {
		cclog.Error(err.Error())
		return err
	}
	defer configFile.Close()
	jsonParser := json.NewDecoder(configFile)
	err = jsonParser.Decode(config)
	return err
}

func ReadCli() {
	flag.StringVar(&flagConfigFile, "config", "./config.json", "Path to configuration file")
	flag.StringVar(&flagLogLevel, "loglevel", "warn", "Sets the logging level: `[debug,info,warn (default),err,fatal,crit]`")
	flag.BoolVar(&flagLogDateTime, "logdate", false, "Set this flag to add date and time to log messages")
	flag.Parse()
}

// General shutdownHandler function that gets executed in case of interrupt or graceful shutdownHandler
func shutdownHandler(shutdownSignal chan os.Signal) {
	defer ShutdownWG.Done()

	<-shutdownSignal
	// Remove shutdown handler
	// every additional interrupt signal will stop without cleaning up
	signal.Stop(shutdownSignal)

	cclog.Info("Shutdown...")

	if receiveManager != nil {
		cclog.Debug("Shutdown ReceiveManager...")
		receiveManager.Close()
	}
	if myRouter != nil {
		cclog.Debug("Shutdown Router...")
		myRouter.Close()
	}
	if storageEngine != nil {
		cclog.Debug("Shutdown StorageManager...")
		storageEngine.Close()
	}
	if myApi != nil {
		cclog.Debug("Shutdown REST API...")
		myApi.Close()
	}
}

func mainFunc() int {
	var err error

	ReadCli()
	cclog.Init(flagLogLevel, flagLogDateTime)
	cfg.Init(flagConfigFile)

	if cfg := cfg.GetPackageConfig("receivers"); cfg != nil {
		receiveManager, err = receivers.New(&ShutdownWG, cfg)
		if err != nil {
			cclog.Error(err.Error())
			return -1
		}
	} else {
		cclog.Error("Receiver configuration must be present")
		return -1
	}

	if cfg := cfg.GetPackageConfig("storage"); cfg != nil {
		storageEngine, err = storage.NewStorageManager(&ShutdownWG, cfg)
		if err != nil {
			cclog.Error(err.Error())
			return -1
		}
	} else {
		cclog.Error("Storage configuration must be present")
		return -1
	}

	myRouter, err := NewRouter(&ShutdownWG)
	if err != nil {
		cclog.Error(err.Error())
		return -1
	}

	if cfg := cfg.GetPackageConfig("api"); cfg != nil {
		myApi, err = api.NewAPI(&ShutdownWG, storageEngine, cfg)
		if err != nil {
			cclog.Error(err.Error())
			return -1
		}
	} else {
		cclog.Error("Api configuration must be present")
		return -1
	}

	// Connect receive manager to metric router
	ReceiveToRouterChannel := make(chan lp.CCMessage, 200)
	RouterToStorageChannel := make(chan lp.CCMessage, 200)
	receiveManager.AddOutput(ReceiveToRouterChannel)
	myRouter.SetInput(ReceiveToRouterChannel)
	myRouter.SetOutput(RouterToStorageChannel)
	storageEngine.SetInput(RouterToStorageChannel)

	// Create shutdown handler
	shutdownSignal := make(chan os.Signal, 1)
	signal.Notify(shutdownSignal, os.Interrupt)
	signal.Notify(shutdownSignal, syscall.SIGTERM)
	ShutdownWG.Add(1)
	go shutdownHandler(shutdownSignal)

	storageEngine.Start()
	myRouter.Start()
	receiveManager.Start()

	myApi.Start()

	// Wait that all goroutines finish
	ShutdownWG.Wait()

	return 0
}

func main() {
	exitCode := mainFunc()
	os.Exit(exitCode)
}
