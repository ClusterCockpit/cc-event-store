package main

import (
	"encoding/json"
	"flag"
	"os"
	"os/signal"
	"sync"
	"syscall"

	lp2 "github.com/ClusterCockpit/cc-energy-manager/pkg/cc-message"
	"github.com/ClusterCockpit/cc-event-store/internal/api"
	storage "github.com/ClusterCockpit/cc-event-store/internal/storage"
	cclog "github.com/ClusterCockpit/cc-metric-collector/pkg/ccLogger"
	lp "github.com/ClusterCockpit/cc-metric-collector/pkg/ccMetric"

	"github.com/ClusterCockpit/cc-metric-collector/receivers"
)

type RuntimeConfig struct {
	storageEngine  storage.StorageManager
	receiveManager receivers.ReceiveManager
	router         Router
	api            api.API
	wg             sync.WaitGroup
}

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

func ReadCli() map[string]string {
	var m map[string]string
	cfg := flag.String("config", "./config.json", "Path to configuration file")
	logfile := flag.String("log", "stderr", "Path for logfile")
	debug := flag.Bool("debug", false, "Activate debug output")
	flag.Parse()
	m = make(map[string]string)
	if cfg != nil {
		m["configfile"] = *cfg
	}
	m["logfile"] = *logfile
	if *debug {
		m["debug"] = "true"
		cclog.SetDebug()
	} else {
		m["debug"] = "false"
	}
	return m
}

// General shutdownHandler function that gets executed in case of interrupt or graceful shutdownHandler
func shutdownHandler(config *RuntimeConfig, shutdownSignal chan os.Signal) {
	defer config.wg.Done()

	<-shutdownSignal
	// Remove shutdown handler
	// every additional interrupt signal will stop without cleaning up
	signal.Stop(shutdownSignal)

	cclog.Info("Shutdown...")

	if config.receiveManager != nil {
		cclog.Debug("Shutdown ReceiveManager...")
		config.receiveManager.Close()
	}
	if config.router != nil {
		cclog.Debug("Shutdown Router...")
		config.router.Close()
	}
	if config.storageEngine != nil {
		cclog.Debug("Shutdown StorageManager...")
		config.storageEngine.Close()
	}
	if config.api != nil {
		cclog.Debug("Shutdown REST API...")
		config.api.Close()
	}

}

func mainFunc() int {

	var config CentralConfig
	cliopts := ReadCli()

	LoadCentralConfiguration(cliopts["configfile"], &config)

	var rcfg RuntimeConfig
	ReceiveManager, err := receivers.New(&rcfg.wg, config.ReceiverConfigFile)
	if err != nil {
		cclog.Error(err.Error())
		return -1
	}

	s, err := storage.NewStorageManager(&rcfg.wg, config.StorageConfigFile)
	if err != nil {
		cclog.Error(err.Error())
		return -1
	}
	rcfg.storageEngine = s

	r, err := NewRouter(&rcfg.wg)
	if err != nil {
		cclog.Error(err.Error())
		return -1
	}
	rcfg.router = r

	a, err := api.NewAPI(&rcfg.wg, rcfg.storageEngine, config.ApiConfigFile)
	if err != nil {
		cclog.Error(err.Error())
		return -1
	}
	rcfg.api = a

	// Connect receive manager to metric router
	ReceiveToRouterChannel := make(chan lp.CCMetric, 200)
	RouterToStorageChannel := make(chan *lp2.CCMessage, 200)
	ReceiveManager.AddOutput(ReceiveToRouterChannel)
	rcfg.receiveManager = ReceiveManager
	rcfg.router.SetInput(ReceiveToRouterChannel)
	rcfg.router.SetOutput(RouterToStorageChannel)
	rcfg.storageEngine.SetInput(RouterToStorageChannel)

	// Create shutdown handler
	shutdownSignal := make(chan os.Signal, 1)
	signal.Notify(shutdownSignal, os.Interrupt)
	signal.Notify(shutdownSignal, syscall.SIGTERM)
	rcfg.wg.Add(1)
	go shutdownHandler(&rcfg, shutdownSignal)

	rcfg.storageEngine.Start()
	rcfg.router.Start()
	rcfg.receiveManager.Start()

	a.Start()

	// Wait that all goroutines finish
	rcfg.wg.Wait()

	return 0
}

func main() {
	exitCode := mainFunc()
	os.Exit(exitCode)
}
