package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"

	cclog "github.com/ClusterCockpit/cc-metric-collector/pkg/ccLogger"
	lp "github.com/ClusterCockpit/cc-metric-collector/pkg/ccMetric"
)

var AvailableStorageBackends map[string]Storage = map[string]Storage{
	"sqlite":   new(sqliteStorage),
	"postgres": new(postgresStorage),
	"influx":   new(influxStorage),
}

type QueryRequestType int

const (
	QueryTypeEvent QueryRequestType = 0
	QueryTypeLog   QueryRequestType = 1
)

type storageManager struct {
	store Storage
}

type QueryCondition struct {
	Pred      string
	Operation string
	Args      []interface{}
}

type QueryRequest struct {
	Event      string
	From       int64
	To         int64
	Hostname   string
	Cluster    string
	QueryType  QueryRequestType
	Conditions []QueryCondition
}

type QueryResultEvent struct {
	Timestamp int64
	Event     string
}

type QueryResult struct {
	Results []QueryResultEvent
	Error   error
}

type StorageManager interface {
	Close()
	Start()
	Query(cluster, event, hostname string, from, to int64, conditions []QueryCondition) (QueryResult, error)
	Write(msg lp.CCMetric)
	SetInput(input chan lp.CCMetric)
}

func (sm *storageManager) Close() {
	if sm.store != nil {
		sm.store.Close()
	}
}

func (sm *storageManager) Start() {
	if sm.store != nil {
		sm.store.Start()
	}
}

func (sm *storageManager) Query(cluster, event, hostname string, from, to int64, conditions []QueryCondition) (QueryResult, error) {
	if sm.store != nil {
		request := QueryRequest{
			Cluster:    cluster,
			Event:      event,
			Hostname:   hostname,
			To:         to,
			From:       from,
			Conditions: conditions,
		}

		return sm.store.Query(request)
	}
	err := fmt.Errorf("no storage backend active, cannot query")
	return QueryResult{Results: make([]QueryResultEvent, 0), Error: err}, err
}

func (sm *storageManager) SetInput(input chan lp.CCMetric) {
	if sm.store != nil {
		sm.store.SetInput(input)
	}
}

func (sm *storageManager) Write(msg lp.CCMetric) {
	if sm.store != nil {
		sm.store.Write(msg)
	}
}

func NewStorageManager(wg *sync.WaitGroup, storageConfigFile string) (StorageManager, error) {
	sm := new(storageManager)

	configFile, err := os.Open(storageConfigFile)
	if err != nil {
		cclog.Error(err.Error())
		return nil, err
	}
	defer configFile.Close()

	var config storageConfig
	jsonParser := json.NewDecoder(configFile)
	err = jsonParser.Decode(&config)
	if err != nil {
		cclog.Error(err.Error())
		return nil, err
	}

	if _, ok := AvailableStorageBackends[config.Type]; !ok {
		err = fmt.Errorf("unknown storage type %s", config.Type)
		return nil, err
	}

	s := AvailableStorageBackends[config.Type]

	err = s.Init(wg, storageConfigFile)
	if err != nil {
		cclog.Error(err.Error())
		return nil, err
	}
	sm.store = s

	return sm, nil
}
