package storage

import (
	"fmt"
	"sync"

	cclog "github.com/ClusterCockpit/cc-metric-collector/pkg/ccLogger"
	lp "github.com/ClusterCockpit/cc-metric-collector/pkg/ccMetric"
)

type storageManagerEntry struct {
	store SqliteStorage
}

type storageManger struct {
	basefolder string
	clusters   map[string]storageManagerEntry
	done       chan bool
	input      chan lp.CCMetric
	wg         *sync.WaitGroup
}

type StorageManager interface {
	Start() error
	Close()
	SetWriteInput(input chan lp.CCMetric)
	Submit(cluster string, event lp.CCMetric) error
	Query(cluster, event, hostname string, from, to int64, conditions []string) ([]QueryResultEvent, error)
	Delete(cluster string, to int64) error
}

func (sm *storageManger) Start() error {
	cclog.ComponentDebug("StorageManager", "START")
	sm.wg.Add(1)
	go func() {
		for {
			select {
			case <-sm.done:
				sm.wg.Done()
				cclog.ComponentDebug("StorageManager", "DONE")
				return
			case e := <-sm.input:
				cclog.ComponentDebug("StorageManager", "Input")
				if e.HasTag("cluster") {
					if c, ok := e.GetTag("cluster"); ok {
						sm.Submit(c, e)
					}
				}
			}
		}
	}()
	cclog.ComponentDebug("StorageManager", "STARTED")
	return nil
}

func (sm *storageManger) Close() {
	for _, sme := range sm.clusters {
		sme.store.Close()
	}
	cclog.ComponentDebug("StorageManager", "CLOSE")
	sm.done <- true
}

func (sm *storageManger) SetWriteInput(input chan lp.CCMetric) {
	sm.input = input
}

func (sm *storageManger) Submit(cluster string, event lp.CCMetric) error {
	if sme, ok := sm.clusters[cluster]; !ok {
		cclog.ComponentDebug("StorageManager", "New storage for", cluster)
		s, err := NewStorage(sm.wg, sm.basefolder, cluster)
		if err == nil {
			sme.store = s
			sme.store.Start()
			sm.clusters[cluster] = sme
		} else {
			return fmt.Errorf("failed to create storage for cluster %s", cluster)
		}
	}
	if sme, ok := sm.clusters[cluster]; ok {
		cclog.ComponentDebug("StorageManager", "SUBMIT", cluster)
		sme.store.Submit(event)
	}
	return nil
}

func (sm *storageManger) Query(cluster, event, hostname string, from, to int64, conditions []string) ([]QueryResultEvent, error) {
	if sme, ok := sm.clusters[cluster]; ok {
		cclog.ComponentDebug("StorageManager", "Query", cluster)
		return sme.store.Query(QueryRequest{
			Event:      event,
			From:       from,
			To:         to,
			Hostname:   hostname,
			Conditions: conditions,
		})
	}
	return nil, fmt.Errorf("query request for unknown cluster %s", cluster)
}

func (sm *storageManger) Delete(cluster string, to int64) error {
	if sme, ok := sm.clusters[cluster]; ok {
		cclog.ComponentDebug("StorageManager", "Query", cluster)
		return sme.store.Delete(to)
	}
	return fmt.Errorf("delete request for unknown cluster %s", cluster)
}

func NewStorageManager(wg *sync.WaitGroup, configFile string) (StorageManager, error) {
	sm := new(storageManger)
	sm.wg = wg
	sm.done = make(chan bool)
	sm.basefolder = "./archive"
	sm.clusters = make(map[string]storageManagerEntry)
	return sm, nil
}
