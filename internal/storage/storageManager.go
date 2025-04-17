// Copyright (C) NHR@FAU, University Erlangen-Nuremberg.
// All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.
package storage

import (
	"encoding/json"
	"fmt"
	"maps"
	"sync"
	"time"

	cclog "github.com/ClusterCockpit/cc-lib/ccLogger"
	lp "github.com/ClusterCockpit/cc-lib/ccMessage"
	"github.com/go-co-op/gocron/v2"
)

var AvailableStorageBackends map[string]Storage = map[string]Storage{
	"sqlite":   new(sqliteStorage),
	"postgres": new(postgresStorage),
	"stdout":   new(stdoutStorage),
}

type QueryRequestType int

const (
	QueryTypeEvent QueryRequestType = 0
	QueryTypeLog   QueryRequestType = 1
)

type storageManagerConfig struct {
	Retention  string          `json:"retention_time"`
	FlushTime  string          `json:"flush_time"`
	Backend    json.RawMessage `json:"backend"`
	BatchSize  int             `json:"batch_size,omitempty"`
	MaxProcess int             `json:"max_process,omitempty"`
	StoreLogs  bool            `json:"store_logs,omitempty"`
}

type StorageManagerStats struct {
	Manager storageStats      `json:"manager"`
	Backend storageStats      `json:"backend"`
	Info    map[string]string `json:"info"`
}

type storageManager struct {
	storeStats    storageStats
	stats         storageStats
	scheduler     gocron.Scheduler
	store         Storage
	buffer        StorageBuffer
	input         chan lp.CCMessage
	flushTimer    *time.Timer
	done          chan struct{}
	wg            *sync.WaitGroup
	info          map[string]string
	config        storageManagerConfig
	flushTime     time.Duration
	retentionTime time.Duration
	timerLock     sync.Mutex
	started       bool
}

type QueryCondition struct {
	Pred      string
	Operation string
	Args      []any
}

type QueryRequest struct {
	Event      string
	Hostname   string
	Cluster    string
	Conditions []QueryCondition
	From       int64
	To         int64
	QueryType  QueryRequestType
}

type QueryResultEvent struct {
	Event     string
	Timestamp int64
}

type QueryResult struct {
	Error   error
	Results []QueryResultEvent
}

type StorageManager interface {
	Close()
	Start()
	Query(request QueryRequest) (QueryResult, error)
	SetInput(input chan lp.CCMessage)
	GetInput() chan lp.CCMessage
	Stats() StorageManagerStats
}

func (sm *storageManager) Close() {
	if sm.started {
		sm.done <- struct{}{}
		<-sm.done
	}
	if sm.store != nil {
		// Stop existing timer and immediately flush
		cclog.ComponentDebug("StorageManager", "Stopping store ", sm.store.GetName())
		if sm.flushTimer != nil {
			cclog.ComponentDebug("StorageManager", "Stopping store timer")
			if ok := sm.flushTimer.Stop(); ok {

				sm.flushTimer = nil
				cclog.ComponentDebug("StorageManager", "Unlocking store timer lock")
				sm.timerLock.Unlock()
			}
		}
		cclog.ComponentDebug("StorageManager", "Flushing store ", sm.store.GetName())
		sm.Flush()
		cclog.ComponentDebug("StorageManager", "Closing store ", sm.store.GetName())
		sm.store.Close()
	}
	if sm.started {
		if sm.scheduler != nil {
			sm.scheduler.Shutdown()
			sm.scheduler = nil
		}
	}
	sm.stats.Close()
	sm.started = false
}

func (sm *storageManager) Flush() {
	if sm.store != nil {
		sm.buffer.Lock()
		if sm.buffer.Len() > 0 {

			sm.store.Write(sm.buffer.Get())
			sm.stats.UpdateStats("flushed", int64(sm.buffer.Len()))
			sm.buffer.Clear()
			if len(sm.input) > 0 {
				for range len(sm.input) {
					sm.buffer.Add(<-sm.input)
				}
				if sm.buffer.Len() > 0 {
					sm.store.Write(sm.buffer.Get())
					sm.stats.UpdateStats("flushed", int64(sm.buffer.Len()))
					sm.buffer.Clear()
				}
			}
		}
		sm.buffer.Unlock()
		if sm.flushTimer != nil {
			if ok := sm.flushTimer.Stop(); ok {
				sm.flushTimer = nil
				sm.timerLock.Unlock()
			}
		}
	}
}

func (sm *storageManager) Start() {
	sched, err := gocron.NewScheduler()
	if err != nil {
		cclog.ComponentError("StorageManager", "failed to initialize gocron scheduler")
		return
	}
	sm.scheduler = sched

	sm.scheduler.NewJob(
		gocron.DailyJob(
			1,
			gocron.NewAtTimes(
				gocron.NewAtTime(3, 0, 0),
			),
		),
		gocron.NewTask(
			func() {
				before := time.Now().Add(-sm.retentionTime)
				cclog.ComponentDebug("StorageManager", "Delete everything before %d", before.Unix())
				err := sm.store.Delete(before.Unix())
				if err != nil {
					cclog.ComponentError("StorageManager", err.Error())
				}
			},
		),
	)
	sm.wg.Add(1)
	go func() {
		to_buffer_or_write_batch := func(msg lp.CCMessage) {
			if msg.IsEvent() || (msg.IsLog() && sm.config.StoreLogs) {
				if sm.buffer.Len() < sm.config.BatchSize {
					cclog.ComponentDebug("StorageManager", "Append to buffer", msg)
					sm.buffer.Add(msg)
				} else {
					cclog.ComponentDebug("StorageManager", "Write batch of", sm.buffer.Len(), "messages")
					sm.stats.UpdateStats("write_batch", int64(sm.buffer.Len()))
					sm.store.Write(sm.buffer.Get())
					sm.buffer.Clear()

				}
			}
		}

		for {
			select {
			case <-sm.done:
				sm.Flush()
				close(sm.done)
				sm.wg.Done()
				return
			case e := <-sm.input:
				processed := int64(0)
				sm.buffer.Lock()
				to_buffer_or_write_batch(e)
				processed++
				for i := 0; i < sm.config.MaxProcess && len(sm.input) > 0; i++ {
					to_buffer_or_write_batch(<-sm.input)
					processed++
				}
				sm.stats.UpdateStats("processed", processed)
				sm.buffer.Unlock()
				if sm.flushTime == 0 {
					// Directly flush if no flush delay is configured
					sm.Flush()
				} else if sm.timerLock.TryLock() {
					// Setup flush timer when flush delay is configured
					// and no other timer is already running
					if sm.flushTimer != nil {
						// Restarting existing flush timer
						cclog.ComponentDebug("StorageManager", "Restarting flush timer")
						sm.flushTimer.Reset(sm.flushTime)
					} else {
						// Creating and starting flush timer
						cclog.ComponentDebug("StorageManager", "Starting new flush timer")
						sm.flushTimer = time.AfterFunc(
							sm.flushTime,
							func() {
								defer sm.timerLock.Unlock()
								cclog.ComponentDebug("StorageManager", "Starting flush triggered by flush timer")
								sm.Flush()
								sm.stats.UpdateStats("timer_flushes", 1)
							})
					}
				}
			}
		}
	}()
	cclog.ComponentDebug("StorageManager", "STARTED")
	sm.started = true
}

func (sm *storageManager) Query(request QueryRequest) (QueryResult, error) {
	if sm.store != nil {
		sm.stats.UpdateStats("query_flushes", 1)
		sm.Flush()
		sm.stats.UpdateStats("queries", 1)
		return sm.store.Query(request)
	}
	err := fmt.Errorf("no storage backend active, cannot query")
	return QueryResult{Results: make([]QueryResultEvent, 0), Error: err}, err
}

func (sm *storageManager) SetInput(input chan lp.CCMessage) {
	sm.input = input
}

func (sm *storageManager) GetInput() chan lp.CCMessage {
	return sm.input
}

func (sm *storageManager) Stats() StorageManagerStats {
	manager := storageStats{
		Errors: make(map[string]int64),
		Stats:  make(map[string]int64),
	}
	backend := storageStats{
		Errors: make(map[string]int64),
		Stats:  make(map[string]int64),
	}

	sm.stats.lock.Lock()
	maps.Copy(manager.Errors, sm.stats.Errors)
	maps.Copy(manager.Stats, sm.stats.Stats)
	sm.stats.lock.Unlock()

	sm.storeStats.lock.Lock()
	maps.Copy(backend.Errors, sm.storeStats.Errors)
	maps.Copy(backend.Stats, sm.storeStats.Stats)
	sm.storeStats.lock.Unlock()

	return StorageManagerStats{
		Manager: manager,
		Backend: backend,
		Info:    sm.info,
	}
}

func NewStorageManager(wg *sync.WaitGroup, rawConfig json.RawMessage) (StorageManager, error) {
	var err error
	sm := new(storageManager)
	sm.input = nil
	sm.store = nil

	cfg := storageManagerConfig{
		BatchSize:  20,
		MaxProcess: 10,
		Retention:  "24h",
		FlushTime:  "1s",
		StoreLogs:  true,
	}

	if err = json.Unmarshal(rawConfig, &cfg); err != nil {
		cclog.Warn("Error while unmarshaling raw config json")
		return nil, err
	}

	if len(cfg.Retention) > 0 {
		t, err := time.ParseDuration(cfg.Retention)
		if err != nil {
			cclog.ComponentError("StorageManager", err.Error())
			return nil, err
		}
		sm.retentionTime = t
	} else {
		cclog.ComponentError("StorageManager", "Retention time cannot be empty")
		return nil, err
	}
	if len(cfg.Retention) > 0 {
		t, err := time.ParseDuration(cfg.FlushTime)
		if err != nil {
			cclog.ComponentError("StorageManager", err.Error())
			return nil, err
		}
		sm.flushTime = t
	}

	var backendConfig struct {
		Type string `json:"type"`
	}

	err = json.Unmarshal(cfg.Backend, &backendConfig)
	if err != nil {
		cclog.Error(err.Error())
		return nil, err
	}
	sm.config = cfg

	if _, ok := AvailableStorageBackends[backendConfig.Type]; !ok {
		err = fmt.Errorf("unknown storage type %s", backendConfig.Type)
		return nil, err
	}
	cclog.ComponentDebug("StorageManager", "Getting storage for", backendConfig.Type)
	s := AvailableStorageBackends[backendConfig.Type]

	err = sm.storeStats.Init()
	if err != nil {
		cclog.Error(err.Error())
		return nil, err
	}

	err = s.Init(cfg.Backend, &sm.storeStats)
	if err != nil {
		cclog.Error(err.Error())
		return nil, err
	}
	sm.store = s

	b, err := NewStorageBuffer(sm.config.BatchSize)
	if err != nil {
		cclog.Error(err.Error())
		return nil, err
	}

	err = sm.stats.Init()
	if err != nil {
		cclog.Error(err.Error())
		return nil, err
	}

	sm.done = make(chan struct{})
	sm.wg = wg
	sm.buffer = b
	sm.info = make(map[string]string)
	sm.info["retention_time"] = cfg.Retention
	sm.info["flush_time"] = cfg.FlushTime
	sm.info["backend_type"] = backendConfig.Type

	return sm, nil
}
