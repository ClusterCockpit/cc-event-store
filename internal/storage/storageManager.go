package storage2

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	lp "github.com/ClusterCockpit/cc-energy-manager/pkg/cc-message"
	cclog "github.com/ClusterCockpit/cc-metric-collector/pkg/ccLogger"
	"github.com/go-co-op/gocron/v2"
)

var AvailableStorageBackends map[string]Storage = map[string]Storage{
	"sqlite":   new(sqliteStorage),
	"postgres": new(postgresStorage),
}

type QueryRequestType int

const (
	QueryTypeEvent QueryRequestType = 0
	QueryTypeLog   QueryRequestType = 1
)

type storageManagerConfig struct {
	BatchSize  int             `json:"batch_size,omitempty"`
	MaxProcess int             `json:"max_process,omitempty"`
	Retention  string          `json:"retention_time"`
	FlushTime  string          `json:"flush_time"`
	StoreLogs  bool            `json:"store_logs,omitempty"`
	Backend    json.RawMessage `json:"backend"`
}

type storageManager struct {
	store         Storage
	wg            *sync.WaitGroup
	done          chan struct{}
	input         chan *lp.CCMessage
	config        storageManagerConfig
	retentionTime time.Duration

	flushTime time.Duration
	// timer to run Flush()
	flushTimer *time.Timer
	// Lock to assure that only one timer is running at a time
	timerLock sync.Mutex

	scheduler gocron.Scheduler
	buffer    StorageBuffer
	started   bool
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
	Query(request QueryRequest) (QueryResult, error)
	SetInput(input chan *lp.CCMessage)
	GetInput() chan *lp.CCMessage
}

func (sm *storageManager) Close() {
	if sm.started {
		sm.done <- struct{}{}
		<-sm.done
	}
	if sm.store != nil {
		// Stop existing timer and immediately flush
		if sm.flushTimer != nil {
			if ok := sm.flushTimer.Stop(); ok {
				sm.flushTimer = nil
				sm.timerLock.Unlock()
			}
		}
		sm.Flush()
		sm.store.Close()
	}
	if sm.started {
		if sm.scheduler != nil {
			sm.scheduler.Shutdown()
			sm.scheduler = nil
		}
	}
	sm.started = false
}

func (sm *storageManager) Flush() {
	if sm.store != nil {
		sm.buffer.Lock()
		if sm.buffer.Len() > 0 {
			total := 0
			sm.store.Write(sm.buffer.Get())
			total += sm.buffer.Len()
			sm.buffer.Clear()
			if len(sm.input) > 0 {
				for i := 0; i < len(sm.input); i++ {
					sm.buffer.Add(<-sm.input)
				}
				if sm.buffer.Len() > 0 {
					sm.store.Write(sm.buffer.Get())
					total += sm.buffer.Len()
					sm.buffer.Clear()
				}
			}
			cclog.ComponentDebug("StorageManager", "Flush", total)
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

		to_buffer_or_write_batch := func(msg *lp.CCMessage) {
			if lp.IsEvent(*msg) || (lp.IsLog(*msg) && sm.config.StoreLogs) {
				if sm.buffer.Len() < sm.config.BatchSize {
					cclog.ComponentDebug("StorageManager", "Append to buffer", *msg)
					sm.buffer.Add(msg)
				} else {
					cclog.ComponentDebug("StorageManager", "Write batch of", sm.buffer.Len(), "messages")
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
				sm.buffer.Lock()
				to_buffer_or_write_batch(e)
				for i := 0; i < sm.config.MaxProcess && len(sm.input) > 0; i++ {
					to_buffer_or_write_batch(<-sm.input)
				}
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
		sm.Flush()
		return sm.store.Query(request)
	}
	err := fmt.Errorf("no storage backend active, cannot query")
	return QueryResult{Results: make([]QueryResultEvent, 0), Error: err}, err
}

func (sm *storageManager) SetInput(input chan *lp.CCMessage) {
	sm.input = input
}
func (sm *storageManager) GetInput() chan *lp.CCMessage {
	return sm.input
}

func NewStorageManager(wg *sync.WaitGroup, storageConfigFile string) (StorageManager, error) {
	sm := new(storageManager)
	sm.input = nil
	sm.store = nil

	configFile, err := os.Open(storageConfigFile)
	if err != nil {
		cclog.Error(err.Error())
		return nil, err
	}
	defer configFile.Close()

	var config storageManagerConfig = storageManagerConfig{
		BatchSize:  20,
		MaxProcess: 10,
		Retention:  "24h",
		FlushTime:  "1s",
		StoreLogs:  true,
	}
	jsonParser := json.NewDecoder(configFile)
	err = jsonParser.Decode(&config)
	if err != nil {
		cclog.Error(err.Error())
		return nil, err
	}

	if len(config.Retention) > 0 {
		t, err := time.ParseDuration(config.Retention)
		if err != nil {
			cclog.ComponentError("StorageManager", err.Error())
			return nil, err
		}
		sm.retentionTime = t
	} else {
		cclog.ComponentError("StorageManager", "Retention time cannot be empty")
		return nil, err
	}
	if len(config.Retention) > 0 {
		t, err := time.ParseDuration(config.FlushTime)
		if err != nil {
			cclog.ComponentError("StorageManager", err.Error())
			return nil, err
		}
		sm.flushTime = t
	}

	var backendConfig struct {
		Type string `json:"type"`
	}

	err = json.Unmarshal(config.Backend, &backendConfig)
	if err != nil {
		cclog.Error(err.Error())
		return nil, err
	}
	sm.config = config

	if _, ok := AvailableStorageBackends[backendConfig.Type]; !ok {
		err = fmt.Errorf("unknown storage type %s", backendConfig.Type)
		return nil, err
	}
	cclog.ComponentDebug("StorageManager", "Getting storage for", backendConfig.Type)
	s := AvailableStorageBackends[backendConfig.Type]

	err = s.Init(wg, config.Backend)
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

	sm.done = make(chan struct{})
	sm.wg = wg
	sm.buffer = b

	return sm, nil
}
