package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"sync"

	lp2 "github.com/ClusterCockpit/cc-energy-manager/pkg/cc-message"
	cclog "github.com/ClusterCockpit/cc-metric-collector/pkg/ccLogger"
	lp "github.com/ClusterCockpit/cc-metric-collector/pkg/ccMetric"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
)

type influxStorageConfig struct {
	Type         string            `json:"type"`
	Server       string            `json:"server,omitempty"`
	Port         int               `json:"port,omitempty"`
	Flags        map[string]string `json:"flags,omitempty"`
	Token        string            `json:"token"`
	Organisation string            `json:"organisation"`
	StoreLogs    bool              `json:"store_logs,omitempty"`
}

type influxStorage struct {
	storage
	config     influxStorageConfig
	client     influxdb2.Client
	dbs        map[string]api.WriteAPI
	empty_meta map[string]bool
}

type InfluxStorage interface {
	Storage
}

func (s *influxStorage) connectDB(database string) {
	if _, ok := s.dbs[database]; !ok {
		s.dbs[database] = s.client.WriteAPI(s.config.Organisation, database)
	}
}

func (s *influxStorage) Init(wg *sync.WaitGroup, configfile string) error {
	s.wg = wg
	s.done = make(chan struct{})
	s.input = nil
	s.name = "InfluxDBStorage"
	s.started = false
	s.config.Server = "localhost"
	s.config.Port = 8086
	s.config.StoreLogs = false
	s.empty_meta = make(map[string]bool)

	configFile, err := os.Open(configfile)
	if err != nil {
		cclog.ComponentError(s.name, err.Error())
		return err
	}
	defer configFile.Close()

	jsonParser := json.NewDecoder(configFile)
	err = jsonParser.Decode(&s.config)
	if err != nil {
		cclog.ComponentError(s.name, err.Error())
		return err
	}

	uri := fmt.Sprintf("http://%s:%d", s.config.Server, s.config.Port)
	clientOptions := influxdb2.DefaultOptions()
	if len(s.config.Flags) > 0 {
		// todo add flags to opts
		if b, ok := s.config.Flags["batch_size"]; ok {
			cclog.ComponentDebug(s.name, "Batch size", b)
			bi, err := strconv.ParseInt(b, 10, 64)
			if err != nil {
				cclog.ComponentError(s.name, "failed to parse batch_size:", err.Error())
			} else {
				clientOptions.SetBatchSize(uint(bi))
			}
		}
		if i, ok := s.config.Flags["flush_interval"]; ok {
			cclog.ComponentDebug(s.name, "Flush interval", i)
			ii, err := strconv.ParseInt(i, 10, 64)
			if err != nil {
				cclog.ComponentError(s.name, "failed to parse flush_interval:", err.Error())
			} else {
				clientOptions.SetFlushInterval(uint(ii))
			}
		}
		if i, ok := s.config.Flags["retry_interval"]; ok {
			cclog.ComponentDebug(s.name, "MaxRetryInterval", i)
			ii, err := strconv.ParseInt(i, 10, 64)
			if err != nil {
				cclog.ComponentError(s.name, "failed to parse retry_interval:", err.Error())
			} else {
				clientOptions.SetMaxRetryInterval(uint(ii))
			}
		}
		if i, ok := s.config.Flags["max_retry_time"]; ok {
			cclog.ComponentDebug(s.name, "MaxRetryTime", i)
			ii, err := strconv.ParseInt(i, 10, 64)
			if err != nil {
				cclog.ComponentError(s.name, "failed to parse max_retry_time:", err.Error())
			} else {
				clientOptions.SetMaxRetryInterval(uint(ii))
			}
		}
		if i, ok := s.config.Flags["retry_exponential_base"]; ok {
			cclog.ComponentDebug(s.name, "Exponential Base", i)
			ii, err := strconv.ParseInt(i, 10, 64)
			if err != nil {
				cclog.ComponentError(s.name, "failed to parse retry_exponential_base:", err.Error())
			} else {
				clientOptions.SetExponentialBase(uint(ii))
			}
		}
		if i, ok := s.config.Flags["max_retries"]; ok {
			cclog.ComponentDebug(s.name, "Max Retries", i)
			ii, err := strconv.ParseInt(i, 10, 64)
			if err != nil {
				cclog.ComponentError(s.name, "failed to parse max_retries:", err.Error())
			} else {
				clientOptions.SetMaxRetries(uint(ii))
			}
		}
	}
	s.client = influxdb2.NewClientWithOptions(uri, s.config.Token, clientOptions)

	return nil
}

func (s *influxStorage) Query(request QueryRequest) (QueryResult, error) {
	return QueryResult{}, nil
}

func (s *influxStorage) Write(msg lp.CCMetric) {
	if _, ok := msg.GetTag(CLUSTER_TAG_KEY); !ok {
		return
	}
	cluster, _ := msg.GetTag(CLUSTER_TAG_KEY)
	dbname := cluster
	if msg.HasField(EVENT_FIELD_KEY) {
		dbname += "_" + EVENT_FIELD_KEY + "s"
	} else if msg.HasField(LOG_FIELD_KEY) {
		if !s.config.StoreLogs {
			return
		}
		dbname += "_" + LOG_FIELD_KEY + "s"
	}

	s.connectDB(dbname)

	handle := s.dbs[dbname]

	p := msg.ToPoint(s.empty_meta)

	handle.WritePoint(p)
	cclog.ComponentDebug(s.name, "Insert into ", dbname, "successful")
}

func (s *influxStorage) Start() {
	cclog.ComponentDebug(s.name, "START")
	s.wg.Add(1)
	go func() {
		for {
			select {
			case <-s.done:
				s.wg.Done()
				close(s.done)
				cclog.ComponentDebug(s.name, "DONE")
				return
			case e := <-s.input:
				cclog.ComponentDebug(s.name, "Input")
				new, err := lp2.NewMessage(e.Name(), e.Tags(), e.Meta(), e.Fields(), e.Time())
				if err == nil {
					s.Write(new)
				}
			}
		}
	}()
	s.started = true
}

func (s *influxStorage) Close() {
	if s.started {
		s.done <- struct{}{}
		<-s.done
	}
}

func (s *influxStorage) SetInput(input chan lp.CCMetric) {
	s.input = input
}
