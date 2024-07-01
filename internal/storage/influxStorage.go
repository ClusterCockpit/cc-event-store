package storage

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	lp2 "github.com/ClusterCockpit/cc-energy-manager/pkg/cc-message"
	cclog "github.com/ClusterCockpit/cc-metric-collector/pkg/ccLogger"
	lp "github.com/ClusterCockpit/cc-metric-collector/pkg/ccMetric"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
)

type influxStorageConfig struct {
	Type                  string `json:"type"`
	Server                string `json:"server,omitempty"`
	Port                  int    `json:"port,omitempty"`
	Token                 string `json:"token"`
	Organisation          string `json:"organisation"`
	StoreLogs             bool   `json:"store_logs,omitempty"`
	FlushInterval         uint   `json:"flush_interval,omitempty"`
	InfluxRetryInterval   string `json:"retry_interval,omitempty"`
	InfluxExponentialBase uint   `json:"retry_exponential_base,omitempty"`
	InfluxMaxRetries      uint   `json:"max_retries,omitempty"`
	InfluxMaxRetryTime    string `json:"max_retry_time,omitempty"`
	CustomFlushInterval   string `json:"custom_flush_interval,omitempty"`
	MaxRetryAttempts      uint   `json:"max_retry_attempts,omitempty"`
	BatchSize             uint   `json:"batch_size,omitempty"`
	SSL                   bool   `json:"ssl,omitempty"`
}

type influxStorage struct {
	storage
	config              influxStorageConfig
	client              influxdb2.Client
	dbs                 map[string]api.WriteAPI
	empty_meta          map[string]bool
	influxRetryInterval uint
	influxMaxRetryTime  uint
	errors              chan error
}

type InfluxStorage interface {
	Storage
}

func (s *influxStorage) connectDB(database string) {
	if _, ok := s.dbs[database]; !ok {
		s.dbs[database] = s.client.WriteAPI(s.config.Organisation, database)
		go func() {
			errch := s.dbs[database].Errors()
			for err := range errch {
				s.errors <- err
			}
		}()
		// s.dbs[database].SetWriteFailedCallback(func(batch string, err influxdb.influxdb2ApiHttp.Error, retryAttempts uint) bool {
		// 	mlist := strings.Split(batch, "\n")
		// 	cclog.ComponentError(s.name, fmt.Sprintf("Failed to write batch with %d metrics %d times (max: %d): %s", len(mlist), retryAttempts, s.config.MaxRetryAttempts, err.Error()))
		// 	return retryAttempts <= s.config.MaxRetryAttempts
		// })
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
	toUint := func(duration string, def uint) uint {
		t, err := time.ParseDuration(duration)
		if err == nil {
			return uint(t.Milliseconds())
		}
		return def
	}
	s.influxRetryInterval = toUint(s.config.InfluxRetryInterval, s.influxRetryInterval)
	s.influxMaxRetryTime = toUint(s.config.InfluxMaxRetryTime, s.influxMaxRetryTime)

	prefix := "http"
	if s.config.SSL {
		prefix += "s"
	}

	uri := fmt.Sprintf("%s://%s:%d", prefix, s.config.Server, s.config.Port)
	clientOptions := influxdb2.DefaultOptions()
	if s.config.BatchSize != 0 {
		cclog.ComponentDebug(s.name, "Batch size", s.config.BatchSize)
		clientOptions.SetBatchSize(s.config.BatchSize)
	}
	if s.config.FlushInterval != 0 {
		cclog.ComponentDebug(s.name, "Flush interval", s.config.FlushInterval)
		clientOptions.SetFlushInterval(s.config.FlushInterval)
	}
	if s.influxRetryInterval != 0 {
		cclog.ComponentDebug(s.name, "MaxRetryInterval", s.influxRetryInterval)
		clientOptions.SetMaxRetryInterval(s.influxRetryInterval)
	}
	if s.influxMaxRetryTime != 0 {
		cclog.ComponentDebug(s.name, "MaxRetryTime", s.influxMaxRetryTime)
		clientOptions.SetMaxRetryTime(s.influxMaxRetryTime)
	}
	if s.config.InfluxExponentialBase != 0 {
		cclog.ComponentDebug(s.name, "Exponential Base", s.config.InfluxExponentialBase)
		clientOptions.SetExponentialBase(s.config.InfluxExponentialBase)
	}
	if s.config.InfluxMaxRetries != 0 {
		cclog.ComponentDebug(s.name, "Max Retries", s.config.InfluxMaxRetries)
		clientOptions.SetMaxRetries(s.config.InfluxMaxRetries)
	}
	clientOptions.SetTLSConfig(
		&tls.Config{
			InsecureSkipVerify: true,
		},
	).SetPrecision(time.Second)
	s.client = influxdb2.NewClientWithOptions(uri, s.config.Token, clientOptions)

	ok, err := s.client.Ping(context.Background())
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("connection to %s not healthy", uri)
	}

	s.errors = make(chan error)
	go func() {
		for err := range s.errors {
			cclog.ComponentError(s.name, err.Error())
		}
	}()

	return nil
}

func (s *influxStorage) Query(request QueryRequest) (QueryResult, error) {
	return QueryResult{}, errors.New("not implemented")
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
		s.client.Close()
	}
}

func (s *influxStorage) SetInput(input chan lp.CCMetric) {
	s.input = input
}
