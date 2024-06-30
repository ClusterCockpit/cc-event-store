package storage

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	cclog "github.com/ClusterCockpit/cc-metric-collector/pkg/ccLogger"
	_ "github.com/lib/pq"
)

type postgresStorageConfig struct {
	Type              string   `json:"type"`
	Flags             []string `json:"flags,omitempty"`
	Server            string   `json:"server,omitempty"`
	Port              int      `json:"port,omitempty"`
	Path              string   `json:"database_path"`
	StoreLogs         bool     `json:"store_logs,omitempty"`
	Retention         string   `json:"retention_time"`
	Username          string   `json:"username,omitempty"`
	Password          string   `json:"password,omitempty"`
	ConnectionTimeout int      `json:"connection_timeout,omitempty"`
}

type postgresStorage struct {
	sqlStorage
	config postgresStorageConfig
}

type PostgresStorage interface {
	SqlStorage
}

func (s *postgresStorage) Init(wg *sync.WaitGroup, configfile string) error {
	s.wg = wg
	s.done = make(chan struct{})
	s.input = nil
	s.name = "PostgresStorage"
	s.started = false
	s.last_rowids = make(map[string]sqlStorageTableID)
	s.config.Server = "localhost"
	s.config.Port = 5432
	s.config.ConnectionTimeout = 1
	s.json_access = "->>"

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
	cclog.ComponentDebug(s.name, "Parsing retention time", s.config.Retention)
	t, err := time.ParseDuration(s.config.Retention)
	if err != nil {
		cclog.ComponentError(s.name, err.Error())
		return err
	}
	s.retention = t

	s.config.Flags = append(s.config.Flags, fmt.Sprintf("connect_timeout=%d", s.config.ConnectionTimeout))
	//pqgotest:password@localhost/pqgotest?sslmode=verify-full"
	fname_with_opts := "postgres://"
	if len(s.config.Username) > 0 {
		fname_with_opts += s.config.Username
		if len(s.config.Password) > 0 {
			fname_with_opts += ":" + s.config.Password
		}
		fname_with_opts += "@"
	}

	fname_with_opts += fmt.Sprintf("%s:%d/%s", s.config.Server, s.config.Port, s.config.Path)
	if len(s.config.Flags) > 0 {
		for i, f := range s.config.Flags {
			if i == 0 {
				fname_with_opts += fmt.Sprintf("?%s", f)
			} else {
				fname_with_opts += fmt.Sprintf("&%s", f)
			}
		}
	}
	cclog.ComponentDebug(s.name, "Open Postgres DB", fname_with_opts)
	db, err := sql.Open("postgres", fname_with_opts)
	if err != nil {
		cclog.ComponentError(s.name, "Failed to open database", fname_with_opts, ":", err.Error())
		return err
	}
	s.handle = db
	s.last_lock = sync.Mutex{}

	return nil
}
