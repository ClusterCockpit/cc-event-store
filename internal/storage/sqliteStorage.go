package storage

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	cclog "github.com/ClusterCockpit/cc-metric-collector/pkg/ccLogger"
	_ "github.com/mattn/go-sqlite3"
)

type sqliteStorageConfig struct {
	Type      string   `json:"type"`
	Flags     []string `json:"flags,omitempty"`
	Path      string   `json:"database_path"`
	StoreLogs bool     `json:"store_logs,omitempty"`
	Retention string   `json:"retention_time"`
	Username  string   `json:"username,omitempty"`
	Password  string   `json:"password,omitempty"`
}

type sqliteStorage struct {
	sqlStorage
	config sqliteStorageConfig
}

type SqliteStorage interface {
	SqlStorage
}

func (s *sqliteStorage) Init(wg *sync.WaitGroup, configfile string) error {
	s.wg = wg
	s.done = make(chan struct{})
	s.input = nil
	s.name = "SQLiteStorage"
	s.started = false
	s.last_rowids = make(map[string]sqlStorageTableID)
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

	fname_with_opts := fmt.Sprintf("file:%s", s.config.Path)
	if len(s.config.Flags) > 0 {
		for i, f := range s.config.Flags {
			if i == 0 {
				fname_with_opts += fmt.Sprintf("?%s", f)
			} else {
				fname_with_opts += fmt.Sprintf("&%s", f)
			}
		}
	}
	cclog.ComponentDebug(s.name, "Open sql3 DB", fname_with_opts)
	db, err := sql.Open("sqlite3", fname_with_opts)
	if err != nil {
		cclog.ComponentError(s.name, "Failed to open database", fname_with_opts, ":", err.Error())
		return err
	} else if db == nil {
		cclog.ComponentError(s.name, "Failed to get database handle with", fname_with_opts)
		return err
	}
	s.handle = db
	s.last_lock = sync.Mutex{}

	return nil
}
