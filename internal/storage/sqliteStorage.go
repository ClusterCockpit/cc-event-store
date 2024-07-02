package storage2

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"

	cclog "github.com/ClusterCockpit/cc-metric-collector/pkg/ccLogger"
	sq "github.com/Masterminds/squirrel"
	_ "github.com/mattn/go-sqlite3"
)

type sqliteStorageConfig struct {
	Type     string   `json:"type"`
	Flags    []string `json:"flags,omitempty"`
	Path     string   `json:"database_path"`
	Username string   `json:"username,omitempty"`
	Password string   `json:"password,omitempty"`
}

type sqliteStorage struct {
	sqlStorage
	config sqliteStorageConfig
}

type SqliteStorage interface {
	SqlStorage
}

func (s *sqliteStorage) Init(wg *sync.WaitGroup, config json.RawMessage) error {
	s.name = "SQLiteStorage"
	cclog.ComponentDebug(s.name, "Init")

	err := s.PreInit(wg, config)
	if err != nil {
		cclog.ComponentError(s.name, err.Error())
	}

	err = json.Unmarshal(config, &s.config)
	if err != nil {
		cclog.ComponentError(s.name, err.Error())
	}

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
	s.uri = fname_with_opts

	cclog.ComponentDebug(s.name, "Open sqlite3 DB", s.uri)
	db, err := sql.Open("sqlite3", s.uri)
	if err != nil {
		cclog.ComponentError(s.name, "Failed to open database", s.uri, ":", err.Error())
		return err
	} else if db == nil {
		cclog.ComponentError(s.name, "Failed to get database handle with", s.uri)
		return err
	}
	s.handle = db

	stmt := sq.Select("name").From("sqlite_schema").Where(sq.Eq{"type": "table"}).Where(sq.NotLike{"name": "sqlite_%"})

	rows, err := stmt.RunWith(db).Query()
	if err == nil {
		s.tablesLock.Lock()
		for rows.Next() {
			var name string
			err = rows.Scan(&name)
			if err == nil {
				s.tablesMap[name] = struct{}{}
			}
		}

		rows.Close()
		s.tablesLock.Unlock()
	} else {
		cclog.ComponentError(s.name, "Failed to get database tables")
		return err
	}

	return s.PostInit(wg, config)
}
