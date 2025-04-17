// Copyright (C) NHR@FAU, University Erlangen-Nuremberg.
// All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.
package storage

import (
	"database/sql"
	"encoding/json"
	"fmt"

	cclog "github.com/ClusterCockpit/cc-lib/ccLogger"
	sq "github.com/Masterminds/squirrel"
	_ "github.com/lib/pq"
)

type postgresStorageConfig struct {
	Type              string   `json:"type"`
	Path              string   `json:"database_path"`
	Username          string   `json:"username,omitempty"`
	Password          string   `json:"password,omitempty"`
	Server            string   `json:"server,omitempty"`
	Flags             []string `json:"flags,omitempty"`
	ConnectionTimeout int      `json:"connection_timeout,omitempty"`
	Port              int      `json:"port,omitempty"`
}

type postgresStorage struct {
	sqlStorage
	config postgresStorageConfig
}

func (s *postgresStorage) Init(config json.RawMessage, stats *storageStats) error {
	s.name = "PostgresStorage"
	s.config.Server = "localhost"
	s.config.Port = 5432
	s.config.ConnectionTimeout = 1
	cclog.ComponentDebug(s.name, "Init")

	err := s.PreInit(config, stats)
	if err != nil {
		cclog.ComponentError(s.name, err.Error())
	}

	err = json.Unmarshal(config, &s.config)
	if err != nil {
		cclog.ComponentError(s.name, err.Error())
	}

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

	stmt := sq.Select("table_name").From("information_schema.tables ").Where(sq.Eq{"table_type": "BASE TABLE"}).Where(sq.Eq{"table_schema": "public"})

	rows, err := stmt.RunWith(db).Query()
	if err != nil {
		cclog.ComponentError(s.name, "Failed to get tables in database", fname_with_opts, ":", err.Error())
		return err
	}
	s.tablesLock.Lock()
	for rows.Next() {
		var name string
		err = rows.Scan(&name)
		if err == nil {
			s.tablesMap[name] = struct{}{}
		}
	}
	s.tablesLock.Unlock()
	rows.Close()
	err = s.PostInit(config)
	if err != nil {
		cclog.ComponentError(s.name, err.Error())
	}
	return nil
}
