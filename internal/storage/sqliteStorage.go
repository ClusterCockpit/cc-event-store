package storage

import (
	"database/sql"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"

	cclog "github.com/ClusterCockpit/cc-metric-collector/pkg/ccLogger"
	lp "github.com/ClusterCockpit/cc-metric-collector/pkg/ccMetric"
	_ "github.com/mattn/go-sqlite3"
)

type sqliteStorage struct {
	name     string
	filename string
	handle   *sql.DB
	last_idx uint64
	submit   chan lp.CCMetric
	queryIn  chan QueryRequest
	queryOut chan QueryResult
	done     chan bool
	wg       *sync.WaitGroup
}

type SqliteStorage interface {
	Start() error
	Close()
	Submit(event lp.CCMetric)
	Query(request QueryRequest) ([]string, error)
}

const createTableStmt = `create table IF NOT EXISTS events (id integer not null primary key, timestamp timestamp, name text, hostname text, type text, typeid text, stype text, stypeid text, othertags text, event text);`
const insertStmt = `insert into events(id, timestamp, name, hostname, type, typeid, stype, stypeid, othertags, event) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`
const (
	EVENT_FIELD_KEY  = `event`
	HOSTNAME_TAG_KEY = `hostname`
	TYPE_TAG_KEY     = `type`
	TYPEID_TAG_KEY   = `type-id`
	STYPE_TAG_KEY    = `stype`
	STYPEID_TAG_KEY  = `stype-id`
)

type QueryRequest struct {
	Event      string
	From       int64
	To         int64
	Hostname   string
	Conditions []string
}

type QueryResult struct {
	Results []string
	Error   error
}

func (cs *sqliteStorage) query(request QueryRequest) ([]string, error) {
	tx, err := cs.handle.Begin()
	if err != nil {
		cclog.ComponentError(cs.name, err.Error())
		return nil, err
	}

	queryStmt := fmt.Sprintf("select event from events where event == %s and time >= %d and time <= %d and hostname == %s", request.Event, request.From, request.To, request.Hostname)
	if len(request.Conditions) > 0 {
		queryStmt += " and " + strings.Join(request.Conditions, " and ")
	}

	stmt, err := tx.Prepare(queryStmt)
	if err != nil {
		cclog.ComponentError(cs.name, err.Error())
		return nil, err
	}
	defer stmt.Close()
	_, err = stmt.Exec()
	if err != nil {
		cclog.ComponentError(cs.name, err.Error())
		return nil, err
	}
	out := make([]string, 0)

	err = tx.Commit()
	if err != nil {
		cclog.ComponentError(cs.name, err.Error())
		return nil, err
	}
	return out, nil
}

func (cs *sqliteStorage) write(event lp.CCMetric) error {
	tx, err := cs.handle.Begin()
	if err != nil {
		cclog.ComponentError(cs.name, err.Error())
		return err
	}
	cclog.ComponentDebug(cs.name, "Preparing insert statement for", cs.filename)
	stmt, err := tx.Prepare(insertStmt)
	if err != nil {
		cclog.ComponentError(cs.name, err.Error())
		return err
	}
	defer stmt.Close()
	if ev, ok := event.GetField(EVENT_FIELD_KEY); ok {
		args := make([]interface{}, 0)
		args = append(args, cs.last_idx+1, event.Time(), event.Name())
		if h, ok := event.GetTag(HOSTNAME_TAG_KEY); ok {
			args = append(args, h)
		} else {
			err = fmt.Errorf("event has no %s tag", HOSTNAME_TAG_KEY)
			return err
		}
		type_tag, ok := event.GetTag(TYPE_TAG_KEY)
		if ok {
			args = append(args, type_tag)
		} else {
			err = fmt.Errorf("event has no %s tag", TYPE_TAG_KEY)
			return err
		}
		if h, ok := event.GetTag(TYPEID_TAG_KEY); ok {
			args = append(args, h)
		} else {
			if type_tag == "node" {
				args = append(args, "0")
			} else {
				err = fmt.Errorf("event has no %s tag", TYPEID_TAG_KEY)
				return err
			}
		}
		stype_tag, ok := event.GetTag(STYPE_TAG_KEY)
		if ok {
			args = append(args, stype_tag)
		} else {
			args = append(args, nil)
		}
		if len(stype_tag) > 0 {
			if stypeid_tag, ok := event.GetTag(STYPEID_TAG_KEY); ok {
				args = append(args, stypeid_tag)
			} else {
				args = append(args, nil)
			}
		} else {
			args = append(args, nil)
		}
		othertags := make([]string, 0)
		for k, v := range event.Tags() {
			switch k {
			case HOSTNAME_TAG_KEY, TYPE_TAG_KEY, TYPEID_TAG_KEY, STYPE_TAG_KEY, STYPEID_TAG_KEY:
			default:
				othertags = append(othertags, fmt.Sprintf("%s=%s", k, v))
			}
		}
		sort.Strings(othertags)
		args = append(args, strings.Join(othertags, ","), ev)
		cclog.ComponentDebug(cs.name, "Insert with", args)
		_, err = stmt.Exec(args...)
		if err != nil {
			cclog.ComponentError(cs.name, err.Error())
			return err
		}
		err = tx.Commit()
		if err != nil {
			cclog.ComponentError(cs.name, err.Error())
			return err
		}
		cs.last_idx++
		cclog.ComponentDebug(cs.name, "Insert done to", cs.filename)
		return nil
	}
	return fmt.Errorf("event has no %s field", EVENT_FIELD_KEY)
}

func (s *sqliteStorage) Start() error {
	s.wg.Add(1)
	cclog.ComponentDebug(s.name, "START")
	go func() {
		for {
			select {
			case <-s.done:
				s.wg.Done()
				cclog.ComponentDebug(s.name, "DONE")
				return
			case e := <-s.submit:
				fmt.Println(e)
				s.write(e)
			}
		}
	}()
	go func() {
		for {
			select {
			case <-s.done:
				s.wg.Done()
				cclog.ComponentDebug(s.name, "DONE")
				return
			case q := <-s.queryIn:
				fmt.Println(q)
				results, err := s.query(q)
				if err == nil {
					s.queryOut <- QueryResult{
						Results: results,
						Error:   nil,
					}
				} else {
					s.queryOut <- QueryResult{
						Results: nil,
						Error:   err,
					}
				}

			}
		}
	}()
	cclog.ComponentDebug(s.name, "STARTED")
	return nil
}

func (s *sqliteStorage) Close() {
	s.done <- true
	s.done <- true
	s.handle.Close()
	cclog.ComponentDebug(s.name, "CLOSE")
}

func (s *sqliteStorage) Submit(event lp.CCMetric) {
	s.submit <- event
}

func (s *sqliteStorage) Query(request QueryRequest) ([]string, error) {
	s.queryIn <- request
	result := <-s.queryOut
	return result.Results, result.Error
}

func NewStorage(wg *sync.WaitGroup, basefolder, cluster string) (SqliteStorage, error) {
	s := new(sqliteStorage)
	s.name = fmt.Sprintf("SqliteStorage(%s)", cluster)
	s.filename = fmt.Sprintf("%s/%s.db", basefolder, cluster)
	if info, err := os.Stat(basefolder); err == nil {
		if !info.IsDir() {
			err := fmt.Errorf("%s exists but is no folder", basefolder)
			cclog.ComponentError(s.name, err.Error())
			return nil, err
		}
	} else {
		err = os.MkdirAll(basefolder, 0700)
		if err != nil {
			cclog.ComponentError(s.name, "Failed creating base folder:", err.Error())
			return nil, err
		}
	}
	db, err := sql.Open("sqlite3", s.filename)
	if err != nil {
		cclog.ComponentError(s.name, "Failed to create database", s.filename, ":", err.Error())
		return nil, err
	}
	_, err = db.Exec(createTableStmt)
	if err != nil {
		cclog.ComponentError(s.name, fmt.Sprintf("%q: %s", err, createTableStmt))
		return nil, err
	}
	s.handle = db
	s.wg = wg
	s.done = make(chan bool)
	s.submit = make(chan lp.CCMetric)
	return s, nil
}
