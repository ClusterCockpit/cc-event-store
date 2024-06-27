package storage

import (
	"database/sql"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	cclog "github.com/ClusterCockpit/cc-metric-collector/pkg/ccLogger"
	lp "github.com/ClusterCockpit/cc-metric-collector/pkg/ccMetric"
	gocron "github.com/go-co-op/gocron/v2"
	_ "github.com/mattn/go-sqlite3"
)

type sqliteStorage struct {
	name      string
	filename  string
	handle    *sql.DB
	lock      sync.Mutex
	last_idx  uint64
	submit    chan lp.CCMetric
	queryIn   chan InternalRequest
	queryOut  chan QueryResult
	done      chan bool
	wg        *sync.WaitGroup
	max_age   time.Duration
	scheduler gocron.Scheduler
	started   bool
}

type SqliteStorage interface {
	Start() error
	Close()
	Submit(event lp.CCMetric)
	Query(request QueryRequest) ([]QueryResultEvent, error)
	Delete(to int64) error
}

const createTableStmt = `create table IF NOT EXISTS events (id integer not null primary key, timestamp integer, name text, hostname text, type text, typeid text, stype text, stypeid text, othertags text, event text);`
const insertStmt = `insert into events(id, timestamp, name, hostname, type, typeid, stype, stypeid, othertags, event) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`
const queryStmt = `select event,timestamp from events where name = ? and timestamp >= ? and timestamp <= ? and hostname = ?`
const deleteStmt = `delete from events where timestamp <= ?`

const (
	EVENT_FIELD_KEY  = `event`
	HOSTNAME_TAG_KEY = `hostname`
	TYPE_TAG_KEY     = `type`
	TYPEID_TAG_KEY   = `type-id`
	STYPE_TAG_KEY    = `stype`
	STYPEID_TAG_KEY  = `stype-id`
)

type InternalRequest struct {
	RequestType string
	Request     QueryRequest
}

type QueryRequest struct {
	Event      string
	From       int64
	To         int64
	Hostname   string
	Conditions []string
}

type QueryResultEvent struct {
	Timestamp int64
	Event     string
}

type QueryResult struct {
	Results []QueryResultEvent
	Error   error
}

//const getLastIdStmt string = `select last(id) from events`

func (cs *sqliteStorage) delete(to int64) error {
	cs.lock.Lock()
	defer cs.lock.Unlock()
	cclog.ComponentDebug(cs.name, "Begin delete")
	tx, err := cs.handle.Begin()
	if err != nil {
		cclog.ComponentError(cs.name, err.Error())
		return err
	}
	args := make([]interface{}, 0)
	args = append(args, to)
	cclog.ComponentDebug(cs.name, fmt.Sprintf("Using '%s' with to = %d", deleteStmt, to))
	stmt, err := tx.Prepare(deleteStmt)
	if err != nil {
		cclog.ComponentError(cs.name, err.Error())
		return err
	}
	defer stmt.Close()
	res, err := stmt.Exec(args...)
	if err != nil {
		cclog.ComponentError(cs.name, err.Error())
		return err
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	cclog.ComponentDebug(cs.name, "Deleted", rowsAffected, "events")
	if rowsAffected == 0 {
		return fmt.Errorf("failed to delete events before %d", to)
	}

	err = tx.Commit()
	if err != nil {
		cclog.ComponentError(cs.name, err.Error())
		return err
	}
	return nil
}

func (cs *sqliteStorage) query(request QueryRequest) ([]QueryResultEvent, error) {
	cs.lock.Lock()
	defer cs.lock.Unlock()
	cclog.ComponentDebug(cs.name, "Begin query")
	tx, err := cs.handle.Begin()
	if err != nil {
		cclog.ComponentError(cs.name, err.Error())
		return nil, err
	}

	args := make([]interface{}, 0)
	args = append(args, request.Event)
	args = append(args, request.From)
	args = append(args, request.To)
	args = append(args, request.Hostname)

	qstmt := queryStmt

	if len(request.Conditions) > 0 {
		mycond := make([]string, 0)
		for _, c := range request.Conditions {
			switch {
			case strings.Contains(c, "stype-id"):
				mycond = append(mycond, strings.ReplaceAll(c, "stype-id", "stypeid"))
			case strings.Contains(c, "type-id"):
				mycond = append(mycond, strings.ReplaceAll(c, "type-id", "typeid"))
			default:
				mycond = append(mycond, c)
			}
		}
		qstmt += " and " + strings.Join(mycond, " and ")
	}
	cclog.ComponentDebug(cs.name, fmt.Sprintf("Using '%s'", qstmt))
	cclog.ComponentDebug(cs.name, fmt.Sprintf("Event '%s' Hostname '%s' From %d To %d", request.Event, request.Hostname, request.From, request.To))
	stmt, err := tx.Prepare(qstmt)
	if err != nil {
		cclog.ComponentError(cs.name, err.Error())
		return nil, err
	}
	defer stmt.Close()
	sqlResult, err := stmt.Query(args...)
	if err != nil {
		cclog.ComponentError(cs.name, err.Error())
		return nil, err
	}

	out := make([]QueryResultEvent, 0)
	for sqlResult.Next() {
		var event string
		var timestamp int64
		sqlResult.Scan(&event, &timestamp)
		//out = append(out, fmt.Sprintf("%d|%s", time.Unix(timestamp, 0).Unix(), event))
		out = append(out, QueryResultEvent{
			Timestamp: time.Unix(timestamp, 0).Unix(),
			Event:     event,
		})
	}

	err = tx.Commit()
	if err != nil {
		cclog.ComponentError(cs.name, err.Error())
		return nil, err
	}

	return out, nil
}

func (cs *sqliteStorage) write(event lp.CCMetric) error {
	cs.lock.Lock()
	defer cs.lock.Unlock()
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
		args = append(args, cs.last_idx+1, event.Time().Unix(), event.Name())
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
	sched, err := gocron.NewScheduler()
	if err != nil {
		cclog.ComponentError(s.name, "failed to initialize gocron scheduler")
		return err
	}
	s.scheduler = sched

	s.scheduler.NewJob(
		gocron.DailyJob(
			1,
			gocron.NewAtTimes(
				gocron.NewAtTime(3, 0, 0),
			),
		),
		gocron.NewTask(
			func() {
				before := time.Now().Add(-s.max_age)
				err := s.Delete(before.Unix())
				if err != nil {
					cclog.ComponentError(s.name)
				}
			},
		),
	)
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
	s.wg.Add(1)
	go func() {
		for {
			select {
			case <-s.done:
				s.wg.Done()
				cclog.ComponentDebug(s.name, "DONE")
				return
			case q := <-s.queryIn:
				switch {
				case q.RequestType == "query":
					results, err := s.query(q.Request)
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
				case q.RequestType == "delete":
					err := s.delete(q.Request.To)
					if err == nil {
						s.queryOut <- QueryResult{
							Results: nil,
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
		}
	}()

	s.started = true
	cclog.ComponentDebug(s.name, "STARTED")
	return nil
}

func (s *sqliteStorage) Close() {
	if s.started {
		s.done <- true
		s.done <- true
		if s.scheduler != nil {
			s.scheduler.Shutdown()
		}
		s.handle.Close()
	}
	cclog.ComponentDebug(s.name, "CLOSE")
}

func (s *sqliteStorage) Submit(event lp.CCMetric) {
	s.submit <- event
}

func (s *sqliteStorage) Query(request QueryRequest) ([]QueryResultEvent, error) {
	s.queryIn <- InternalRequest{
		RequestType: "query",
		Request:     request,
	}
	result := <-s.queryOut
	return result.Results, result.Error
}

func (s *sqliteStorage) Delete(to int64) error {
	s.queryIn <- InternalRequest{
		RequestType: "delete",
		Request: QueryRequest{
			To: to,
		},
	}
	out := <-s.queryOut
	return out.Error
}

func NewStorage(wg *sync.WaitGroup, basefolder, cluster string, max_age time.Duration) (SqliteStorage, error) {
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
	s.queryIn = make(chan InternalRequest)
	s.queryOut = make(chan QueryResult)
	s.max_age = max_age
	s.started = false

	return s, nil
}
