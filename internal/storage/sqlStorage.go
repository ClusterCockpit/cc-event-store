package storage

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	cclog "github.com/ClusterCockpit/cc-metric-collector/pkg/ccLogger"
	lp "github.com/ClusterCockpit/cc-metric-collector/pkg/ccMetric"
	sq "github.com/Masterminds/squirrel"
	"github.com/go-co-op/gocron/v2"
)

type sqlStorageConfig struct {
	Type      string   `json:"type"`
	SqlFlags  []string `json:"flags,omitempty"`
	Path      string   `json:"database_path"`
	StoreLogs bool     `json:"store_logs,omitempty"`
	Retention string   `json:"retention_time"`
	Username  string   `json:"username,omitempty"`
}

type sqlStorageTableID struct {
	Last int64
	Lock *sync.Mutex
}

type sqlStorage struct {
	storage
	handle      *sql.DB
	last_lock   sync.Mutex
	last_rowids map[string]sqlStorageTableID
	config      sqlStorageConfig
	retention   time.Duration
	scheduler   gocron.Scheduler
}

type SqlStorage interface {
	Close()
	Start()
	Init(wg *sync.WaitGroup, configfile string) error
	SetInput(input chan lp.CCMetric)
	Query(request QueryRequest) (QueryResult, error)
	Write(msg lp.CCMetric)
}

const createEventsTableStmt = `create table IF NOT EXISTS %s (id integer primary key, timestamp bigint, name text, hostname text, type text, typeid text, stype text, stypeid text, othertags JSON, event text);`
const createLogsTableStmt = `create table IF NOT EXISTS %s (id integer primary key, timestamp bigint, name text, hostname text, type text, typeid text, stype text, stypeid text, othertags JSON, log text);`

// const insertEventsStmt = `insert into %s (id, timestamp, name, hostname, type, typeid, stype, stypeid, othertags, event) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`
// const insertLogsStmt = `insert into %s (id, timestamp, name, hostname, type, typeid, stype, stypeid, othertags, log) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`
// const queryStmt = `select event,timestamp from events where name = ? and hostname = ? and timestamp >= ? and timestamp <= ?`

const (
	EVENT_FIELD_KEY  = `event`
	HOSTNAME_TAG_KEY = `hostname`
	CLUSTER_TAG_KEY  = `cluster`
	TYPE_TAG_KEY     = `type`
	TYPEID_TAG_KEY   = `type-id`
	STYPE_TAG_KEY    = `stype`
	STYPEID_TAG_KEY  = `stype-id`
	LOG_FIELD_KEY    = `log`
)

var MetricToSchema map[string]string = map[string]string{
	"event":    "event",
	"log":      "log",
	"hostname": "hostname",
	"type-id":  "typeid",
	"stype-id": "stypeid",
	"type":     "type",
	"stype":    "stype",
	"cluster":  "",
}

type otherTagsType map[string]string

func create_cluster_table(tablename string, handle *sql.DB) (int64, error) {
	stmtFormat := ""
	if strings.Contains(tablename, "events") {
		stmtFormat = createEventsTableStmt
	} else if strings.Contains(tablename, "logs") {
		stmtFormat = createLogsTableStmt
	}

	stmt := fmt.Sprintf(stmtFormat, tablename)
	cclog.Debug("Creating table ", tablename)
	_, err := handle.Exec(stmt)
	if err != nil {
		cclog.Error(fmt.Sprintf("%q: %s", err, stmt))
		return -1, err
	}
	// laststmt := sq.Select("max(id)").From(tablename)
	// res, err := laststmt.RunWith(handle).Query()
	// if err != nil {
	// 	return -1, err
	// }
	// idx := int64(0)
	// for res.Next() {
	// 	res.Scan(&idx)
	// }

	return 0, nil
}

func (s *sqlStorage) delete(to int64) error {
	for name, tab := range s.last_rowids {
		tab.Lock.Lock()
		stmt := sq.Delete(name).Where(sq.LtOrEq{"timestamp": to})
		_, err := stmt.RunWith(s.handle).Exec()
		tab.Lock.Unlock()
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *sqlStorage) Init(wg *sync.WaitGroup, configfile string) error {
	// s.wg = wg
	// s.done = make(chan struct{})
	// s.input = nil
	// s.name = "SQLStorage"
	// s.started = false
	// s.last_rowids = make(map[string]sqlStorageTableID)

	// configFile, err := os.Open(configfile)
	// if err != nil {
	// 	cclog.ComponentError(s.name, err.Error())
	// 	return err
	// }
	// defer configFile.Close()

	// jsonParser := json.NewDecoder(configFile)
	// err = jsonParser.Decode(&s.config)
	// if err != nil {
	// 	cclog.ComponentError(s.name, err.Error())
	// 	return err
	// }
	// cclog.ComponentDebug(s.name, "Parsing retention time", s.config.Retention)
	// t, err := time.ParseDuration(s.config.Retention)
	// if err != nil {
	// 	cclog.ComponentError(s.name, err.Error())
	// 	return err
	// }
	// s.retention = t

	// fname_with_opts := fmt.Sprintf("file:%s", s.config.Path)
	// if len(s.config.SqlFlags) > 0 {
	// 	for i, f := range s.config.SqlFlags {
	// 		if i == 0 {
	// 			fname_with_opts += fmt.Sprintf("?%s", f)
	// 		} else {
	// 			fname_with_opts += fmt.Sprintf("&%s", f)
	// 		}
	// 	}
	// }
	// cclog.ComponentDebug(s.name, "Open sql3 DB", fname_with_opts)
	// db, err := sql.Open("sql3", fname_with_opts)
	// if err != nil {
	// 	cclog.ComponentError(s.name, "Failed to open database", fname_with_opts, ":", err.Error())
	// 	return err
	// } else if db == nil {
	// 	cclog.ComponentError(s.name, "Failed to get database handle with", fname_with_opts)
	// 	return err
	// }
	// s.handle = db
	// s.last_lock = sync.Mutex{}

	return fmt.Errorf("not meant to be called directly, use SqliteStorage.Init() or PostgresStorage.Init()")
}

func (s *sqlStorage) Query(request QueryRequest) (QueryResult, error) {
	if s.handle == nil {
		return QueryResult{}, errors.New("cannot query, not initialized")
	}
	tablename := request.Cluster
	if request.QueryType == QueryTypeEvent {
		tablename += "_events"
	} else if request.QueryType == QueryTypeLog {
		tablename += "_logs"
	}
	cclog.ComponentDebug(s.name, "Query for table", tablename)
	qstmt := sq.Select("timestamp", "event").
		From(tablename).
		Where(sq.Eq{"hostname": request.Hostname}).
		Where(sq.Eq{"name": request.Event}).
		Where(sq.LtOrEq{"timestamp": request.To}).
		Where(sq.GtOrEq{"timestamp": request.From})
	if len(request.Conditions) > 0 {
		cclog.ComponentDebug(s.name, "Adding query condition", tablename)
		for _, c := range request.Conditions {
			pred := c.Pred
			if _, ok := MetricToSchema[pred]; !ok {
				pred = fmt.Sprintf("othertags%s'$.%s'", s.json_access, pred)
			} else {
				if strings.Contains(c.Pred, "type-id") {
					pred = strings.ReplaceAll(c.Pred, "type-id", "typeid")
				}
			}

			switch c.Operation {
			case "==":
				cclog.ComponentDebug(s.name, "Equal condition")
				qstmt = qstmt.Where(sq.Eq{pred: c.Args})
			case "!=":
				cclog.ComponentDebug(s.name, "NotEqual condition")
				qstmt = qstmt.Where(sq.NotEq{pred: c.Args})
			default:
				if len(c.Args) > 0 {
					cclog.ComponentDebug(s.name, c.Operation, "condition")
					qmarks := "(?"
					for i := 1; i < len(c.Args); i++ {
						qmarks += ",?"
					}
					// previous loop contains only open bracket, so this format string has to end with
					// a closing brakckt
					qstmt = qstmt.Where(fmt.Sprintf("%s %s %s)", pred, c.Operation, qmarks), c.Args...)
				}
				// if len(c.Args) == 1 {
				// 	qstmt = qstmt.Where(fmt.Sprintf("%s %s ?", pred, c.Operation), c.Args)
				// }
			}
		}
	}
	str, args, err := qstmt.ToSql()
	if err == nil {
		cclog.ComponentDebug(s.name, "Query", str, "with", args)
	}
	res, err := qstmt.RunWith(s.handle).Query()
	if err != nil {
		return QueryResult{}, err
	}
	out := QueryResult{
		Results: make([]QueryResultEvent, 0),
		Error:   nil,
	}
	for res.Next() {
		var ts int64
		var event string
		err = res.Scan(&ts, &event)
		if err == nil {
			out.Results = append(out.Results, QueryResultEvent{
				Timestamp: ts,
				Event:     event,
			})
		} else {
			out.Results = out.Results[:0]
			out.Error = err
			return out, err
		}
	}
	return out, nil
}

func (s *sqlStorage) Write(msg lp.CCMetric) {
	if s.handle == nil {
		return
	}
	cclog.ComponentDebug(s.name, fmt.Sprintf("Write: %v", msg.String()))
	if s.handle != nil {
		requires_tid := false
		if _, ok := msg.GetTag(CLUSTER_TAG_KEY); !ok {
			return
		}
		cluster, _ := msg.GetTag(CLUSTER_TAG_KEY)
		tablename := cluster
		if msg.HasField(EVENT_FIELD_KEY) {
			tablename += "_" + EVENT_FIELD_KEY + "s"
		} else if msg.HasField(LOG_FIELD_KEY) {
			if !s.config.StoreLogs {
				return
			}
			tablename += "_" + LOG_FIELD_KEY + "s"
		}
		_, err := create_cluster_table(tablename, s.handle)
		// myid := int64(-1)
		// s.last_lock.Lock()
		// if _, ok := s.last_rowids[tablename]; !ok {
		// 	last_id, err := create_cluster_table(tablename, s.handle)
		// 	if err != nil {
		// 		return
		// 	}
		// 	s.last_rowids[tablename] = sqlStorageTableID{
		// 		Last: last_id,
		// 		Lock: &sync.Mutex{},
		// 	}
		// }
		// x := s.last_rowids[tablename]
		// x.Last++
		// s.last_rowids[tablename] = x
		// myid = x.Last
		// s.last_lock.Unlock()

		// if myid < 0 {
		// 	cclog.ComponentError(s.name, "Last ID of table", tablename, "not set")
		// 	return
		// }

		cols := make([]string, 0)
		args := make([]interface{}, 0)
		//cols = append(cols, "id", "timestamp", "name")
		cols = append(cols, "timestamp", "name")
		//args = append(args, myid, msg.Time().Unix(), msg.Name())
		args = append(args, msg.Time().Unix(), msg.Name())
		//cclog.ComponentDebug(s.name, fmt.Sprintf("Write: add %s -> %v", "id", myid))
		cclog.ComponentDebug(s.name, fmt.Sprintf("Write: add %s -> %v", "timestamp", msg.Time().Unix()))
		cclog.ComponentDebug(s.name, fmt.Sprintf("Write: add %s -> %v", "name", msg.Name()))
		if h, ok := msg.GetTag(HOSTNAME_TAG_KEY); ok {
			cols = append(cols, MetricToSchema[HOSTNAME_TAG_KEY])
			args = append(args, h)
			cclog.ComponentDebug(s.name, fmt.Sprintf("Write: add %s -> %v", MetricToSchema[HOSTNAME_TAG_KEY], h))
		} else {
			return
		}
		if t, ok := msg.GetTag(TYPE_TAG_KEY); ok {
			cols = append(cols, MetricToSchema[TYPE_TAG_KEY])
			args = append(args, t)
			cclog.ComponentDebug(s.name, fmt.Sprintf("Write: add %s -> %v", MetricToSchema[TYPE_TAG_KEY], t))
			if t != "node" {
				requires_tid = true
			}
		} else {
			cclog.ComponentError(s.name, fmt.Sprintf("Message has no %s tag", TYPE_TAG_KEY))
			return
		}
		if requires_tid {
			if t, ok := msg.GetTag(TYPEID_TAG_KEY); ok {
				cols = append(cols, MetricToSchema[TYPEID_TAG_KEY])
				args = append(args, t)
				cclog.ComponentDebug(s.name, fmt.Sprintf("Write: add %s -> %v", MetricToSchema[TYPEID_TAG_KEY], t))
			} else {
				cclog.ComponentError(s.name, fmt.Sprintf("Message has no %s tag", TYPEID_TAG_KEY))
				return
			}
		}
		if t, ok := msg.GetTag(STYPE_TAG_KEY); ok {
			cols = append(cols, MetricToSchema[STYPE_TAG_KEY])
			args = append(args, t)
			cclog.ComponentDebug(s.name, fmt.Sprintf("Write: add %s -> %v", MetricToSchema[STYPE_TAG_KEY], t))
		}
		if t, ok := msg.GetTag(STYPEID_TAG_KEY); ok {
			cols = append(cols, MetricToSchema[STYPEID_TAG_KEY])
			args = append(args, t)
			cclog.ComponentDebug(s.name, fmt.Sprintf("Write: add %s -> %v", "stypeid", t))
		} else if msg.HasTag(STYPE_TAG_KEY) {
			cclog.ComponentError(s.name, fmt.Sprintf("Message has no %s tag but a %s tag", MetricToSchema[STYPEID_TAG_KEY], STYPE_TAG_KEY))
			return
		}
		if e, ok := msg.GetField(EVENT_FIELD_KEY); ok {
			cols = append(cols, MetricToSchema[EVENT_FIELD_KEY])
			args = append(args, e)
			cclog.ComponentDebug(s.name, fmt.Sprintf("Write: add %s -> %v", MetricToSchema[EVENT_FIELD_KEY], e))
		} else if l, ok := msg.GetField(LOG_FIELD_KEY); ok {
			cols = append(cols, MetricToSchema[LOG_FIELD_KEY])
			args = append(args, l)
			cclog.ComponentDebug(s.name, fmt.Sprintf("Write: add %s -> %v", MetricToSchema[LOG_FIELD_KEY], l))
		} else {
			cclog.ComponentError(s.name, fmt.Sprintf("Message has neither %s nor %s field", EVENT_FIELD_KEY, LOG_FIELD_KEY))
		}
		tags := make(otherTagsType, 0)
		for k, v := range msg.Tags() {
			if _, ok := MetricToSchema[k]; !ok {
				tags[k] = v
			}
		}
		if len(tags) > 0 {
			d, err := json.Marshal(tags)
			if err == nil {
				cols = append(cols, "othertags")
				args = append(args, d)
				cclog.ComponentDebug(s.name, fmt.Sprintf("Write: add %s -> %v", "othertags", string(d)))
			}
		}
		myinsert := sq.Insert(tablename).Columns(cols...).Values(args...)
		res, err := myinsert.RunWith(s.handle).Exec()
		if err != nil {
			cclog.ComponentError(s.name, err.Error())
			return
		}
		added, err := res.RowsAffected()
		if err != nil {
			cclog.ComponentError(s.name, err.Error())
			return
		}
		cclog.ComponentDebug(s.name, "Insert into ", tablename, "successful:", added, "rows")
	}
}

func (s *sqlStorage) Start() {
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
				s.Write(e)
			}
		}
	}()
	sched, err := gocron.NewScheduler()
	if err != nil {
		cclog.ComponentError(s.name, "failed to initialize gocron scheduler")
		return
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
				before := time.Now().Add(-s.retention)
				err := s.delete(before.Unix())
				if err != nil {
					cclog.ComponentError(s.name)
				}
			},
		),
	)
	s.started = true
	cclog.ComponentDebug(s.name, "STARTED")
}

func (s *sqlStorage) Close() {
	if s.started {
		s.done <- struct{}{}
		<-s.done
		if s.scheduler != nil {
			s.scheduler.Shutdown()
		}
	}
}

func (s *sqlStorage) SetInput(input chan lp.CCMetric) {
	s.input = input
}
