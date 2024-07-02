package storage2

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	lp "github.com/ClusterCockpit/cc-energy-manager/pkg/cc-message"
	cclog "github.com/ClusterCockpit/cc-metric-collector/pkg/ccLogger"
	sq "github.com/Masterminds/squirrel"
)

const createTableStmt = `create table IF NOT EXISTS %s (id integer primary key, timestamp bigint, name text, hostname text, type text, typeid text, stype text, stypeid text, othertags JSON, field text);`

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
	EVENT_FIELD_KEY:  "field",
	LOG_FIELD_KEY:    "field",
	HOSTNAME_TAG_KEY: "hostname",
	TYPEID_TAG_KEY:   "typeid",
	STYPEID_TAG_KEY:  "stypeid",
	TYPE_TAG_KEY:     "type",
	STYPE_TAG_KEY:    "stype",
	CLUSTER_TAG_KEY:  "",
}

type sqlStorage struct {
	storage
	handle     *sql.DB
	tablesLock sync.RWMutex
	tablesMap  map[string]struct{}
}

type SqlStorage interface {
	Storage
}

func (s *sqlStorage) PreInit(wg *sync.WaitGroup, config json.RawMessage) error {
	s.tablesMap = make(map[string]struct{})
	return nil
}

func (s *sqlStorage) PostInit(wg *sync.WaitGroup, config json.RawMessage) error {
	return nil
}

func (s *sqlStorage) Init(wg *sync.WaitGroup, config json.RawMessage) error {
	s.name = "SQLStorage"
	s.handle = nil
	cclog.ComponentDebug(s.name, "Init")
	s.PostInit(wg, config)
	return s.PostInit(wg, config)
}

func (s *sqlStorage) Query(request QueryRequest) (QueryResult, error) {
	if s.handle == nil {
		return QueryResult{}, errors.New("cannot query, not initialized")
	}
	tablename := request.Cluster
	if request.QueryType == QueryTypeEvent {
		tablename += "_" + EVENT_FIELD_KEY + "s"
	} else if request.QueryType == QueryTypeLog {
		tablename += "_" + LOG_FIELD_KEY + "s"
	} else {
		return QueryResult{}, errors.New("unknown query type in request")
	}
	cclog.ComponentDebug(s.name, "Query for table", tablename)
	qstmt := sq.Select("timestamp", "field").
		From(tablename).
		Where(sq.Eq{MetricToSchema[HOSTNAME_TAG_KEY]: request.Hostname}).
		Where(sq.Eq{"name": request.Event}).
		Where(sq.LtOrEq{"timestamp": request.To}).
		Where(sq.GtOrEq{"timestamp": request.From})
	if len(request.Conditions) > 0 {
		cclog.ComponentDebug(s.name, "Adding query condition", tablename)
		for _, c := range request.Conditions {
			pred := c.Pred
			if _, ok := MetricToSchema[pred]; !ok {
				pred = fmt.Sprintf("othertags->>'$.%s'", pred)
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
			}
		}
	}
	str, args, err := qstmt.ToSql()
	if err == nil {
		cclog.ComponentDebug(s.name, "Query", str, "with", args)
	}
	cclog.ComponentDebug(s.name, "Run Query")
	res, err := qstmt.RunWith(s.handle).Query()
	if err != nil {
		cclog.ComponentError(s.name, "Error running query:", err.Error())
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

func (s *sqlStorage) CreateTable(tablename string) error {
	stmt := fmt.Sprintf(createTableStmt, tablename)
	cclog.ComponentDebug(s.name, "Creating table ", tablename)
	_, err := s.handle.Exec(stmt)
	if err != nil {
		cclog.Error(fmt.Sprintf("%q: %s", err, stmt))
		return err
	}
	return nil
}

func (s *sqlStorage) Write(msgs []*lp.CCMessage) error {
	cclog.ComponentDebug(s.name, "Write", s.handle)
	if s.handle == nil {
		return errors.New("cannot write, not initialized")
	}
	cclog.ComponentDebug(s.name, "Write")
	othertags := make(map[string]string)
	colargs := make(map[string]interface{})

	error_no_cluster_tag := 0
	error_create_table := 0
	error_decode_othertags := 0
	error_exec := 0
	error_sql := 0
	error_sql_prepare := 0

	tx, err := s.handle.Begin()
	if err != nil {
		cclog.ComponentError(s.name, err.Error())
		return err
	}
	defer tx.Rollback()

	for _, msg := range msgs {
		if msg == nil || *msg == nil {
			continue
		}
		clear(othertags)
		clear(colargs)
		cclog.ComponentDebug(s.name, *msg)
		if _, ok := (*msg).GetTag(CLUSTER_TAG_KEY); !ok {
			error_no_cluster_tag++
			continue
		}
		cluster, _ := (*msg).GetTag(CLUSTER_TAG_KEY)
		tablename := cluster
		if lp.IsEvent(*msg) {
			tablename += "_" + EVENT_FIELD_KEY + "s"
			if x, ok := (*msg).GetField(EVENT_FIELD_KEY); ok {
				colargs[MetricToSchema[EVENT_FIELD_KEY]] = x
			}
		} else if lp.IsLog(*msg) {
			tablename += "_" + LOG_FIELD_KEY + "s"
			if x, ok := (*msg).GetField(LOG_FIELD_KEY); ok {
				colargs[MetricToSchema[LOG_FIELD_KEY]] = x
			}
		}
		s.tablesLock.RLock()
		if _, ok := s.tablesMap[tablename]; !ok {
			err := s.CreateTable(tablename)
			if err != nil {
				error_create_table++
				s.tablesLock.RUnlock()
				continue
			}
			s.tablesLock.RUnlock()
			s.tablesLock.Lock()
			s.tablesMap[tablename] = struct{}{}
			s.tablesLock.Unlock()
		} else {
			s.tablesLock.RUnlock()
		}

		colargs["timestamp"] = (*msg).Time().Unix()
		colargs["name"] = (*msg).Name()

		for k, v := range (*msg).Tags() {
			if colname, ok := MetricToSchema[k]; ok {
				// Filters out all tags with
				// MetricToSchema[tag key] = ""
				if len(colname) > 0 {
					colargs[colname] = v
				}
			} else {
				othertags[k] = v
			}
		}
		if len(othertags) > 0 {
			x, err := json.Marshal(othertags)
			if err != nil {
				error_decode_othertags++
			} else {
				colargs["othertags"] = x
			}
		}

		isql, args, err := sq.Insert(tablename).SetMap(colargs).ToSql()
		if err != nil {
			cclog.ComponentError(s.name, "Cannot get SQL of insert statement")
			error_sql++
			continue
		}

		// Not sure whether preparing makes sense here because we use and forget the
		// SQL insert statement since we do not know whether the next CCMessage
		// is for the same table and has the same amount and kind of tags/fields.

		// stmt, err := tx.Prepare(isql)
		// if err != nil {
		// 	cclog.ComponentError(s.name, err.Error())
		// 	error_sql_prepare++
		// 	continue
		// }
		// defer stmt.Close()
		// _, err = stmt.Exec(args...)
		cclog.ComponentDebug(s.name, isql)
		_, err = tx.Exec(isql, args...)
		if err != nil {
			error_exec++
		}
	}
	if error_no_cluster_tag+error_create_table+error_decode_othertags+error_exec > 0 {
		if error_no_cluster_tag > 0 {
			cclog.ComponentError(s.name, error_no_cluster_tag, "message had no cluster tag")
		}
		if error_create_table > 0 {
			cclog.ComponentError(s.name, error_create_table, "tables failed to create")
		}
		if error_decode_othertags > 0 {
			cclog.ComponentError(s.name, error_decode_othertags, "times failed to create othertags")
		}
		if error_exec > 0 {
			cclog.ComponentError(s.name, error_exec, "times the execution of the insert statement failed")
		}
		if error_sql > 0 {
			cclog.ComponentError(s.name, error_sql, "times the insert statement was invalid")
		}
		if error_sql_prepare > 0 {
			cclog.ComponentError(s.name, error_sql_prepare, "times the preparation of the insert statement failed")
		}
	}

	err = tx.Commit()
	if err != nil {
		cclog.ComponentError(s.name, err.Error())
		return err
	}

	return nil
}
func (s *sqlStorage) Delete(to int64) error {
	ecount := 0
	s.tablesLock.RLock()
	for tab := range s.tablesMap {
		isql, iargs, err := sq.Delete(tab).Where(sq.LtOrEq{"timestamp": to}).ToSql()
		if err == nil {
			_, err = s.handle.Exec(isql, iargs...)
			if err != nil {
				ecount++
			}
		}
	}
	if ecount > 0 {
		return fmt.Errorf("failed to delete messages older than %d from %d tables", to, ecount)
	}
	return nil
}
func (s *sqlStorage) Close() {
	if s.handle != nil {
		s.handle.Close()
	}
}
