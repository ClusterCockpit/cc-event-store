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

func (s *sqlStorage) PreInit(config json.RawMessage, stats *storageStats) error {
	cclog.ComponentDebug(s.name, "PreInit")
	s.tablesMap = make(map[string]struct{})
	s.stats = stats
	return nil
}

func (s *sqlStorage) PostInit(config json.RawMessage) error {
	cclog.ComponentDebug(s.name, "PostInit")
	return nil
}

func (s *sqlStorage) Init(config json.RawMessage, stats *storageStats) error {
	s.name = "SQLStorage"
	s.handle = nil

	cclog.ComponentDebug(s.name, "Init")
	s.PreInit(config, stats)
	return s.PostInit(config)
}

func (s *sqlStorage) Query(request QueryRequest) (QueryResult, error) {
	if s.handle == nil {
		s.stats.UpdateError("not_initialized", 1)
		return QueryResult{}, errors.New("cannot query, not initialized")
	}
	tablename := request.Cluster
	if request.QueryType == QueryTypeEvent {
		tablename += "_" + EVENT_FIELD_KEY + "s"
	} else if request.QueryType == QueryTypeLog {
		tablename += "_" + LOG_FIELD_KEY + "s"
	} else {
		s.stats.UpdateError("query_unknown_type", 1)
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
		s.stats.UpdateError("query_exec_failed", 1)
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
			s.stats.UpdateError("query_scan_failed", 1)
			return out, err
		}
	}
	s.stats.UpdateStats("queries", 1)
	return out, nil
}

func (s *sqlStorage) CreateTable(tablename string) error {
	stmt := fmt.Sprintf(createTableStmt, tablename)
	cclog.ComponentDebug(s.name, "Creating table ", tablename)
	_, err := s.handle.Exec(stmt)
	if err != nil {
		cclog.Error(fmt.Sprintf("%q: %s", err, stmt))
		s.stats.UpdateError("create_table_failed", 1)
		return err
	}
	return nil
}

func (s *sqlStorage) Write(msgs []*lp.CCMessage) error {
	if s.handle == nil {
		s.stats.UpdateError("not_initialized", 1)
		return errors.New("cannot write, not initialized")
	}
	dbhandle := s.handle
	cclog.ComponentDebug(s.name, "Write")
	othertags := make(map[string]string)
	colargs := make(map[string]interface{})

	// Begin a new SQL transaction
	tx, err := dbhandle.Begin()
	if err != nil {
		cclog.ComponentError(s.name, err.Error())
		s.stats.UpdateError("write_sql_begin_failed", 1)
		return err
	}
	// Rollback is executed only if the Commit fails
	defer tx.Rollback()

	for _, msg := range msgs {
		if msg == nil || *msg == nil {
			continue
		}
		// Clear maps we reuse in this iteration
		clear(othertags)
		clear(colargs)
		// First we need the cluster name and take that from the
		// message itself. As a workaround we could get the hostlist
		// of each cluster from Cluster Cockpit and resolve it in
		// cases the message does not contain the 'cluster' tag
		if _, ok := (*msg).GetTag(CLUSTER_TAG_KEY); !ok {
			s.stats.UpdateError("write_no_cluster_tag", 1)
			continue
		}
		cluster, _ := (*msg).GetTag(CLUSTER_TAG_KEY)
		// Depending whether the table should handle CCEvent or
		// CCLog messages, we add it to the cluster name.
		// Moreover, we get the event or log string and add it
		// to the colargs map
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
		} else {
			s.stats.UpdateError("write_invalid_message_type", 1)
			continue
		}
		// Although each SQL backend using this function
		// gets a list of existing table from the database,
		// we manage an own lookup map. If there is no entry
		// in the map for the table, we create a new table
		// and add the table in the lookup map.
		// This table needs to be protected for lookup (read
		// lock) and adding (write lock).
		s.tablesLock.RLock()
		if _, ok := s.tablesMap[tablename]; !ok {
			err := s.CreateTable(tablename)
			if err != nil {
				s.stats.UpdateError("write_create_table_failed", 1)
				s.tablesLock.RUnlock()
				continue
			}
			s.tablesLock.RUnlock()
			s.tablesLock.Lock()
			// No futher check whether the table already exists
			s.tablesMap[tablename] = struct{}{}
			s.tablesLock.Unlock()
			s.stats.UpdateStats("active_tables", 1)
		} else {
			s.tablesLock.RUnlock()
		}

		// Add the info that is always present in a CCMessage
		colargs["timestamp"] = (*msg).Time().Unix()
		colargs["name"] = (*msg).Name()

		// Iterate over the tags and check which one should
		// be part of colargs and which are stored in the
		// JSON field 'othertags'
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
		// Add the 'othertags' as JSON if there are any
		if len(othertags) > 0 {
			x, err := json.Marshal(othertags)
			if err != nil {
				s.stats.UpdateError("write_decode_othertags_failed", 1)
			} else {
				colargs["othertags"] = x
			}
		}

		// Create the insert statement. Preserving the order of
		// columns and values is task of squirrel package
		isql, args, err := sq.Insert(tablename).SetMap(colargs).ToSql()
		if err != nil {
			cclog.ComponentError(s.name, "Cannot get SQL of insert statement")
			s.stats.UpdateError("write_sql_invalid", 1)
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
		//cclog.ComponentDebug(s.name, isql)

		// Execute the SQL statement with args inside the SQL transaction
		_, err = tx.Exec(isql, args...)
		if err != nil {
			s.stats.UpdateError("write_sql_exec_failed", 1)
		}
		s.stats.UpdateStats("writes", 1)
	}

	// Commit the transaction
	err = tx.Commit()
	if err != nil {
		cclog.ComponentError(s.name, err.Error())
		if _, ok := s.stats.Errors["write_sql_commit_failed"]; !ok {
			s.stats.Errors["write_sql_commit_failed"] = 0
		}
		s.stats.Errors["write_sql_commit_failed"] += 1
		return err
	}

	return nil
}
func (s *sqlStorage) Delete(to int64) error {
	ecount := 0
	event_tables := make([]string, 0)
	// Get all event tables. Log tables cannot be deleted like this
	// We acquire the read lock for the map to make it safe for
	// concurrent calls with Write().
	s.tablesLock.RLock()
	for tab := range s.tablesMap {
		if strings.Contains(tab, EVENT_FIELD_KEY) {
			event_tables = append(event_tables, tab)
		}
	}
	s.tablesLock.RUnlock()
	// Now delete all CCMessages before 'to' in the event tables
	for _, tab := range event_tables {
		isql, iargs, err := sq.Delete(tab).Where(sq.LtOrEq{"timestamp": to}).ToSql()
		if err == nil {
			_, err = s.handle.Exec(isql, iargs...)
			if err != nil {
				ecount++
			}
		}
	}
	if ecount > 0 {
		s.stats.lock.Lock()
		s.stats.UpdateError("delete_sql_exec_failed", int64(ecount))
		s.stats.lock.Unlock()
		return fmt.Errorf("failed to delete messages older than %d from %d out of %d event tables", to, ecount, len(event_tables))
	}
	s.stats.UpdateStats("deletes", 1)
	return nil
}
func (s *sqlStorage) Close() {
	if s.handle != nil {
		s.handle.Close()
	}
}
