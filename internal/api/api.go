package api

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/ClusterCockpit/cc-event-store/internal/storage"
	cclog "github.com/ClusterCockpit/cc-metric-collector/pkg/ccLogger"
	lp "github.com/ClusterCockpit/cc-metric-collector/pkg/ccMetric"
	"github.com/gorilla/mux"
	influx "github.com/influxdata/line-protocol/v2/lineprotocol"
)

type apiConfig struct {
	Addr string `json:"address"`
	Port string `json:"port"`

	// Maximum amount of time to wait for the next request when keep-alives are enabled
	// should be larger than the measurement interval to keep the connection open
	IdleTimeout string `json:"idle_timeout"`
	idleTimeout time.Duration

	// Controls whether HTTP keep-alives are enabled. By default, keep-alives are enabled
	KeepAlivesEnabled bool `json:"keep_alives_enabled"`

	// Basic authentication
	Username     string `json:"username"`
	Password     string `json:"password"`
	useBasicAuth bool
}

type api struct {
	wg      *sync.WaitGroup
	started bool
	server  *http.Server
	store   storage.StorageManager
	config  apiConfig
}

type API interface {
	Init(wg *sync.WaitGroup, store storage.StorageManager, apiConfigFile string) error
	Start()
	Close()
}

type ApiQueryRequest struct {
	Cluster     string     `json:"cluster"`
	Queries     []ApiQuery `json:"queries"`
	ForAllNodes []string   `json:"for-all-nodes"`
	From        int64      `json:"from"`
	To          int64      `json:"to"`
}

type ApiQuery struct {
	Type       *string  `json:"type,omitempty"`
	SubType    *string  `json:"subtype,omitempty"`
	Event      string   `json:"event"`
	Hostname   string   `json:"host"`
	TypeIds    []string `json:"type-ids,omitempty"`
	SubTypeIds []string `json:"subtype-ids,omitempty"`
}

type ApiQueryResponse struct {
	Queries []ApiQuery        `json:"queries,omitempty"`
	Results [][]ApiMetricData `json:"results"`
}

type ApiMetricData struct {
	Error *string  `json:"error,omitempty"`
	Data  []string `json:"data,omitempty"`
	From  int64    `json:"from"`
	To    int64    `json:"to"`
}

func (a *api) HandleQuery(w http.ResponseWriter, r *http.Request) {
	cclog.ComponentDebug("REST", "HandleQuery")
	// if r.Method != http.MethodGet {
	// 	msg := "Only GET method allowed"
	// 	http.Error(w, msg, http.StatusMethodNotAllowed)
	// 	cclog.ComponentError("REST", msg)
	// 	return
	// }
	// Check basic authentication
	if a.config.useBasicAuth {
		cclog.ComponentDebug("REST", "HandleQuery (BasicAuth)")
		username, password, ok := r.BasicAuth()
		if !ok || username != a.config.Username || password != a.config.Password {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
	}

	// vars := mux.Vars(r)
	// cclog.ComponentDebug("URL parameters:", vars)
	// if _, ok := vars["cluster"]; !ok {
	// 	cclog.ComponentDebug("REST", "HandleWrite (BasicAuth)")
	// 	msg := "HandleWrite: No cluster given as URL parameter"
	// 	cclog.ComponentError("REST", msg)
	// 	http.Error(w, msg, http.StatusBadRequest)
	// 	return
	// }
	// cluster := vars["cluster"]

	var req ApiQueryRequest
	jsonParser := json.NewDecoder(r.Body)
	err := jsonParser.Decode(&req)
	if err != nil {
		err = fmt.Errorf("failed to parse API request: %v", err.Error())
		cclog.ComponentError("REST", err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	resp := ApiQueryResponse{
		Results: make([][]ApiMetricData, 0, len(req.Queries)),
	}
	for i, q := range req.Queries {
		resp.Results[i] = make([]ApiMetricData, 0, len(q.TypeIds))
		for j, t := range q.TypeIds {

			conditions := make([]string, 0)
			conditions = append(conditions, fmt.Sprintf("type-id == %s", t))
			if len(*q.Type) > 0 {
				conditions = append(conditions, fmt.Sprintf("type == %s", *q.Type))
			}

			res, err := a.store.Query(req.Cluster, q.Event, q.Hostname, req.From, req.To, conditions)
			if err != nil {
				err = fmt.Errorf("failed to parse API request: %v", err.Error())
				cclog.ComponentError("REST", err.Error())
				return
			}
			resp.Results[i][j] = ApiMetricData{
				Data: res,
			}
		}
	}

	w.Header().Set("Content-Type", "application/json")
	bw := bufio.NewWriter(w)
	defer bw.Flush()
	if err := json.NewEncoder(bw).Encode(resp); err != nil {
		err = fmt.Errorf("failed to encode API response: %v", err.Error())
		cclog.ComponentError("REST", err.Error())
		return
	}

}

func (a *api) HandleWrite(w http.ResponseWriter, r *http.Request) {
	cclog.ComponentDebug("REST", "HandleWrite")

	if r.Method != http.MethodPost {
		msg := "Only POST method allowed"
		http.Error(w, msg, http.StatusMethodNotAllowed)
		cclog.ComponentError("REST", msg)
		return
	}

	// Check basic authentication
	if a.config.useBasicAuth {
		cclog.ComponentDebug("REST", "HandleWrite (BasicAuth)")
		username, password, ok := r.BasicAuth()
		if !ok || username != a.config.Username || password != a.config.Password {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
	}

	vars := mux.Vars(r)
	cclog.ComponentDebug("URL parameters:", vars)
	if _, ok := vars["cluster"]; !ok {
		cclog.ComponentDebug("REST", "HandleWrite (BasicAuth)")
		msg := "HandleWrite: No cluster given as URL parameter"
		cclog.ComponentError("REST", msg)
		http.Error(w, msg, http.StatusBadRequest)
		return
	}
	cluster := vars["cluster"]

	d := influx.NewDecoder(r.Body)
	for d.Next() {

		// Decode measurement name
		measurement, err := d.Measurement()
		if err != nil {
			msg := "HandleWrite: Failed to decode measurement: " + err.Error()
			cclog.ComponentError("REST", msg)
			http.Error(w, msg, http.StatusInternalServerError)
			return
		}

		// Decode tags
		tags := make(map[string]string)
		for {
			key, value, err := d.NextTag()
			if err != nil {
				msg := "HandleWrite: Failed to decode tag: " + err.Error()
				cclog.ComponentError("REST", msg)
				http.Error(w, msg, http.StatusInternalServerError)
				return
			}
			if key == nil {
				break
			}
			tags[string(key)] = string(value)
		}

		// Decode fields
		fields := make(map[string]interface{})
		for {
			key, value, err := d.NextField()
			if err != nil {
				msg := "HandleWrite: Failed to decode field: " + err.Error()
				cclog.ComponentError("REST", msg)
				http.Error(w, msg, http.StatusInternalServerError)
				return
			}
			if key == nil {
				break
			}
			fields[string(key)] = value.Interface()
		}

		// Decode time stamp
		t, err := d.Time(influx.Nanosecond, time.Time{})
		if err != nil {
			msg := "HandleWrite: Failed to decode time stamp: " + err.Error()
			cclog.ComponentError("REST", msg)
			http.Error(w, msg, http.StatusInternalServerError)
			return
		}

		y, _ := lp.New(
			string(measurement),
			tags,
			map[string]string{},
			fields,
			t,
		)

		a.store.Submit(cluster, y)
	}
	err := d.Err()
	if err != nil {
		msg := "HandleWrite: Failed to decode: " + err.Error()
		cclog.ComponentError("REST", msg)
		http.Error(w, msg, http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (a *api) Init(wg *sync.WaitGroup, store storage.StorageManager, apiConfigFile string) error {
	a.wg = wg
	a.store = store
	a.started = false

	a.config.KeepAlivesEnabled = true
	// should be larger than the measurement interval to keep the connection open
	a.config.IdleTimeout = "120s"
	a.config.useBasicAuth = false

	// Read config
	configFile, err := os.Open(apiConfigFile)
	if err != nil {
		cclog.Error(err.Error())
		return err
	}
	defer configFile.Close()
	jsonParser := json.NewDecoder(configFile)
	err = jsonParser.Decode(&a.config)
	if err != nil {
		err = fmt.Errorf("failed to parse API config file %s: %v", apiConfigFile, err.Error())
		cclog.ComponentError("REST", err.Error())
		return err
	}
	r := mux.NewRouter()
	r.HandleFunc("/write", a.HandleWrite).Queries("cluster", "{cluster}")
	r.HandleFunc("/query", a.HandleQuery)
	// Create http server
	addr := fmt.Sprintf("%s:%s", a.config.Addr, a.config.Port)
	a.server = &http.Server{
		Addr:        addr,
		Handler:     r, // handler to invoke, http.DefaultServeMux if nil
		IdleTimeout: a.config.idleTimeout,
	}
	a.server.SetKeepAlivesEnabled(a.config.KeepAlivesEnabled)
	cclog.ComponentDebug("REST", "Initialized REST API to listen at", addr)
	return nil
}

func (a *api) Start() {
	a.wg.Add(1)
	go func() {
		err := a.server.ListenAndServe()
		if err != nil && err.Error() != "http: Server closed" {
			cclog.ComponentError("REST", err.Error())
		}
		a.wg.Done()
		cclog.ComponentDebug("REST", "DONE")
	}()
	a.started = true
	cclog.ComponentDebug("REST", "STARTED")
}

func (a *api) Close() {
	if a.started {
		cclog.ComponentDebug("REST", "CLOSE")
		a.server.Shutdown(context.Background())
	}
}

func NewAPI(wg *sync.WaitGroup, store storage.StorageManager, apiConfigFile string) (API, error) {
	a := new(api)

	err := a.Init(wg, store, apiConfigFile)
	if err != nil {
		err = fmt.Errorf("failed to create new API: %v", err.Error())
		cclog.ComponentError("REST", err.Error())
		return nil, err
	}
	return a, nil
}
