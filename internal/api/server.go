// Copyright (C) NHR@FAU, University Erlangen-Nuremberg.
// All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.
package api

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	storage "github.com/ClusterCockpit/cc-event-store/internal/storage"
	cclog "github.com/ClusterCockpit/cc-lib/ccLogger"
	httpSwagger "github.com/swaggo/http-swagger"
)

type apiConfig struct {
	Addr string `json:"address"`
	Port string `json:"port"`

	// Maximum amount of time to wait for the next request when keep-alives are enabled
	// should be larger than the measurement interval to keep the connection open
	IdleTimeout string `json:"idle_timeout,omitempty"`
	idleTimeout time.Duration

	// Controls whether HTTP keep-alives are enabled. By default, keep-alives are enabled
	KeepAlivesEnabled bool `json:"keep_alives_enabled"`

	// JWT token
	JwtPublicKey string `json:"jwt_public_key"`

	// Enable Swagger UI
	EnableSwaggerUI bool `json:"enable_swagger_ui"`
}

type api struct {
	store   storage.StorageManager
	wg      *sync.WaitGroup
	server  *http.Server
	config  apiConfig
	started bool
}

type API interface {
	Init(wg *sync.WaitGroup, store storage.StorageManager, rawConfig json.RawMessage) error
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
	Error *string              `json:"error,omitempty"`
	Data  []ApiMetricDataEntry `json:"data,omitempty"`
	From  int64                `json:"from"`
	To    int64                `json:"to"`
}
type ApiMetricDataEntry struct {
	Event string `json:"Event"`
	Time  int64  `json:"timestamp"`
}

func (a *api) Init(wg *sync.WaitGroup, store storage.StorageManager, rawConfig json.RawMessage) error {
	var err error
	a.wg = wg
	a.store = store
	a.started = false
	a.config.Addr = "localhost"
	a.config.Port = "8088"

	a.config.KeepAlivesEnabled = true
	// should be larger than the measurement interval to keep the connection open
	a.config.IdleTimeout = "120s"

	if err = json.Unmarshal(rawConfig, &a.config); err != nil {
		cclog.Warn("Error while unmarshaling raw config json")
		return err
	}

	r := http.NewServeMux()
	if len(a.config.JwtPublicKey) > 0 {
		buf, err := base64.StdEncoding.DecodeString(a.config.JwtPublicKey)
		if err != nil {
			// TODO: Ensure this is a fatal error
			cclog.ComponentError("REST", err.Error())
		}
		publicKey := ed25519.PublicKey(buf)
		r.Handle("POST /api/write/", authHandler(http.HandlerFunc(a.HandleWrite), publicKey))
		r.Handle("GET /api/query/", authHandler(http.HandlerFunc(a.HandleQuery), publicKey))
		r.Handle("GET /api/stats/", authHandler(http.HandlerFunc(a.HandleStats), publicKey))
		// r.Handle("GET /api/free/", authHandler(http.HandlerFunc(a.HandleDelete), publicKey))
	} else {
		r.HandleFunc("POST /api/write/", a.HandleWrite)
		r.HandleFunc("GET /api/query/", a.HandleQuery)
		r.HandleFunc("GET /api/stats/", a.HandleStats)
		// r.HandleFunc("GET /api/free/", a.HandleDelete)
	}

	addr := fmt.Sprintf("%s:%s", a.config.Addr, a.config.Port)
	if a.config.EnableSwaggerUI {
		cclog.Info("Enable Swagger UI")
		r.HandleFunc("GET /swagger/", httpSwagger.Handler(
			httpSwagger.URL("http://"+addr+"/swagger/doc.json")))
	}

	// Create http server
	a.server = &http.Server{
		Addr:        addr,
		Handler:     r,
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

func NewAPI(wg *sync.WaitGroup, store storage.StorageManager, rawConfig json.RawMessage) (API, error) {
	a := new(api)

	err := a.Init(wg, store, rawConfig)
	if err != nil {
		err = fmt.Errorf("failed to create new API: %v", err.Error())
		cclog.ComponentError("REST", err.Error())
		return nil, err
	}
	return a, nil
}
