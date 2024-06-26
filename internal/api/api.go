package api

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	cclog "github.com/ClusterCockpit/cc-metric-collector/pkg/ccLogger"
	lp "github.com/ClusterCockpit/cc-metric-collector/pkg/ccMetric"
	influx "github.com/influxdata/line-protocol/v2/lineprotocol"
)

// @title                      cc-event-store REST API
// @version                    1.0.0
// @description                API for cc-event-store

// @contact.name               ClusterCockpit Project
// @contact.url                https://clustercockpit.org
// @contact.email              support@clustercockpit.org

// @license.name               MIT License
// @license.url                https://opensource.org/licenses/MIT

// @host                       localhost:8098
// @basePath                   /api/

// @securityDefinitions.apikey ApiKeyAuth
// @in                         header
// @name                       X-Auth-Token
//
// ErrorResponse model
type ErrorResponse struct {
	// Statustext of Errorcode
	Status string `json:"status"`
	Error  string `json:"error"` // Error Message
}

func handleError(err error, statusCode int, rw http.ResponseWriter) {
	// log.Warnf("REST ERROR : %s", err.Error())
	rw.Header().Add("Content-Type", "application/json")
	rw.WriteHeader(statusCode)
	json.NewEncoder(rw).Encode(ErrorResponse{
		Status: http.StatusText(statusCode),
		Error:  err.Error(),
	})
}

// handleQuery godoc
// @summary    Query events
// @tags query
// @description Query events.
// @accept      json
// @produce     json
// @param       request body     api.ApiQueryRequest  true "API query payload object"
// @success     200            {object} api.ApiQueryResponse  "API query response object"
// @failure     400            {object} api.ErrorResponse       "Bad Request"
// @failure     401   		   {object} api.ErrorResponse       "Unauthorized"
// @failure     500            {object} api.ErrorResponse       "Internal Server Error"
// @security    ApiKeyAuth
// @router      /query/ [get]
func (a *api) HandleQuery(w http.ResponseWriter, r *http.Request) {
	cclog.ComponentDebug("REST", "HandleQuery")
	// if r.Method != http.MethodGet {
	// 	msg := "Only GET method allowed"
	// 	http.Error(w, msg, http.StatusMethodNotAllowed)
	// 	cclog.ComponentError("REST", msg)
	// 	return
	// }
	// Check basic authentication

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
		handleError(err, http.StatusBadRequest, w)
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
				handleError(err, http.StatusBadRequest, w)
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
		handleError(err, http.StatusInternalServerError, w)
		return
	}
}

// handleWrite godoc
// @summary    Receive events
// @tags write
// @description Receive events in line-protocol
// @accept      plain
// @produce     json
// @param       cluster      path     string                   true "Cluster to store events"
// @success     200            {string} string  "ok"
// @failure     400            {object} api.ErrorResponse       "Bad Request"
// @failure     401      {object} api.ErrorResponse       "Unauthorized"
// @failure     500            {object} api.ErrorResponse       "Internal Server Error"
// @security    ApiKeyAuth
// @router      /write/{cluster}/ [post]
func (a *api) HandleWrite(w http.ResponseWriter, r *http.Request) {
	cclog.ComponentDebug("REST", "HandleWrite")

	cluster := r.PathValue("cluster")

	d := influx.NewDecoder(r.Body)
	for d.Next() {

		// Decode measurement name
		measurement, err := d.Measurement()
		if err != nil {
			msg := "HandleWrite: Failed to decode measurement: " + err.Error()
			cclog.ComponentError("REST", msg)
			handleError(err, http.StatusBadRequest, w)
			return
		}

		// Decode tags
		tags := make(map[string]string)
		for {
			key, value, err := d.NextTag()
			if err != nil {
				msg := "HandleWrite: Failed to decode tag: " + err.Error()
				cclog.ComponentError("REST", msg)
				handleError(err, http.StatusInternalServerError, w)
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
				handleError(err, http.StatusInternalServerError, w)
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
			handleError(err, http.StatusInternalServerError, w)
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
		handleError(err, http.StatusInternalServerError, w)
		return
	}

	w.WriteHeader(http.StatusOK)
}
