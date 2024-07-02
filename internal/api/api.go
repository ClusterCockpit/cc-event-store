package api

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"time"

	lp "github.com/ClusterCockpit/cc-energy-manager/pkg/cc-message"
	storage "github.com/ClusterCockpit/cc-event-store/internal/storage"
	cclog "github.com/ClusterCockpit/cc-metric-collector/pkg/ccLogger"
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

	do_query := func(cluster, event, hostname string, from, to int64, conditions []storage.QueryCondition) (ApiMetricData, error) {
		request := storage.QueryRequest{
			Event:      event,
			To:         to,
			From:       from,
			Hostname:   hostname,
			Cluster:    cluster,
			QueryType:  storage.QueryTypeEvent,
			Conditions: conditions,
		}
		res, err := a.store.Query(request)
		if err != nil {
			err = fmt.Errorf("failed to parse API request: %v", err.Error())
			return ApiMetricData{}, err
		}
		if res.Error != nil {
			err = fmt.Errorf("failed to parse API request: %v", res.Error.Error())
			return ApiMetricData{}, err
		}
		d := ApiMetricData{
			Data: make([]ApiMetricDataEntry, 0, len(res.Results)),
		}
		for _, r := range res.Results {
			d.Data = append(d.Data, ApiMetricDataEntry{
				Event: r.Event,
				Time:  r.Timestamp,
			})
		}
		return d, nil
	}

	for qid, q := range req.Queries {
		for _, t := range q.TypeIds {

			conditions := make([]storage.QueryCondition, 0)
			conditions = append(conditions, storage.QueryCondition{
				Pred:      "type-id",
				Operation: "==",
				Args:      []interface{}{t},
			})
			if len(*q.Type) > 0 {
				conditions = append(conditions, storage.QueryCondition{
					Pred:      "type",
					Operation: "==",
					Args:      []interface{}{*q.Type},
				})
			}
			if len(q.SubTypeIds) > 0 {
				subconditions := slices.Clone(conditions)
				if len(*q.SubType) > 0 {
					subconditions = append(conditions, storage.QueryCondition{
						Pred:      "stype",
						Operation: "==",
						Args:      []interface{}{*q.SubType},
					})
				}
				for _, st := range q.SubTypeIds {
					subconditions = append(conditions, storage.QueryCondition{
						Pred:      "stype-id",
						Operation: "==",
						Args:      []interface{}{st},
					})
				}
				d, err := do_query(req.Cluster, q.Event, q.Hostname, req.From, req.To, subconditions)
				if err != nil {
					cclog.ComponentError("REST", err.Error())
					handleError(err, http.StatusBadRequest, w)
				}
				resp.Results[qid] = append(resp.Results[qid], d)

			} else {
				d, err := do_query(req.Cluster, q.Event, q.Hostname, req.From, req.To, conditions)
				if err != nil {
					cclog.ComponentError("REST", err.Error())
					handleError(err, http.StatusBadRequest, w)
				}
				resp.Results[qid] = append(resp.Results[qid], d)
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
// @param       cluster        query string true "If the lines in the body do not have a cluster tag, use this value instead."
// @success     200            {string} string  "ok"
// @failure     400            {object} api.ErrorResponse       "Bad Request"
// @failure     401      {object} api.ErrorResponse       "Unauthorized"
// @failure     500            {object} api.ErrorResponse       "Internal Server Error"
// @security    ApiKeyAuth
// @router      /write/ [post]
func (a *api) HandleWrite(w http.ResponseWriter, r *http.Request) {
	cclog.ComponentDebug("REST", "HandleWrite")

	cluster := r.URL.Query().Get("cluster")
	if cluster == "" {
		handleError(errors.New("query parameter cluster is required"), http.StatusBadRequest, w)
		return
	}

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

		y, _ := lp.NewMessage(
			string(measurement),
			tags,
			map[string]string{},
			fields,
			t,
		)

		ch := a.store.GetInput()
		if ch != nil {
			ch <- &y
		}
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

// HandleDelete godoc
// @summary    Delete events
// @tags free
// @description Delete events before given timestamp
// @accept      plain
// @produce     json
// @param       cluster        query string true "If the lines in the body do not have a cluster tag, use this value instead."
// @param       to        	   query string true "Delete events before this UNIX timestamp in seconds."
// @success     200            {string} string  "ok"
// @failure     400            {object} api.ErrorResponse       "Bad Request"
// @failure     401            {object} api.ErrorResponse       "Unauthorized"
// @failure     500            {object} api.ErrorResponse       "Internal Server Error"
// @security    ApiKeyAuth
// @router      /free/ [post]
// func (a *api) HandleDelete(w http.ResponseWriter, r *http.Request) {
// 	cclog.ComponentDebug("REST", "HandleDelete")
// 	cluster := r.URL.Query().Get("cluster")
// 	if cluster == "" {
// 		handleError(errors.New("query parameter cluster is required"), http.StatusBadRequest, w)
// 		return
// 	}
// 	to_string := r.URL.Query().Get("to")
// 	if cluster == "" {
// 		handleError(errors.New("query parameter to is required"), http.StatusBadRequest, w)
// 		return
// 	}

// 	to, err := strconv.ParseInt(to_string, 10, 64)
// 	if err != nil {
// 		msg := "HandleDelete: Failed to parse " + to_string + ": " + err.Error()
// 		cclog.ComponentError("REST", msg)
// 		handleError(err, http.StatusInternalServerError, w)
// 	}

// 	err = a.store.Delete(cluster, to)
// 	if err != nil {
// 		msg := fmt.Sprintf("HandleDelete: Failed to delete events for cluster %s before %d: %v", cluster, to, err.Error())
// 		cclog.ComponentError("REST", msg)
// 		handleError(err, http.StatusInternalServerError, w)
// 	}

// 	w.WriteHeader(http.StatusOK)
// }
