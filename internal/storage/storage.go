// Copyright (C) NHR@FAU, University Erlangen-Nuremberg.
// All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.
package storage

import (
	"encoding/json"

	lp "github.com/ClusterCockpit/cc-energy-manager/pkg/cc-message"
)

type storage struct {
	stats *storageStats
	name  string
	uri   string
}

type Storage interface {
	Init(config json.RawMessage, stats *storageStats) error
	Query(request QueryRequest) (QueryResult, error)
	Write(msg []lp.CCMessage) error
	Delete(to int64) error
	Close()
}
