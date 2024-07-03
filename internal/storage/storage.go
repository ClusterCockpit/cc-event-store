package storage2

import (
	"encoding/json"

	lp "github.com/ClusterCockpit/cc-energy-manager/pkg/cc-message"
)

type storage struct {
	name  string
	uri   string
	stats *storageStats
}

type Storage interface {
	Init(config json.RawMessage, stats *storageStats) error
	Query(request QueryRequest) (QueryResult, error)
	Write(msg []*lp.CCMessage) error
	Delete(to int64) error
	Close()
}
