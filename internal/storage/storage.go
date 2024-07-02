package storage2

import (
	"encoding/json"
	"sync"

	lp "github.com/ClusterCockpit/cc-energy-manager/pkg/cc-message"
)

type storage struct {
	name string
	uri  string
}

type Storage interface {
	Init(wg *sync.WaitGroup, config json.RawMessage) error
	Query(request QueryRequest) (QueryResult, error)
	Write(msg []*lp.CCMessage) error
	Delete(to int64) error
	Close()
}
