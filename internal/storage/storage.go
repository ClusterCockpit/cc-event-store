package storage

import (
	"sync"

	lp "github.com/ClusterCockpit/cc-metric-collector/pkg/ccMetric"
)

type storageConfig struct {
	Type string `json:"type"`
}

type storage struct {
	name        string
	wg          *sync.WaitGroup
	done        chan struct{}
	input       chan lp.CCMetric
	json_access string
	started     bool
}

type Storage interface {
	Close()
	Start()
	Init(wg *sync.WaitGroup, configfile string) error
	SetInput(input chan lp.CCMetric)
	Query(request QueryRequest) (QueryResult, error)
	Write(msg lp.CCMetric)
}
