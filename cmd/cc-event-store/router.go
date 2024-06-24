package main

import (
	"errors"
	"sync"

	cclog "github.com/ClusterCockpit/cc-metric-collector/pkg/ccLogger"
	lp "github.com/ClusterCockpit/cc-metric-collector/pkg/ccMetric"
)

type router struct {
	input      chan lp.CCMetric
	output     chan lp.CCMetric
	done       chan bool
	wg         *sync.WaitGroup
	maxForward int
}

type Router interface {
	Start() error
	Close()
	SetInput(input chan lp.CCMetric)
	SetOutput(output chan lp.CCMetric)
}

func (r *router) Start() error {
	cclog.ComponentDebug("Router", "START")
	r.wg.Add(1)
	if r.input != nil && r.output != nil {
		go func() {
			for {
				select {
				case <-r.done:
					r.wg.Done()
					cclog.ComponentDebug("Router", "DONE")
					return
				case e := <-r.input:
					cclog.ComponentDebug("Router", "FORWARD", e)
					r.output <- e
					for i := 0; i < len(r.input) && i < r.maxForward; i++ {
						r.output <- e
					}
				}
			}
		}()
	} else {
		return errors.New("either input or output channel is not set")
	}
	cclog.ComponentDebug("Router", "STARTED")
	return nil
}
func (r *router) Close() {
	cclog.ComponentDebug("Router", "CLOSE")
	r.done <- true
}
func (r *router) SetInput(input chan lp.CCMetric) {
	r.input = input
}
func (r *router) SetOutput(output chan lp.CCMetric) {
	r.output = output
}

func NewRouter(wg *sync.WaitGroup) (Router, error) {
	r := new(router)
	r.maxForward = 10
	r.done = make(chan bool)
	r.wg = wg
	return r, nil
}
