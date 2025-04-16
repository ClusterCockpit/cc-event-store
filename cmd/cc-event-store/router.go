// Copyright (C) NHR@FAU, University Erlangen-Nuremberg.
// All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.
package main

import (
	"errors"
	"sync"

	cclog "github.com/ClusterCockpit/cc-lib/ccLogger"
	lp "github.com/ClusterCockpit/cc-lib/ccMessage"
)

type router struct {
	input      chan lp.CCMessage
	output     chan lp.CCMessage
	done       chan bool
	wg         *sync.WaitGroup
	maxForward int
}

type Router interface {
	Start() error
	Close()
	SetInput(input chan lp.CCMessage)
	SetOutput(output chan lp.CCMessage)
}

func (r *router) Start() error {
	cclog.ComponentDebug("Router", "START")
	empty_meta := make(map[string]string)

	toCCMessage := func(msg lp.CCMessage) lp.CCMessage {
		x, err := lp.NewMessage(msg.Name(), msg.Tags(), empty_meta, msg.Fields(), msg.Time())
		if err != nil {
			return nil
		}
		return x
	}

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
					r.output <- toCCMessage(e)
					for i := 0; i < len(r.input) && i < r.maxForward; i++ {
						r.output <- toCCMessage(<-r.input)
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

func (r *router) SetInput(input chan lp.CCMessage) {
	r.input = input
}

func (r *router) SetOutput(output chan lp.CCMessage) {
	r.output = output
}

func NewRouter(wg *sync.WaitGroup) (Router, error) {
	r := new(router)
	r.maxForward = 10
	r.done = make(chan bool)
	r.wg = wg
	return r, nil
}
