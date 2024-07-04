// Copyright (C) NHR@FAU, University Erlangen-Nuremberg.
// All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.
package main

import (
	"fmt"
	"time"

	lp "github.com/ClusterCockpit/cc-energy-manager/pkg/cc-message"
	cclog "github.com/ClusterCockpit/cc-metric-collector/pkg/ccLogger"
	"github.com/nats-io/nats.go"
)

func main() {
	subject := "testcluster"
	cluster := "testcluster"
	events := make([]lp.CCMessage, 0)

	for i := 0; i < 10; i++ {
		tags := map[string]string{
			"hostname": "nuc",
			"cluster":  cluster,
			"type":     "socket",
			"type-id":  fmt.Sprintf("%d", i%2),
		}
		meta := map[string]string{}
		event := fmt.Sprintf("event %d happened", i)
		e, err := lp.NewEvent("slurm", tags, meta, event, time.Now())
		if err != nil {
			fmt.Printf("Failed to generate event %d: %v", i, err.Error())
		}
		events = append(events, e)
	}

	conn, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		cclog.Error(err.Error())
		return
	}
	fmt.Println("Connected")

	for _, e := range events {
		fmt.Println("Publish ", e)
		conn.Publish(subject, []byte(e.ToLineProtocol(nil)))
	}
	time.Sleep(1 * time.Second)
	fmt.Println("Closing")
	conn.Close()
}
