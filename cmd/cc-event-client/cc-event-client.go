// Copyright (C) NHR@FAU, University Erlangen-Nuremberg.
// All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	lp "github.com/ClusterCockpit/cc-energy-manager/pkg/cc-message"
	cclog "github.com/ClusterCockpit/cc-metric-collector/pkg/ccLogger"
	"github.com/nats-io/nats.go"
)

const (
	DefaultFakeCluster     string = "testcluster"
	DefaultFakeNatsSubject string = "ee-hpc-nats"
	DefaultFakeNatsUser    string = ""
	DefaultFakeNatsPass    string = ""
	DefaultFakeNatsNkey    string = ""
	DefaultFakeEventName   string = "myevent"
	DefaultFakeServer      string = "127.0.0.1"
	DefaultFakeEventString string = "This is my event"
	DefaultFakePort        int    = 4222
	DefaultFakePrint       bool   = false
)

func main() {
	var uinfo nats.Option = nil
	var conn nats.Conn
	myhostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("failed to get hostname: %v", err.Error())
		os.Exit(1)
	}
	cluster := flag.String("cluster", DefaultFakeCluster, "Cluster name")
	subject := flag.String("subject", DefaultFakeNatsSubject, "NATS subject")
	user := flag.String("username", DefaultFakeNatsUser, "NATS username")
	password := flag.String("password", DefaultFakeNatsPass, "NATS password")
	nkey := flag.String("nkeyfile", DefaultFakeNatsNkey, "NATS NKEY file")
	name := flag.String("name", DefaultFakeEventName, "Event name to use when creating the CCEvent")
	event := flag.String("event", DefaultFakeEventString, "Event string to send")
	hostname := flag.String("hostname", myhostname, "Hostname used for starting the job")
	server := flag.String("server", DefaultFakeServer, "NATS server to connect")
	port := flag.Int("port", DefaultFakePort, "NATS server port")
	print := flag.Bool("print", DefaultFakePrint, "Print message only but do not send")
	fmt.Println("Helper tool to send events to NATS as CCEvent")
	fmt.Println()
	flag.Parse()

	if len(*user) > 0 && len(*password) > 0 {
		uinfo = nats.UserInfo(*user, *password)
	}
	if len(*nkey) > 0 {
		if _, err := os.Stat(*nkey); err == nil {
			uinfo = nats.UserCredentials(*nkey)
		} else {
			cclog.Error("Cannot use NATS NKEY file ", *nkey, ":", err.Error())
		}
	}

	if !*print {
		uri := fmt.Sprintf("nats://%s:%d", *server, *port)
		conn, err := nats.Connect(uri, uinfo)
		if err != nil {
			cclog.Error(err.Error())
			return
		}
		defer conn.Close()
		cclog.Debug("Connected to ", uri)
	}

	msg, err := lp.NewEvent(*name, map[string]string{
		"cluster":  *cluster,
		"hostname": *hostname,
	}, nil, *event, time.Now())
	if err != nil {
		cclog.Error("Failed to create event message:", err.Error())
	}

	if !*print {
		uri := fmt.Sprintf("nats://%s:%d", *server, *port)
		cclog.Debug("Publishing")
		cclog.Debug(msg.String())
		conn.Publish(*subject, []byte(msg.ToLineProtocol(nil)))
		conn.Flush()
		cclog.Debug("Closing connection to", uri)
	} else {
		fmt.Println(msg.String())
	}
}
