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

	cclog "github.com/ClusterCockpit/cc-lib/ccLogger"
	lp "github.com/ClusterCockpit/cc-lib/ccMessage"
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
	DefaultFakeDebug       bool   = false
)

var (
	flagVersion, flagLogDateTime                           bool
	flagCluster, flagSubject, flagConfigFile, flagLogLevel string
	flagUser, flagPassword, flagNkey, flagName, flagEvent  string
	flagServer, flagHostname                               string
	flagPort                                               int64
	flagPrint                                              bool
)

func ReadCli() {
	myhostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("failed to get hostname: %v", err.Error())
		os.Exit(1)
	}
	flag.StringVar(&flagConfigFile, "config", "./config.json", "Path to configuration file")
	flag.StringVar(&flagLogLevel, "loglevel", "warn", "Sets the logging level: `[debug,info,warn (default),err,fatal,crit]`")
	flag.BoolVar(&flagLogDateTime, "logdate", false, "Set this flag to add date and time to log messages")
	flag.BoolVar(&flagPrint, "print", DefaultFakePrint, "Print message only but do not send")
	flag.StringVar(&flagCluster, "cluster", DefaultFakeCluster, "Cluster name")
	flag.StringVar(&flagSubject, "subject", DefaultFakeNatsSubject, "NATS subject")
	flag.StringVar(&flagUser, "username", DefaultFakeNatsUser, "NATS username")
	flag.StringVar(&flagPassword, "password", DefaultFakeNatsPass, "NATS password")
	flag.StringVar(&flagNkey, "nkeyfile", DefaultFakeNatsNkey, "NATS NKEY file")
	flag.StringVar(&flagName, "name", DefaultFakeEventName, "Event name to use when creating the CCEvent")
	flag.StringVar(&flagEvent, "event", DefaultFakeEventString, "Event string to send")
	flag.StringVar(&flagHostname, "hostname", myhostname, "Hostname used for starting the job")
	flag.StringVar(&flagServer, "server", DefaultFakeServer, "NATS server to connect")
	flag.Int64Var(&flagPort, "port", int64(DefaultFakePort), "NATS server port")
	flag.Parse()
}

func main() {
	var err error
	var uinfo nats.Option = nil
	var conn *nats.Conn
	ReadCli()
	cclog.Init(flagLogLevel, flagLogDateTime)
	fmt.Println("Helper tool to send events to NATS as CCEvent")
	fmt.Println()

	if len(flagUser) > 0 && len(flagPassword) > 0 {
		uinfo = nats.UserInfo(flagUser, flagPassword)
	}
	if len(flagNkey) > 0 {
		if _, err := os.Stat(flagNkey); err == nil {
			uinfo = nats.UserCredentials(flagNkey)
		} else {
			cclog.Error("Cannot use NATS NKEY file ", flagNkey, ":", err.Error())
		}
	}

	if !flagPrint {
		uri := fmt.Sprintf("nats://%s:%d", flagServer, flagPort)
		conn, err = nats.Connect(uri, uinfo)
		if err != nil {
			cclog.Error(err.Error())
			return
		}
		cclog.Debug("Connected to ", uri)
	}

	msg, err := lp.NewEvent(flagName, map[string]string{
		"cluster":  flagCluster,
		"hostname": flagHostname,
	}, nil, flagEvent, time.Now())
	if err != nil {
		cclog.Error("Failed to create event message:", err.Error())
	}

	if !flagPrint {
		uri := fmt.Sprintf("nats://%s:%d", flagServer, flagPort)
		cclog.Debug("Publishing")
		cclog.Debug(msg.String())
		conn.Publish(flagSubject, []byte(msg.ToLineProtocol(map[string]bool{})))
		conn.Flush()
		cclog.Debug("Closing connection to ", uri)
		conn.Close()
	} else {
		fmt.Println(msg.String())
	}
}
