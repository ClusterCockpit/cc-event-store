// Copyright (C) NHR@FAU, University Erlangen-Nuremberg.
// All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.
package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	lp "github.com/ClusterCockpit/cc-energy-manager/pkg/cc-message"
	cclog "github.com/ClusterCockpit/cc-metric-collector/pkg/ccLogger"
)

const (
	TEST_DB_NAME     = `testing.db`
	TEST_DB_PATH     = `.`
	TEST_CONFIG_PATH = `.`
	TEST_CONFIG_NAME = `testing.json`
)

func Write_testconfig(filename string) error {
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	config := storageManagerConfig{
		Backend:    json.RawMessage(fmt.Sprintf(`{"type" : "sqlite", "database_path" : "%s/%s", "flags" : ["_journal=WAL", "_timeout=5000", "_fk=true"] }`, TEST_DB_PATH, TEST_DB_NAME)),
		BatchSize:  10,
		MaxProcess: 5,
		Retention:  "48h",
		FlushTime:  "2s",
	}

	b, err := json.MarshalIndent(config, "", " ")
	if err != nil {
		return err
	}

	f.Write(b)
	return nil
}

func Generate_metrics(count int) ([]lp.CCMessage, error) {
	out := make([]lp.CCMessage, 0, count)
	for i := 0; i < count; i++ {
		y, err := lp.NewMessage("test", map[string]string{"hostname": "myhost", "type": "socket", "type-id": fmt.Sprintf("%d", i), "cluster": "testcluster"}, map[string]string{"unit": "kHz"}, map[string]interface{}{"event": fmt.Sprintf("event%d", i)}, time.Now())
		if err != nil {
			return nil, err
		}
		out = append(out, y)
	}
	return out, nil
}

func TestNewManager(t *testing.T) {
	var wg sync.WaitGroup

	err := Write_testconfig(fmt.Sprintf("%s/%s", TEST_CONFIG_PATH, TEST_CONFIG_NAME))
	if err != nil {
		t.Error(err.Error())
		return
	}
	t.Cleanup(func() {
		os.Remove(fmt.Sprintf("%s/%s", TEST_CONFIG_PATH, TEST_CONFIG_NAME))
	})
	cclog.SetDebug()
	_, err = NewStorageManager(&wg, fmt.Sprintf("%s/%s", TEST_CONFIG_PATH, TEST_CONFIG_NAME))
	if err != nil {
		t.Error(err.Error())
		return
	}
}

func TestCloseManager(t *testing.T) {
	var wg sync.WaitGroup

	err := Write_testconfig(fmt.Sprintf("%s/%s", TEST_CONFIG_PATH, TEST_CONFIG_NAME))
	if err != nil {
		t.Error(err.Error())
		return
	}
	t.Cleanup(func() {
		os.Remove(fmt.Sprintf("%s/%s", TEST_CONFIG_PATH, TEST_CONFIG_NAME))
	})

	sm, err := NewStorageManager(&wg, fmt.Sprintf("%s/%s", TEST_CONFIG_PATH, TEST_CONFIG_NAME))
	if err != nil {
		t.Error(err.Error())
		return
	}
	sm.Close()
}

func TestWriteManager(t *testing.T) {
	var wg sync.WaitGroup
	ch := make(chan lp.CCMessage, 10)

	err := Write_testconfig(fmt.Sprintf("%s/%s", TEST_CONFIG_PATH, TEST_CONFIG_NAME))
	if err != nil {
		t.Error(err.Error())
	}
	t.Cleanup(func() {
		os.Remove(fmt.Sprintf("%s/%s", TEST_CONFIG_PATH, TEST_CONFIG_NAME))
	})
	cclog.SetDebug()
	sm, err := NewStorageManager(&wg, fmt.Sprintf("%s/%s", TEST_CONFIG_PATH, TEST_CONFIG_NAME))
	if err != nil {
		t.Error(err.Error())
	}

	sm.SetInput(ch)
	sm.Start()

	t.Cleanup(func() {
		os.Remove(fmt.Sprintf("%s/%s", TEST_CONFIG_PATH, TEST_CONFIG_NAME))
		dbfiles, err := filepath.Glob(fmt.Sprintf("%s/%s*", TEST_DB_PATH, TEST_DB_NAME))
		if err != nil {
			t.Error(err.Error())
		}
		for _, f := range dbfiles {
			os.Remove(f)
		}
	})

	mlist, err := Generate_metrics(4)
	if err != nil {
		t.Error(err.Error())
	}
	for _, m := range mlist {
		t.Log(m)
		ch <- m
	}

	// time.Sleep(time.Second * 2)
	mlist, _ = Generate_metrics(10)
	for _, m := range mlist {
		t.Log(m)
		ch <- m
	}
	time.Sleep(time.Second)
	sm.Close()
}

func gen_parallel_for_manager(ch chan lp.CCMessage, mlist []lp.CCMessage) func(b *testing.B) {
	return func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				for _, msg := range mlist {
					ch <- msg
				}
			}
		})
	}
}

func BenchmarkWriteManagerParallel(b *testing.B) {
	var wg sync.WaitGroup
	num_threads := 16
	ch := make(chan lp.CCMessage, 20)

	err := Write_testconfig(fmt.Sprintf("%s/%s", TEST_CONFIG_PATH, TEST_CONFIG_NAME))
	if err != nil {
		b.Error(err.Error())
		return
	}
	sm, err := NewStorageManager(&wg, fmt.Sprintf("%s/%s", TEST_CONFIG_PATH, TEST_CONFIG_NAME))
	if err != nil {
		b.Error(err.Error())
		return
	}
	sm.SetInput(ch)
	sm.Start()
	b.Cleanup(func() {
		sm.Close()
		os.Remove(fmt.Sprintf("%s/%s", TEST_CONFIG_PATH, TEST_CONFIG_NAME))
		dbfiles, err := filepath.Glob(fmt.Sprintf("%s/%s*", TEST_DB_PATH, TEST_DB_NAME))
		if err != nil {
			b.Error(err.Error())
			return
		}
		for _, f := range dbfiles {
			os.Remove(f)
		}
	})
	mlists := make([][]lp.CCMessage, 0)
	for i := 0; i < num_threads; i++ {
		mlist, err := Generate_metrics(b.N)
		if err != nil {
			b.Error(err)
			return
		}
		mlists = append(mlists, mlist)
	}

	for i := 1; i < num_threads; i++ {
		b.SetParallelism(i)
		b.StartTimer()
		b.Run(fmt.Sprintf("threads=%d", i), gen_parallel_for_manager(ch, mlists[i-1]))
		b.StopTimer()
	}
}

func TestQueryManager(t *testing.T) {
	var wg sync.WaitGroup
	ch := make(chan lp.CCMessage, 10)

	err := Write_testconfig(fmt.Sprintf("%s/%s", TEST_CONFIG_PATH, TEST_CONFIG_NAME))
	if err != nil {
		t.Error(err.Error())
		return
	}
	t.Cleanup(func() {
		os.Remove(fmt.Sprintf("%s/%s", TEST_CONFIG_PATH, TEST_CONFIG_NAME))
	})
	cclog.SetDebug()
	sm, err := NewStorageManager(&wg, fmt.Sprintf("%s/%s", TEST_CONFIG_PATH, TEST_CONFIG_NAME))
	if err != nil {
		t.Error(err.Error())
		return
	}

	sm.SetInput(ch)
	sm.Start()

	t.Cleanup(func() {
		os.Remove(fmt.Sprintf("%s/%s", TEST_CONFIG_PATH, TEST_CONFIG_NAME))
		dbfiles, err := filepath.Glob(fmt.Sprintf("%s/%s*", TEST_DB_PATH, TEST_DB_NAME))
		if err != nil {
			t.Error(err.Error())
		}
		for _, f := range dbfiles {
			os.Remove(f)
		}
	})

	mlist, err := Generate_metrics(20)
	if err != nil {
		t.Error(err.Error())
		return
	}
	for _, m := range mlist {
		ch <- m
	}

	request := QueryRequest{
		Event:     mlist[0].Name(),
		To:        time.Now().Unix(),
		From:      time.Now().Unix() - 10,
		Hostname:  "myhost",
		Cluster:   "testcluster",
		QueryType: QueryTypeEvent,
	}

	result, err := sm.Query(request)
	if err != nil {
		t.Error()
		return
	}
	if len(result.Results) == 0 {
		t.Errorf("no results")
		return
	}
	for _, e := range result.Results {
		t.Logf("%d : %s", e.Timestamp, e.Event)
	}
	sm.Close()
}
