package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	cclog "github.com/ClusterCockpit/cc-metric-collector/pkg/ccLogger"
	lp "github.com/ClusterCockpit/cc-metric-collector/pkg/ccMetric"
)

const TEST_DB_NAME = `testing.db`
const TEST_DB_PATH = `.`
const TEST_CONFIG_PATH = `.`
const TEST_CONFIG_NAME = `testing.json`

func Write_testconfig(filename string) error {
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	config := sqliteStorageConfig{
		Type:      "sql",
		Path:      fmt.Sprintf("%s/%s", TEST_DB_PATH, TEST_DB_NAME),
		Retention: "48h",
		Flags: []string{
			"_journal=WAL",
			"_timeout=5000",
			"_fk=true",
		},
	}

	b, err := json.MarshalIndent(config, "", " ")
	if err != nil {
		return err
	}

	f.Write(b)
	return nil
}

func Generate_metrics(count int) ([]lp.CCMetric, error) {
	out := make([]lp.CCMetric, 0, count)
	for i := 0; i < count; i++ {
		y, err := lp.New("test", map[string]string{"hostname": "myhost", "type": "node", "cluster": "testcluster"}, map[string]string{"unit": "kHz"}, map[string]interface{}{"event": fmt.Sprintf("event%d", i)}, time.Now())
		if err != nil {
			return nil, err
		}
		out = append(out, y)
	}
	return out, nil
}

func TestNew(t *testing.T) {
	var wg sync.WaitGroup
	x := new(sqliteStorage)

	err := Write_testconfig(fmt.Sprintf("%s/%s", TEST_CONFIG_PATH, TEST_CONFIG_NAME))
	if err != nil {
		t.Error(err.Error())
	}
	cclog.SetDebug()
	err = x.Init(&wg, fmt.Sprintf("%s/%s", TEST_CONFIG_PATH, TEST_CONFIG_NAME))
	if err != nil {
		t.Error(err.Error())
	}
	t.Cleanup(func() {
		x.Close()
		os.Remove(fmt.Sprintf("%s/%s", TEST_CONFIG_PATH, TEST_CONFIG_NAME))
		dbfiles, err := filepath.Glob(fmt.Sprintf("%s/%s", TEST_DB_PATH, TEST_DB_NAME))
		if err != nil {
			t.Error(err.Error())
		}
		for _, f := range dbfiles {
			os.Remove(f)
		}
	})
}

func TestWrite(t *testing.T) {
	var wg sync.WaitGroup
	x := new(sqliteStorage)

	err := Write_testconfig(fmt.Sprintf("%s/%s", TEST_CONFIG_PATH, TEST_CONFIG_NAME))
	if err != nil {
		t.Error(err.Error())
	}
	cclog.SetDebug()
	err = x.Init(&wg, fmt.Sprintf("%s/%s", TEST_CONFIG_PATH, TEST_CONFIG_NAME))
	if err != nil {
		t.Error(err.Error())
	}
	t.Cleanup(func() {
		x.Close()
		os.Remove(fmt.Sprintf("%s/%s", TEST_CONFIG_PATH, TEST_CONFIG_NAME))
		dbfiles, err := filepath.Glob(fmt.Sprintf("%s/%s", TEST_DB_PATH, TEST_DB_NAME))
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
		x.Write(m)
	}
}

func BenchmarkWrite(b *testing.B) {
	var wg sync.WaitGroup
	x := new(sqliteStorage)

	err := Write_testconfig(fmt.Sprintf("%s/%s", TEST_CONFIG_PATH, TEST_CONFIG_NAME))
	if err != nil {
		b.Error(err.Error())
	}
	err = x.Init(&wg, fmt.Sprintf("%s/%s", TEST_CONFIG_PATH, TEST_CONFIG_NAME))
	if err != nil {
		b.Error(err.Error())
	}
	b.Cleanup(func() {
		x.Close()
		os.Remove(fmt.Sprintf("%s/%s", TEST_CONFIG_PATH, TEST_CONFIG_NAME))
		dbfiles, err := filepath.Glob(fmt.Sprintf("%s/%s", TEST_DB_PATH, TEST_DB_NAME))
		if err != nil {
			b.Error(err.Error())
		}
		for _, f := range dbfiles {
			os.Remove(f)
		}
	})

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			for i := 0; i < b.N; i++ {
				y, err := lp.New("test", map[string]string{"hostname": "myhost", "type": "node", "cluster": "testcluster"}, map[string]string{"unit": "kHz"}, map[string]interface{}{"event": fmt.Sprintf("event%d", i)}, time.Now())
				if err == nil {
					x.Write(y)
				}
			}
		}
	})
}

func gen_parallel(x SqlStorage, threads int) func(b *testing.B) {
	return func(b *testing.B) {
		b.SetParallelism(threads)
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				for i := 0; i < b.N; i++ {
					y, err := lp.New("test", map[string]string{"hostname": "myhost", "type": "node", "cluster": "testcluster"}, map[string]string{"unit": "kHz"}, map[string]interface{}{"event": fmt.Sprintf("event%d", i)}, time.Now())
					if err == nil {
						x.Write(y)
					}
				}
			}
		})
	}
}

func BenchmarkWriteParallel(b *testing.B) {
	var wg sync.WaitGroup
	x := new(sqliteStorage)

	err := Write_testconfig(fmt.Sprintf("%s/%s", TEST_CONFIG_PATH, TEST_CONFIG_NAME))
	if err != nil {
		b.Error(err.Error())
	}
	err = x.Init(&wg, fmt.Sprintf("%s/%s", TEST_CONFIG_PATH, TEST_CONFIG_NAME))
	if err != nil {
		b.Error(err.Error())
	}
	b.Cleanup(func() {
		x.Close()
		os.Remove(fmt.Sprintf("%s/%s", TEST_CONFIG_PATH, TEST_CONFIG_NAME))
		dbfiles, err := filepath.Glob(fmt.Sprintf("%s/%s", TEST_DB_PATH, TEST_DB_NAME))
		if err != nil {
			b.Error(err.Error())
		}
		for _, f := range dbfiles {
			os.Remove(f)
		}
	})

	for i := 1; i < 16; i++ {
		b.Run(fmt.Sprintf("threads=%d", i), gen_parallel(x, i))
	}
}

func TestQuery(t *testing.T) {
	var wg sync.WaitGroup
	x := new(sqliteStorage)

	err := Write_testconfig(fmt.Sprintf("%s/%s", TEST_CONFIG_PATH, TEST_CONFIG_NAME))
	if err != nil {
		t.Error(err.Error())
	}
	cclog.SetDebug()
	err = x.Init(&wg, fmt.Sprintf("%s/%s", TEST_CONFIG_PATH, TEST_CONFIG_NAME))
	if err != nil {
		t.Error(err.Error())
	}
	t.Cleanup(func() {
		x.Close()
		os.Remove(fmt.Sprintf("%s/%s", TEST_CONFIG_PATH, TEST_CONFIG_NAME))
		dbfiles, err := filepath.Glob(fmt.Sprintf("%s/%s", TEST_DB_PATH, TEST_DB_NAME))
		if err != nil {
			t.Error(err.Error())
		}
		for _, f := range dbfiles {
			os.Remove(f)
		}
	})

	mlist, err := Generate_metrics(8)
	if err != nil {
		t.Error(err.Error())
	}
	for _, m := range mlist {
		x.Write(m)
	}

	request := QueryRequest{
		Event:     mlist[0].Name(),
		To:        time.Now().Unix(),
		From:      time.Now().Unix() - 10,
		Hostname:  "myhost",
		Cluster:   "testcluster",
		QueryType: QueryTypeEvent,
	}

	result, err := x.Query(request)
	if err != nil {
		t.Error()
	}
	if len(result.Results) == 0 {
		t.Errorf("no results")
	}
	for _, e := range result.Results {
		t.Logf("%d : %s", e.Timestamp, e.Event)
	}
}

func TestQueryConditions(t *testing.T) {
	var wg sync.WaitGroup
	x := new(sqliteStorage)

	err := Write_testconfig(fmt.Sprintf("%s/%s", TEST_CONFIG_PATH, TEST_CONFIG_NAME))
	if err != nil {
		t.Error(err.Error())
	}
	cclog.SetDebug()
	err = x.Init(&wg, fmt.Sprintf("%s/%s", TEST_CONFIG_PATH, TEST_CONFIG_NAME))
	if err != nil {
		t.Error(err.Error())
	}
	t.Cleanup(func() {
		x.Close()
		os.Remove(fmt.Sprintf("%s/%s", TEST_CONFIG_PATH, TEST_CONFIG_NAME))
		dbfiles, err := filepath.Glob(fmt.Sprintf("%s/%s", TEST_DB_PATH, TEST_DB_NAME))
		if err != nil {
			t.Error(err.Error())
		}
		for _, f := range dbfiles {
			os.Remove(f)
		}
	})

	mlist, err := Generate_metrics(8)
	if err != nil {
		t.Error(err.Error())
	}
	for i, m := range mlist {
		if i%2 == 0 {
			m.AddTag("type", "socket")
			m.AddTag("type-id", "1")
		}
	}
	for _, m := range mlist {
		x.Write(m)
	}

	request := QueryRequest{
		Event:     mlist[0].Name(),
		To:        time.Now().Unix(),
		From:      time.Now().Unix() - 10,
		Hostname:  "myhost",
		Cluster:   "testcluster",
		QueryType: QueryTypeEvent,
		Conditions: []QueryCondition{
			{
				Pred:      "type",
				Operation: "!=",
				Args:      []interface{}{"node"},
			},
			{
				Pred:      "type",
				Operation: "==",
				Args:      []interface{}{"socket"},
			},
			{
				Pred:      "type-id",
				Operation: "==",
				Args:      []interface{}{"0", "1"},
			},
		},
	}

	result, err := x.Query(request)
	if err != nil {
		t.Error()
	}
	if len(result.Results) == 0 {
		t.Errorf("no results")
	}
	for _, e := range result.Results {
		t.Logf("%d : %s", e.Timestamp, e.Event)
	}
}

func TestWriteChan(t *testing.T) {
	var wg sync.WaitGroup
	x := new(sqliteStorage)
	ch := make(chan lp.CCMetric)

	err := Write_testconfig(fmt.Sprintf("%s/%s", TEST_CONFIG_PATH, TEST_CONFIG_NAME))
	if err != nil {
		t.Error(err.Error())
	}
	cclog.SetDebug()
	err = x.Init(&wg, fmt.Sprintf("%s/%s", TEST_CONFIG_PATH, TEST_CONFIG_NAME))
	if err != nil {
		t.Error(err.Error())
	}
	t.Cleanup(func() {
		x.Close()
		os.Remove(fmt.Sprintf("%s/%s", TEST_CONFIG_PATH, TEST_CONFIG_NAME))
		dbfiles, err := filepath.Glob(fmt.Sprintf("%s/%s", TEST_DB_PATH, TEST_DB_NAME))
		if err != nil {
			t.Error(err.Error())
		}
		for _, f := range dbfiles {
			os.Remove(f)
		}
	})
	x.SetInput(ch)
	x.Start()

	mlist, err := Generate_metrics(4)
	if err != nil {
		t.Error(err.Error())
	}
	for _, m := range mlist {
		ch <- m
	}
}

func TestWriteChanQuery(t *testing.T) {
	var wg sync.WaitGroup
	x := new(sqliteStorage)
	ch := make(chan lp.CCMetric)

	err := Write_testconfig(fmt.Sprintf("%s/%s", TEST_CONFIG_PATH, TEST_CONFIG_NAME))
	if err != nil {
		t.Error(err.Error())
	}
	cclog.SetDebug()
	err = x.Init(&wg, fmt.Sprintf("%s/%s", TEST_CONFIG_PATH, TEST_CONFIG_NAME))
	if err != nil {
		t.Error(err.Error())
	}
	x.SetInput(ch)
	x.Start()
	t.Cleanup(func() {
		x.Close()
		os.Remove(fmt.Sprintf("%s/%s", TEST_CONFIG_PATH, TEST_CONFIG_NAME))
		dbfiles, err := filepath.Glob(fmt.Sprintf("%s/%s", TEST_DB_PATH, TEST_DB_NAME))
		if err != nil {
			t.Error(err.Error())
		}
		for _, f := range dbfiles {
			os.Remove(f)
		}
	})

	mlist, err := Generate_metrics(8)
	if err != nil {
		t.Error(err.Error())
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

	result, err := x.Query(request)
	if err != nil {
		t.Error()
	}
	if len(result.Results) == 0 {
		t.Errorf("no results")
	}
	for _, e := range result.Results {
		t.Logf("%d : %s", e.Timestamp, e.Event)
	}
}
