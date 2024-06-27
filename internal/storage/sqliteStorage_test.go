package storage

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	cclog "github.com/ClusterCockpit/cc-metric-collector/pkg/ccLogger"
	lp "github.com/ClusterCockpit/cc-metric-collector/pkg/ccMetric"
)

func generate_metrics(count int) ([]lp.CCMetric, error) {
	out := make([]lp.CCMetric, 0, count)
	for i := 0; i < count; i++ {
		y, err := lp.New("test", map[string]string{"hostname": "myhost", "type": "node"}, map[string]string{"unit": "kHz"}, map[string]interface{}{"event": fmt.Sprintf("event%d", i)}, time.Now())
		if err != nil {
			return nil, err
		}
		out = append(out, y)
	}
	return out, nil
}

func TestNewStore(t *testing.T) {
	var wg sync.WaitGroup
	s, err := NewStorage(&wg, ".", "testing")
	if err != nil {
		t.Error(err.Error())
	}
	t.Cleanup(func() {
		s.Close()
		os.Remove("./testing.db")
	})
}

func TestStartStore(t *testing.T) {
	var wg sync.WaitGroup
	s, err := NewStorage(&wg, ".", "testing")
	if err != nil {
		t.Error(err.Error())
	}
	t.Cleanup(func() {
		s.Close()
		os.Remove("./testing.db")
	})
	s.Start()

}

func TestCloseWithoutStart(t *testing.T) {
	var wg sync.WaitGroup
	s, err := NewStorage(&wg, ".", "testing")
	if err != nil {
		t.Error(err.Error())
	}
	t.Cleanup(func() {
		os.Remove("./testing.db")
	})
	s.Close()

}

func TestAddStore(t *testing.T) {
	var wg sync.WaitGroup
	s, err := NewStorage(&wg, ".", "testing")
	if err != nil {
		t.Error(err.Error())
	}
	t.Cleanup(func() {
		s.Close()
		os.Remove("./testing.db")
	})
	s.Start()

	mlist, err := generate_metrics(10)
	if err != nil {
		t.Error(err.Error())
	}
	for _, m := range mlist {
		s.Submit(m)
	}

}

func TestQueryStore(t *testing.T) {
	var wg sync.WaitGroup
	s, err := NewStorage(&wg, ".", "testing")
	if err != nil {
		t.Error(err.Error())
	}
	t.Cleanup(func() {
		s.Close()
		os.Remove("./testing.db")
	})
	s.Start()
	t_before := time.Now()

	mlist, err := generate_metrics(10)
	if err != nil {
		t.Error(err.Error())
	}
	for i, m := range mlist {
		m.AddTag("stype", "filesystem")
		if i%2 == 0 {
			m.AddTag("stype-id", "/home")
		} else {
			m.AddTag("stype-id", "/mnt")
		}
	}
	cclog.SetDebug()
	for _, m := range mlist {
		s.Submit(m)
	}

	req := QueryRequest{
		Event:    "test",
		From:     t_before.Unix(),
		To:       time.Now().Unix(),
		Hostname: "myhost",
		Conditions: []string{
			"stype-id = \"/home\"",
		},
	}
	cclog.Debug("Start Query")
	events, err := s.Query(req)
	if err != nil {
		t.Error(err.Error())
	}
	for _, e := range events {
		t.Log(e)
	}

}

func TestDeleteStore(t *testing.T) {
	before := 5
	after := 5
	var wg sync.WaitGroup
	s, err := NewStorage(&wg, ".", "testing")
	if err != nil {
		t.Error(err.Error())
	}
	t.Cleanup(func() {
		s.Close()
		os.Remove("./testing.db")
	})
	s.Start()
	t_before := time.Now()
	mlist, err := generate_metrics(before)
	if err != nil {
		t.Error(err.Error())
	}
	for _, m := range mlist {
		s.Submit(m)
	}
	middle := time.Now()
	time.Sleep(2 * time.Second)
	mlist, err = generate_metrics(after)
	if err != nil {
		t.Error(err.Error())
	}
	for _, m := range mlist {
		s.Submit(m)
	}
	cclog.SetDebug()
	err = s.Delete(middle.Unix())
	if err != nil {
		t.Error(err.Error())
	}

	req := QueryRequest{
		Event:    "test",
		From:     t_before.Unix(),
		To:       time.Now().Unix(),
		Hostname: "myhost",
	}
	cclog.Debug("Start Query")
	events, err := s.Query(req)
	if err != nil {
		t.Error(err.Error())
	}
	if len(events) != after {
		for _, e := range events {
			t.Log(e)
		}
		t.Errorf("delete failed, there should only be %d events in the DB", after)

	}
}
