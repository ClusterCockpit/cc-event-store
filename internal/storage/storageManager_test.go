package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	lp "github.com/ClusterCockpit/cc-metric-collector/pkg/ccMetric"
)

func TestNewManager(t *testing.T) {
	var wg sync.WaitGroup

	err := Write_testconfig(fmt.Sprintf("%s/%s", TEST_CONFIG_PATH, TEST_CONFIG_NAME))
	if err != nil {
		t.Error(err.Error())
	}
	t.Cleanup(func() {
		os.Remove(fmt.Sprintf("%s/%s", TEST_CONFIG_PATH, TEST_CONFIG_NAME))
	})

	_, err = NewStorageManager(&wg, fmt.Sprintf("%s/%s", TEST_CONFIG_PATH, TEST_CONFIG_NAME))
	if err != nil {
		t.Error(err.Error())
	}
}

func TestCloseManager(t *testing.T) {
	var wg sync.WaitGroup

	err := Write_testconfig(fmt.Sprintf("%s/%s", TEST_CONFIG_PATH, TEST_CONFIG_NAME))
	if err != nil {
		t.Error(err.Error())
	}
	t.Cleanup(func() {
		os.Remove(fmt.Sprintf("%s/%s", TEST_CONFIG_PATH, TEST_CONFIG_NAME))
	})

	sm, err := NewStorageManager(&wg, fmt.Sprintf("%s/%s", TEST_CONFIG_PATH, TEST_CONFIG_NAME))
	if err != nil {
		t.Error(err.Error())
	}
	sm.Close()
}

func TestWriteManager(t *testing.T) {
	var wg sync.WaitGroup

	err := Write_testconfig(fmt.Sprintf("%s/%s", TEST_CONFIG_PATH, TEST_CONFIG_NAME))
	if err != nil {
		t.Error(err.Error())
	}
	t.Cleanup(func() {
		os.Remove(fmt.Sprintf("%s/%s", TEST_CONFIG_PATH, TEST_CONFIG_NAME))
	})
	sm, err := NewStorageManager(&wg, fmt.Sprintf("%s/%s", TEST_CONFIG_PATH, TEST_CONFIG_NAME))
	if err != nil {
		t.Error(err.Error())
	}
	defer sm.Close()
	t.Cleanup(func() {
		sm.Close()
		os.Remove(fmt.Sprintf("%s/%s", TEST_CONFIG_PATH, TEST_CONFIG_NAME))
		os.Remove("./testconfig.db")
	})

	mlist, err := Generate_metrics(4)
	if err != nil {
		t.Error(err.Error())
	}
	for _, m := range mlist {
		sm.Write(m)
	}
}

func gen_parallel_for_manager(x StorageManager, threads int) func(b *testing.B) {
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

func BenchmarkWriteManagerParallel(b *testing.B) {
	var wg sync.WaitGroup

	err := Write_testconfig(fmt.Sprintf("%s/%s", TEST_CONFIG_PATH, TEST_CONFIG_NAME))
	if err != nil {
		b.Error(err.Error())
	}
	sm, err := NewStorageManager(&wg, fmt.Sprintf("%s/%s", TEST_CONFIG_PATH, TEST_CONFIG_NAME))
	if err != nil {
		b.Error(err.Error())
	}
	b.Cleanup(func() {
		sm.Close()
		os.Remove(fmt.Sprintf("%s/%s", TEST_CONFIG_PATH, TEST_CONFIG_NAME))
		dbfiles, err := filepath.Glob("./testing.db*")
		if err != nil {
			b.Error(err.Error())
		}
		for _, f := range dbfiles {
			os.Remove(f)
		}
	})

	mlist, err := Generate_metrics(4)
	if err != nil {
		b.Error(err.Error())
	}
	for _, m := range mlist {
		sm.Write(m)
	}

	for i := 1; i < 16; i++ {
		b.Run(fmt.Sprintf("threads=%d", i), gen_parallel_for_manager(sm, i))
	}
}

func TestStartManager(t *testing.T) {
	var wg sync.WaitGroup

	err := Write_testconfig(fmt.Sprintf("%s/%s", TEST_CONFIG_PATH, TEST_CONFIG_NAME))
	if err != nil {
		t.Error(err.Error())
	}

	sm, err := NewStorageManager(&wg, fmt.Sprintf("%s/%s", TEST_CONFIG_PATH, TEST_CONFIG_NAME))
	if err != nil {
		t.Error(err.Error())
	}
	t.Cleanup(func() {
		sm.Close()
		os.Remove(fmt.Sprintf("%s/%s", TEST_CONFIG_PATH, TEST_CONFIG_NAME))
		dbfiles, err := filepath.Glob("./testing.db*")
		if err != nil {
			t.Error(err.Error())
		}
		for _, f := range dbfiles {
			os.Remove(f)
		}
	})
	sm.Start()
}

func TestWriteChanManager(t *testing.T) {
	var wg sync.WaitGroup
	ch := make(chan lp.CCMetric)

	err := Write_testconfig(fmt.Sprintf("%s/%s", TEST_CONFIG_PATH, TEST_CONFIG_NAME))
	if err != nil {
		t.Error(err.Error())
	}

	sm, err := NewStorageManager(&wg, fmt.Sprintf("%s/%s", TEST_CONFIG_PATH, TEST_CONFIG_NAME))
	if err != nil {
		t.Error(err.Error())
	}
	t.Cleanup(func() {
		sm.Close()
		os.Remove(fmt.Sprintf("%s/%s", TEST_CONFIG_PATH, TEST_CONFIG_NAME))
	})
	sm.SetInput(ch)
	sm.Start()

	mlist, err := Generate_metrics(4)
	if err != nil {
		t.Error(err.Error())
	}
	for _, m := range mlist {
		ch <- m
	}
}
