package storage

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"

	cclog "github.com/ClusterCockpit/cc-metric-collector/pkg/ccLogger"
)

func Write_postgresconfig(filename string) error {
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	config := postgresStorageConfig{
		Type:      "postgres",
		Path:      "testing",
		Retention: "48h",
	}

	b, err := json.MarshalIndent(config, "", " ")
	if err != nil {
		return err
	}

	f.Write(b)
	return nil
}

func TestNewPostgres(t *testing.T) {
	var wg sync.WaitGroup
	x := new(postgresStorage)

	err := Write_postgresconfig("testconfig.json")
	if err != nil {
		t.Error(err.Error())
	}
	cclog.SetDebug()
	err = x.Init(&wg, "testconfig.json")
	if err != nil {
		t.Error(err.Error())
	}
	t.Cleanup(func() {
		x.Close()
		os.Remove("./testconfig.json")
		dbfiles, err := filepath.Glob("./testing.db*")
		if err != nil {
			t.Error(err.Error())
		}
		for _, f := range dbfiles {
			t.Logf("Remove file %s", f)
			os.Remove(f)
		}
	})
}

func TestWritePostgres(t *testing.T) {
	var wg sync.WaitGroup
	x := new(postgresStorage)

	err := Write_postgresconfig("testconfig.json")
	if err != nil {
		t.Error(err.Error())
	}
	cclog.SetDebug()
	err = x.Init(&wg, "testconfig.json")
	if err != nil {
		t.Error(err.Error())
	}
	t.Cleanup(func() {
		x.Close()
		os.Remove("./testconfig.json")
		dbfiles, err := filepath.Glob("./testing.db*")
		if err != nil {
			t.Error(err.Error())
		}
		for _, f := range dbfiles {
			t.Logf("Remove file %s", f)
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
