package main

import (
	"fmt"
	"time"

	cclog "github.com/ClusterCockpit/cc-metric-collector/pkg/ccLogger"
	"github.com/nats-io/nats.go"
)

func main() {
	cluster := "testcluster"
	unixTime := time.Now().Unix()
	events := []string{
		`cpu_freq.max_cpu_freq,hostname=nuc,cluster=testcluster,type=node,type-id=0,method=GET event="foobar" ` + fmt.Sprintf("%d", unixTime),
		`cpu_freq.basefreq,hostname=m1023,cluster=meggie,type=node,type-id=0,method=GET event="foobar" ` + fmt.Sprintf("%d", unixTime),
	}

	conn, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		cclog.Error(err.Error())
		return
	}
	fmt.Println("Connected")

	for _, e := range events {
		fmt.Println("Publish ", e)
		conn.Publish(cluster, []byte(e))
	}
	time.Sleep(1 * time.Second)
	fmt.Println("Closing")
	conn.Close()
}
