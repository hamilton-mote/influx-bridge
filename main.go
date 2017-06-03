package main

import (
	"fmt"
	"log"
	"os"
	"reflect"
	"regexp"
	"sync"
	"sync/atomic"
	"time"

	"github.com/immesys/bw2bind"
	"github.com/influxdata/influxdb/client/v2"
)

type insert struct {
	path string
	time int64
	dat  map[string]float64
}

var ichan chan insert
var mu sync.Mutex
var chq int64
var db client.Client

func main() {
	if len(os.Args) != 2 {
		fmt.Printf("Usage: influx-bridge <customerid>\n")
		os.Exit(1)
	}
	bw := bw2bind.ConnectOrExit("")
	bw.SetEntityFromEnvironOrExit()
	// Create a new HTTPClient
	var err error
	db, err = client.NewHTTPClient(client.HTTPConfig{
		Addr: "http://localhost:8086",
	})
	if err != nil {
		panic(err)
	}
	ichan = make(chan insert, 5000)
	for i := 0; i < 100; i++ {
		go inserts()
	}

	go hamiltons(bw, os.Args[1])
	for {
		time.Sleep(5 * time.Second)
		iq := atomic.LoadInt64(&chq)
		fmt.Printf("INSERTQ %d\n", iq)
	}
}

var floatType = reflect.TypeOf(float64(0))

func inserts() {
	for i := range ichan {
		then := time.Now()
		bp, err := client.NewBatchPoints(client.BatchPointsConfig{
			Database:  "hamilton",
			Precision: "ns",
		})
		if err != nil {
			log.Fatal(err)
		}
		tags := map[string]string{}
		pts := make(map[string]interface{})
		for k, v := range i.dat {
			pts[k] = v
		}
		pt, err := client.NewPoint(i.path, tags, pts, time.Unix(0, i.time))
		if err != nil {
			panic(err)
		}
		bp.AddPoint(pt)
		// Write the batch
		if err := db.Write(bp); err != nil {
			panic(err)
		}
		fmt.Printf("Whole record took %d ms to insert\n", time.Now().Sub(then)/time.Millisecond)
		atomic.AddInt64(&chq, -1)
	}
}

func getFloat(unk interface{}) (float64, error) {
	b, ok := unk.(bool)
	if ok {
		if b {
			return 1.0, nil
		} else {
			return 0.0, nil
		}
	}
	v := reflect.ValueOf(unk)
	v = reflect.Indirect(v)
	if !v.Type().ConvertibleTo(floatType) {
		return 0, fmt.Errorf("cannot convert %v to float64", v.Type())
	}
	fv := v.Convert(floatType)
	return fv.Float(), nil
}

var hampat = regexp.MustCompile(".*s.hamilton/([^/]+)/.*")

func doinsert(path string, time int64, dat map[string]float64) {
	is := insert{path, time, dat}
	select {
	case ichan <- is:
		atomic.AddInt64(&chq, 1)
	default:
		fmt.Printf("dropping data. Backlogged\n")
	}
}
func hamiltons(bw *bw2bind.BW2Client, customer string) {
	sc := bw.SubscribeOrExit(&bw2bind.SubscribeParams{
		URI:       fmt.Sprintf("hamiltonbackend/sensors/%s/*", customer),
		AutoChain: true,
	})
	for m := range sc {
		ms := make(map[string]interface{})
		po := m.GetOnePODF("2.0.11.2")
		if po == nil {
			fmt.Printf("no PO\n")
			continue
		}
		err := po.(bw2bind.MsgPackPayloadObject).ValueInto(&ms)
		if err != nil {
			fmt.Printf("Extract error: %v\n", err)
			continue
		}

		flmap := make(map[string]float64)
		var time int64
		for k, v := range ms {
			if k == "time" {
				time = int64(v.(float64))
				continue
			}
			flv, err := getFloat(v)
			if err != nil {
				panic(err)
			}
			flmap[k] = flv
		}
		path := hampat.ReplaceAllString(m.URI, `hamilton/$1`)
		doinsert(path, time, flmap)
	}
}
