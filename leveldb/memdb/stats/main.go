package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"runtime/pprof"
	"time"

	"github.com/syndtr/goleveldb/leveldb/comparer"
	"github.com/syndtr/goleveldb/leveldb/memdb"
)

func main() {
	f, err := os.Create("cpu.out")
	if err != nil {
		fmt.Printf("could not create CPU profile: %v\n", err)
		return
	}
	if err := pprof.StartCPUProfile(f); err != nil {
		fmt.Printf("could not start CPU profile: %v\n", err)
		return
	}
	defer pprof.StopCPUProfile()

	// keys: 32 byte x 4*1024 * 1024 = 128 Mb
	// values: 32 byte x 4*1024 * 1024 = 128 Mb
	// A total of around 256MB, with 4M key/value pairs

	//size := 30 * 1024 * 1024
	size := 4 * 1024 * 1024
	RunStats(size, 32, 32)
}

func RunStats(itemCount int, keySize, valueSize int) {
	db := memdb.New(comparer.DefaultComparer, 1024*1024*64)
	t0 := time.Now()
	nItems := 0
	key := make([]byte, keySize)
	var xvalues []float64
	var yvalues []float64
	xvalues = append(xvalues, float64(0))
	yvalues = append(yvalues, float64(0))
	zero := time.Now()
	value := make([]byte, valueSize)
	for i := 0; i < itemCount; i++ {
		rand.Read(key)
		db.Put(key, value)
		nItems++
		if i > 0 && i%(itemCount/400) == 0 {
			d := time.Since(t0)
			fmt.Printf("%d items, %v\n", i, d)
			xvalues = append(xvalues, float64(i))
			yvalues = append(yvalues, float64(d)/float64(1_000_000))
			nItems = 0
			t0 = time.Now()
		}
	}
	total := time.Since(zero)
	legend := fmt.Sprintf("%d (%d:%d) elems\nTime: %.2f s\n%v",
		itemCount, keySize, valueSize, float64(total)/float64(1000000000), db.Stats())
	name := fmt.Sprintf("memdb-%d", itemCount)
	store(xvalues, yvalues, nil, "items", "ms", "", "db.Put time", legend, name)
}

type storage struct {
	Legend   string
	Title    string
	Xvalues  []float64
	XUnit    string
	Yvalues  []float64
	YUnit    string
	Y2values []float64
	Y2Unit   string
}

func store(xvals, yvals, y2vals []float64, xUnit, yUnit, y2Unit, title, legend, name string) {
	data, err := json.Marshal(&storage{
		Legend:   legend,
		Title:    title,
		Xvalues:  xvals,
		XUnit:    xUnit,
		Yvalues:  yvals,
		YUnit:    yUnit,
		Y2values: y2vals,
		Y2Unit:   y2Unit,
	})
	if err != nil {
		panic(err)
	}
	fname := fmt.Sprintf("%v.%d.json", name, time.Now().Unix())
	out, err := os.Create(fname)
	if err != nil {
		panic(err)
	}
	out.Write(data)
	out.Close()
	fmt.Printf("Wrote %v\n", fname)
}
