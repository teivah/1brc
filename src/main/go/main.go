package main

import (
	"bufio"
	"fmt"
	"os"
	"runtime"
	"runtime/debug"
	"runtime/trace"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const numLines = 100_000

func main() {
	file := "file.trace"
	f, _ := os.Create(file)
	defer f.Close()
	trace.Start(f)
	defer trace.Stop()

	filename := os.Args[1]
	if err := run(filename); err != nil {
		panic(err)
	}
}

type data struct {
	min   float64
	max   float64
	sum   float64
	count int
}

func run(filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	const maxCapacity = 512 * 1024 // 512KB
	buf := make([]byte, maxCapacity)
	scanner.Buffer(buf, maxCapacity)

	debug.SetGCPercent(800)
	workers := runtime.NumCPU()
	input := make(chan []string, 100)
	output := make(chan map[string]*data, workers)
	wg := &sync.WaitGroup{}
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go worker(input, output, wg)
	}

	var lines []string
	var i int
	for {
		lines = make([]string, 0, numLines)
		for i = 0; i < numLines; i++ {
			if !scanner.Scan() {
				break
			}
			lines = append(lines, scanner.Text())
		}
		if len(lines) == 0 {
			break
		}
		input <- lines
	}
	close(input)

	go func() {
		wg.Wait()
		close(output)
	}()

	var m map[string]*data
	for m2 := range output {
		if m == nil {
			m = m2
			continue
		}
		var (
			k  string
			v2 *data
		)
		for k, v2 = range m2 {
			v, contains := m[k]
			if !contains {
				m[k] = v2
				continue
			}

			v.min = min(v.min, v2.min)
			v.max = max(v.max, v2.max)
			v.sum += v2.sum
			v.count += v2.count
		}
	}

	names := make([]string, len(m))
	i = 0
	var city string
	for city = range m {
		names[i] = city
		i++
	}

	sort.Strings(names)
	fmt.Print("{")
	for i := 0; i < len(names)-1; i++ {
		d := m[names[i]]
		mean := d.sum / float64(d.count)
		fmt.Printf("%s=%.1f/%.1f/%.1f, ", names[i], d.min, mean, d.max)
	}

	d := m[names[len(names)-1]]
	mean := d.sum / float64(d.count)
	fmt.Printf("%s=%.1f/%.1f/%.1f", names[len(names)-1], d.min, mean, d.max)
	fmt.Println("}")

	return nil
}

func worker(input <-chan []string, output chan<- map[string]*data, wg *sync.WaitGroup) {
	defer wg.Done()
	m := make(map[string]*data, numLines)
	var (
		lines  []string
		line   string
		ok     bool
		split  int
		city   string
		s      string
		value  float64
		v      *data
		exists bool
		i      int
	)
	for true {
		lines, ok = <-input
		if !ok {
			break
		}
		i = 0
		for ; i < len(lines); i++ {
			line = lines[i]
			split = strings.Index(line, ";")
			city = line[:split]
			s = line[split+1:]
			value, _ = strconv.ParseFloat(s, 64)
			v, exists = m[city]
			if !exists {
				v = &data{
					min: 100.,
					max: -100.,
				}
				m[city] = v
			}
			v.min = min(v.min, value)
			v.max = max(v.max, value)
			v.sum += value
			v.count++
		}
	}
	output <- m
}
