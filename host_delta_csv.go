package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"log"
	"net/url"
	"os"
	"strings"
	"time"
)

/*
Scan crawl.log and print csv containing max, min and average timedeltas
between requests to hosts.
*/

const (
	timestampFieldIndex = 0
	urlFieldIndex       = 3
)

func main() {
	if len(os.Args) < 2 {
		log.Fatalln("Needs path to crawl.log as parameter!")
	}

	// Init scanning part
	inputFile := os.Args[1]

	f, err := os.Open(inputFile)
	if err != nil {
		log.Fatalln(err)
	}
	defer f.Close()

	line := bufio.NewScanner(f)

	hostTimestamps := make(map[string][]time.Time, 0)

	// Scanning part
	for line.Scan() {
		fields := strings.Fields(line.Text())

		if strings.HasPrefix(fields[urlFieldIndex], "dns:") {
			continue
		}

		url, err := url.ParseRequestURI(fields[urlFieldIndex])
		if err != nil {
			log.Println(err)
			continue
		}

		timestamp, err := time.Parse(time.RFC3339, fields[timestampFieldIndex])
		if err != nil {
			log.Println(err)
			continue
		}

		_, ok := hostTimestamps[url.Host]
		if !ok {
			hostTimestamps[url.Host] = make([]time.Time, 0, 8)
		}
		hostTimestamps[url.Host] = append(hostTimestamps[url.Host], timestamp)
	}
	if line.Err() != nil {
		log.Fatalln(err)
	}

	// Init writing part
	w := csv.NewWriter(os.Stdout)
	w.Comma = ','
	w.UseCRLF = true
	defer func() {
		w.Flush()
		if w.Error() != nil {
			log.Fatalln(w.Error())
		}
	}()

	header := []string{"host", "max_delay_s", "min_delay_s", "average_delay_s", "NO_timestamps"}
	err = w.Write(header)

	// Writing part
	for host, timestamps := range hostTimestamps {
		var maxDelay time.Duration
		var minDelay time.Duration = time.Duration(1 << 62)
		var totalDelay time.Duration
		var averageDelay time.Duration

		for i := 1; i < len(timestamps); i++ {
			delay := timestamps[i].Sub(timestamps[i-1])

			if maxDelay < delay {
				maxDelay = delay
			}

			if delay < minDelay {
				minDelay = delay
			}

			totalDelay += delay
		}

		fields := make([]string, 0, len(header))
		fields = append(fields, host)
		fields = append(fields, fmt.Sprint(maxDelay.Seconds()))
		fields = append(fields, fmt.Sprint(minDelay.Seconds()))
		if len(timestamps) < 2 {
			averageDelay = 0
		} else {
			averageDelay = totalDelay/time.Duration(len(timestamps)-1)
		}
		fields = append(fields, fmt.Sprint(averageDelay.Seconds()))
		numberOfTimestamps := len(timestamps)
		fields = append(fields, fmt.Sprint(numberOfTimestamps))

		w.Write(fields)
	}
}
