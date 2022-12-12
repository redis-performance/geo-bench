/*
Copyright © 2022 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	hdrhistogram "github.com/HdrHistogram/hdrhistogram-go"
	"github.com/rueian/rueidis"
	"github.com/spf13/cobra"
	"io"
	"log"
	"os"
	"os/signal"
	"sync/atomic"
	"time"
)

var totalCommands uint64
var totalErrors uint64
var latencies *hdrhistogram.Histogram

// loadCmd represents the load command
var loadCmd = &cobra.Command{
	Use:   "load",
	Short: "Benchmark indexing operations of geographic coordinates.",
	Long:  `This command covers indexing operations of geographic coordinates.`,
	Run: func(cmd *cobra.Command, args []string) {
		db, _ := cmd.Flags().GetString("db")
		input, _ := cmd.Flags().GetString("input")
		uri, _ := cmd.Flags().GetString("uri")
		concurrency, _ := cmd.Flags().GetInt("concurrency")
		requests, _ := cmd.Flags().GetInt("requests")
		log.Printf("Using %d concurrent workers to ingest datapoints", concurrency)
		latencies = hdrhistogram.New(1, 90000000, 3)
		file, err := os.Open(input)
		nLines, err := LineCounter(file)
		if err != nil {
			log.Fatal(err)
		}
		file.Close()
		log.Printf("There are a total of %d datapoints in %s", nLines, input)

		nDatapoints := requests
		if nLines < nDatapoints || nDatapoints < 0 {
			nDatapoints = nLines
		}
		datapointsChan := make(chan datapoint, nDatapoints)
		workQueue := make(chan string)

		// We need to know when everyone is done so we can exit.
		complete := make(chan bool)

		// Read the lines into the work queue.
		go func() {
			file, err = os.Open(input)
			if err != nil {
				log.Fatal(err)
			}
			// Close when the functin returns
			defer file.Close()

			scanner := bufio.NewScanner(file)

			for scanner.Scan() {
				workQueue <- scanner.Text()
			}

			// Close the channel so everyone reading from it knows we're done.
			close(workQueue)
		}()

		var geopoints uint64
		setupStage(uri, db)
		// listen for C-c
		controlC := make(chan os.Signal, 1)
		signal.Notify(controlC, os.Interrupt)
		client_update_tick := 1
		tick := time.NewTicker(time.Duration(client_update_tick) * time.Second)
		start := time.Now()
		// Now read them all off, concurrently.
		for i := 0; i < concurrency; i++ {
			go startWorking(uri, workQueue, complete, &geopoints, datapointsChan, uint64(nDatapoints), db)
			// delay the creation 1ms for each additional client
			time.Sleep(time.Millisecond * 1)
		}

		_, _, duration, totalMessages, _ := updateCLI(tick, controlC, uint64(nDatapoints), false, datapointsChan, start)
		messageRate := float64(totalMessages) / float64(duration.Seconds())
		avgMs := float64(latencies.Mean()) / 1000.0
		p50IngestionMs := float64(latencies.ValueAtQuantile(50.0)) / 1000.0
		p95IngestionMs := float64(latencies.ValueAtQuantile(95.0)) / 1000.0
		p99IngestionMs := float64(latencies.ValueAtQuantile(99.0)) / 1000.0
		log.Printf("Finished load stage for %s DB\n", db)

		fmt.Printf("\n")
		fmt.Printf("#################################################\n")
		fmt.Printf("Total Duration %.3f Seconds\n", duration.Seconds())
		fmt.Printf("Total Datapoints %d\n", totalCommands)
		fmt.Printf("Total Errors %d\n", totalErrors)
		fmt.Printf("Throughput summary: %.0f requests per second\n", messageRate)
		fmt.Printf("Latency summary (msec):\n")
		fmt.Printf("    %9s %9s %9s %9s\n", "avg", "p50", "p95", "p99")
		fmt.Printf("    %9.3f %9.3f %9.3f %9.3f\n", avgMs, p50IngestionMs, p95IngestionMs, p99IngestionMs)
		fmt.Println(fmt.Sprintf("Finished inserting %d geo points", geopoints))
	},
}

func setupStage(uri, db string) {
	c, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress: []string{uri},
	})
	defer c.Close()
	ctx := context.Background()
	log.Printf("Starting setup stage for %s DB. Sending setup commands...\n", db)
	switch db {
	case "redisearch-json":
		err = c.Do(ctx, c.B().FtCreate().Index("idx").OnJson().Schema().FieldName("$.location").As("location").Geo().Build()).Error()
	case "redisearch-hash":
		err = c.Do(ctx, c.B().FtCreate().Index("idx").OnHash().Schema().FieldName("location").Geo().Build()).Error()
	case "redis":
		log.Printf("No setup for %s DB\n", db)
		fallthrough
	case "redis-geo":
		log.Printf("No setup for %s DB\n", db)
	default:
		log.Fatal(fmt.Sprintf("DB was not recognized. Exiting..."))
	}
	if err != nil {
		log.Fatal(fmt.Sprintf("Received error on setup stage: '%s'. Exiting...", err.Error()))
	}
	log.Printf("Finished setup stage for %s DB\n", db)
}

func updateCLI(tick *time.Ticker, c chan os.Signal, message_limit uint64, loop bool, datapointsChan chan datapoint, start time.Time) (bool, time.Time, time.Duration, uint64, []float64) {
	var currentErr uint64 = 0
	var currentCount uint64 = 0
	prevTime := time.Now()
	prevMessageCount := uint64(0)
	messageRateTs := []float64{}
	var dp datapoint
	fmt.Printf("%26s %7s %25s %25s %7s %25s %25s\n", "Test time", " ", "Total Commands", "Total Errors", "", "Command Rate", "p50 lat. (msec)")
	for {
		select {
		case dp = <-datapointsChan:
			{
				latencies.RecordValue(dp.duration_ms)
				if !dp.success {
					currentErr++
				}
				currentCount++
			}
		case <-tick.C:
			{
				totalCommands += currentCount
				totalErrors += currentErr
				currentErr = 0
				currentCount = 0
				now := time.Now()
				took := now.Sub(prevTime)
				messageRate := float64(totalCommands-prevMessageCount) / float64(took.Seconds())
				completionPercentStr := "[----%]"
				if !loop {
					completionPercent := float64(totalCommands) / float64(message_limit) * 100.0
					completionPercentStr = fmt.Sprintf("[%3.1f%%]", completionPercent)
				}
				errorPercent := float64(totalErrors) / float64(totalCommands) * 100.0

				p50 := float64(latencies.ValueAtQuantile(50.0)) / 1000.0

				if totalCommands != 0 {
					messageRateTs = append(messageRateTs, messageRate)
				}
				prevMessageCount = totalCommands
				prevTime = now

				fmt.Printf("%25.0fs %s %25d %25d [%3.1f%%] %25.2f %25.2f\t", time.Since(start).Seconds(), completionPercentStr, totalCommands, totalErrors, errorPercent, messageRate, p50)
				fmt.Printf("\r")
				if message_limit > 0 && totalCommands >= message_limit && !loop {
					return true, start, time.Since(start), totalCommands, messageRateTs
				}

				break
			}

		case <-c:
			fmt.Println("\nreceived Ctrl-c - shutting down")
			return true, start, time.Since(start), totalCommands, messageRateTs
		}
	}
}

func init() {
	rootCmd.AddCommand(loadCmd)
	loadCmd.Flags().StringP("db", "", "redis", "Database to load the data to")
	loadCmd.Flags().StringP("input", "i", "documents.json", "Input json file")
	loadCmd.Flags().IntP("concurrency", "c", 50, "Concurrency")
	loadCmd.Flags().IntP("requests", "n", -1, "Requests. If -1 then it will use all input datapoints")
	loadCmd.Flags().StringP("uri", "u", "localhost:6379", "Server URI")
	loadCmd.Flags().BoolP("cluster", "", false, "Enable cluster mode")
}

type GeoPoint struct {
	LatLon []float64 `json:"location"`
}

func LineCounter(r io.Reader) (int, error) {

	var count int
	const lineBreak = '\n'

	buf := make([]byte, bufio.MaxScanTokenSize)

	for {
		bufferSize, err := r.Read(buf)
		if err != nil && err != io.EOF {
			return 0, err
		}

		var buffPosition int
		for {
			i := bytes.IndexByte(buf[buffPosition:], lineBreak)
			if i == -1 || bufferSize == buffPosition {
				break
			}
			buffPosition += i + 1
			count++
		}
		if err == io.EOF {
			break
		}
	}

	return count, nil
}

type datapoint struct {
	success     bool
	duration_ms int64
}

func startWorking(uri string, queue chan string, complete chan bool, ops *uint64, datapointsChan chan datapoint, totalDatapoints uint64, db string) {
	c, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress: []string{uri},
	})
	if err != nil {
		panic(err)
	}
	defer c.Close()
	ctx := context.Background()
	for line := range queue {
		var geo GeoPoint
		json.Unmarshal([]byte(line), &geo)
		lon := geo.LatLon[0]
		lat := geo.LatLon[1]
		previousOpsVal := atomic.LoadUint64(ops)
		if previousOpsVal >= totalDatapoints {
			break
		}
		atomic.AddUint64(ops, 1)

		opsVal := atomic.LoadUint64(ops)
		memberS := fmt.Sprintf("%d", opsVal)
		startT := time.Now()
		switch db {
		case "redisearch-json":
			err = c.Do(ctx, c.B().JsonSet().Key(memberS).Path("$").Value(fmt.Sprintf("{\"location\":\"%f,%f\"}", lon, lat)).Build()).Error()
		case "redisearch-hash":
			err = c.Do(ctx, c.B().Hset().Key(memberS).FieldValue().FieldValue("location", fmt.Sprintf("%f,%f", lon, lat)).Build()).Error()
		case "redis":
			fallthrough
		case "redis-geo":
			fallthrough
		default:
			err = c.Do(ctx, c.B().Geoadd().Key("key").LongitudeLatitudeMember().LongitudeLatitudeMember(lon, lat, memberS).Build()).Error()
		}
		endT := time.Now()

		duration := endT.Sub(startT)
		datapointsChan <- datapoint{!(err != nil), duration.Microseconds()}

	}
	// Let the main process know we're done.
	complete <- true
}
