/*
Copyright © 2022 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	hdrhistogram "github.com/HdrHistogram/hdrhistogram-go"
	"github.com/redis/rueidis"
	"github.com/spf13/cobra"
	"io"
	"log"
	"os"
	"os/signal"
	"strings"
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
		indexSearch, _ := cmd.Flags().GetBool(REDIS_IDX_PROPERTY)
		indexSearchName, _ := cmd.Flags().GetString(REDIS_IDX_NAME_PROPERTY)
		db, _ := cmd.Flags().GetString("db")
		input, _ := cmd.Flags().GetString("input")
		inputType, _ := cmd.Flags().GetString("input-type")
		uri, _ := cmd.Flags().GetString("uri")
		concurrency, _ := cmd.Flags().GetInt("concurrency")
		requests, _ := cmd.Flags().GetInt("requests")
		redisGeoKeyname, _ := cmd.Flags().GetString(REDIS_GEO_KEYNAME_PROPERTY)
		validateDB(db)
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
			buf := make([]byte, 512*1024*1024)
			scanner.Buffer(buf, 512*1024*1024)

			n := 0
			for scanner.Scan() {
				finalInputLine := scanner.Text()
				workQueue <- finalInputLine
				n = n + 1
				if n >= nDatapoints {
					break
				}
			}

			// Close the channel so everyone reading from it knows we're done.
			close(workQueue)
		}()

		var geoCommands uint64
		if strings.Compare(inputType, INPUT_TYPE_GEOSHAPE) == 0 {
			setupStageGeoShape(uri, db, indexSearch, indexSearchName, INDEX_FIELDNAME_GEOSHAPE)
			// geopoint
		} else {
			setupStageGeoPoint(uri, db, indexSearch, indexSearchName, INDEX_FIELDNAME_GEOPOINT)
		}

		// listen for C-c
		controlC := make(chan os.Signal, 1)
		signal.Notify(controlC, os.Interrupt)
		client_update_tick := 1
		tick := time.NewTicker(time.Duration(client_update_tick) * time.Second)
		start := time.Now()
		// Now read them all off, concurrently.
		for i := 0; i < concurrency; i++ {
			// geoshape
			if strings.Compare(inputType, INPUT_TYPE_GEOSHAPE) == 0 {
				go loadWorkerGeoshape(uri, workQueue, complete, &geoCommands, datapointsChan, uint64(nDatapoints), db, INDEX_FIELDNAME_GEOSHAPE)
				// geopoint
			} else {
				go loadWorkerGeopoint(uri, workQueue, complete, &geoCommands, datapointsChan, uint64(nDatapoints), db, redisGeoKeyname, INDEX_FIELDNAME_GEOPOINT)
			}

			// delay the creation 1ms for each additional client
			time.Sleep(time.Millisecond * 1)
		}

		_, _, duration, totalMessages, _, _, _ := updateCLI(tick, controlC, uint64(nDatapoints), false, datapointsChan, start, 0)
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
		fmt.Println(fmt.Sprintf("Finished inserting %d geo points", geoCommands))
	},
}

func validateDB(db string) {
	switch db {
	case REDIS_TYPE_JSON:
		log.Printf("Using %s db...", db)
	case REDIS_TYPE_HASH:
		log.Printf("Using %s db...", db)
	case REDIS_TYPE_GENERIC:
		log.Printf("Using %s db...", db)
	case REDIS_TYPE_GEO:
		log.Printf("Using %s db...", db)
	default:
		log.Fatal(fmt.Sprintf("DB was not recognized. Exiting..."))
	}
}

func setupStageGeoShape(uri, db string, indexSearch bool, indexName, fieldName string) {
	c, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress: []string{uri},
	})
	defer c.Close()
	ctx := context.Background()
	fieldPath := fmt.Sprintf("$.%s", fieldName)
	log.Printf("Starting setup stage for %s DB. Sending setup commands...\n", db)
	switch db {
	case REDIS_TYPE_JSON:
		if indexSearch {
			log.Printf("Creating redisearch index ON JSON named %s on field %s\n", indexName, fieldName)
			err = c.Do(ctx, c.B().FtCreate().Index(indexName).OnJson().Schema().FieldName(fieldPath).As(fieldName).Geometry().Build()).Error()
		} else {
			log.Printf("Skipping the creation of redisearch index %s.\n", indexName)
		}
	case REDIS_TYPE_HASH:
		if indexSearch {
			log.Printf("Creating redisearch index ON HASH named %s on field %s\n", indexName, fieldName)
			err = c.Do(ctx, c.B().FtCreate().Index(indexName).OnHash().Schema().FieldName(fieldName).Geometry().Build()).Error()
		} else {
			log.Printf("Skipping the creation of redisearch index %s.\n", indexName)
		}
	default:
		log.Fatal(fmt.Sprintf("DB was not recognized. Exiting..."))
	}
	if err != nil {
		log.Fatal(fmt.Sprintf("Received error on setup stage: '%s'. Exiting...", err.Error()))
	}
	log.Printf("Finished setup stage for %s DB\n", db)
}

func setupStageGeoPoint(uri, db string, indexSearch bool, indexName, fieldname string) {
	c, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress: []string{uri},
	})
	defer c.Close()
	ctx := context.Background()
	log.Printf("Starting setup stage for %s DB. Sending setup commands...\n", db)
	switch db {
	case REDIS_TYPE_JSON:
		if indexSearch {
			log.Printf("Creating redisearch index named %s.\n", indexName)
			err = c.Do(ctx, c.B().FtCreate().Index(indexName).OnJson().Schema().FieldName("$.location").As(fieldname).Geo().Build()).Error()
		} else {
			log.Printf("Skipping the creation of redisearch index %s.\n", indexName)
		}
	case REDIS_TYPE_HASH:
		if indexSearch {
			log.Printf("Creating redisearch index named %s.\n", indexName)
			err = c.Do(ctx, c.B().FtCreate().Index(indexName).OnHash().Schema().FieldName(fieldname).Geo().Build()).Error()
		} else {
			log.Printf("Skipping the creation of redisearch index %s.\n", indexName)
		}
	case REDIS_TYPE_GENERIC:
		log.Printf("No setup for %s DB\n", db)
		fallthrough
	case REDIS_TYPE_GEO:
		log.Printf("No setup for %s DB\n", db)
	default:
		log.Fatal(fmt.Sprintf("DB was not recognized. Exiting..."))
	}
	if err != nil {
		log.Fatal(fmt.Sprintf("Received error on setup stage: '%s'. Exiting...", err.Error()))
	}
	log.Printf("Finished setup stage for %s DB\n", db)
}

func updateCLI(tick *time.Ticker, c chan os.Signal, message_limit uint64, loop bool, datapointsChan chan datapoint, start time.Time, testTime int) (bool, time.Time, time.Duration, uint64, []float64, map[int]int, float64) {
	var currentErr uint64 = 0
	var currentCount uint64 = 0
	var currentReplySize int64 = 0
	prevTime := time.Now()
	prevMessageCount := uint64(0)
	messageRateTs := []float64{}
	var histogram = make(map[int]int)
	var dp datapoint
	fmt.Printf("%26s %7s %25s %25s %7s %25s %25s\n", "Test time", " ", "Total Commands", "Total Errors", "", "Command Rate", "p50 lat. (msec)")
	for {
		if testTime > 0 && time.Now().Sub(start).Seconds() > float64(testTime) {
			fmt.Println("\nReached test limit - shutting down")
			return true, start, time.Since(start), totalCommands, messageRateTs, histogram, float64(currentReplySize / int64(totalCommands))
		}

		select {
		case dp = <-datapointsChan:
			{
				latencies.RecordValue(dp.duration_ms)
				if !dp.success {
					currentErr++
				}
				currentCount++
				histogram[int(dp.resultset_size)]++
				currentReplySize += dp.resultset_size
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
					if testTime > 0 {
						currentTestDurationSec := time.Now().Sub(start).Seconds()
						completionPercent = currentTestDurationSec / float64(testTime) * 100.0
					}
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
					return true, start, time.Since(start), totalCommands, messageRateTs, histogram, float64(currentReplySize / int64(totalCommands))
				}

				break
			}

		case <-c:
			fmt.Println("\nreceived Ctrl-c - shutting down")
			return true, start, time.Since(start), totalCommands, messageRateTs, histogram, float64(currentReplySize / int64(totalCommands))
		}
	}
}

func init() {
	rootCmd.AddCommand(loadCmd)
	loadCmd.Flags().StringP("db", "", REDIS_TYPE_GEO, fmt.Sprintf("Database to load the data to. One of %s", strings.Join([]string{REDIS_TYPE_GEO, REDIS_TYPE_JSON, REDIS_TYPE_HASH}, ",")))
	loadCmd.Flags().StringP("input", "i", "documents.json", "Input json file")
	loadCmd.Flags().StringP("input-type", "", DEFAULT_INPUT_TYPE, "Input type. One of 'geopoint' or 'geoshape'")
	loadCmd.Flags().IntP("concurrency", "c", 50, "Concurrency")
	loadCmd.Flags().IntP("requests", "n", -1, "Requests. If -1 then it will use all input datapoints")
	loadCmd.Flags().StringP("uri", "u", "localhost:6379", "Server URI")
	loadCmd.Flags().BoolP("cluster", "", false, "Enable cluster mode")
	loadCmd.Flags().BoolP(REDIS_IDX_PROPERTY, "", true, "Enable redisearch secondary index on HASH and JSON datatypes")
	loadCmd.Flags().StringP(REDIS_IDX_NAME_PROPERTY, "", REDIS_DEFAULT_IDX_NAME, "redisearch secondary index name")
	loadCmd.Flags().StringP(REDIS_GEO_KEYNAME_PROPERTY, "", REDIS_GEO_DEFAULT_KEYNAME, "redis GEO keyname")
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

func loadWorkerGeopoint(uri string, queue chan string, complete chan bool, ops *uint64, datapointsChan chan datapoint, totalDatapoints uint64, db string, redisGeoKeyname string, fieldName string) {
	c, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress: []string{uri},
	})
	if err != nil {
		panic(err)
	}
	defer c.Close()
	ctx := context.Background()
	for line := range queue {
		lon, lat := lineToLonLat(line)
		previousOpsVal := atomic.LoadUint64(ops)
		if previousOpsVal >= totalDatapoints {
			break
		}
		atomic.AddUint64(ops, 1)

		opsVal := atomic.LoadUint64(ops)
		memberS := fmt.Sprintf("%d", opsVal)
		startT := time.Now()
		switch db {
		case REDIS_TYPE_JSON:
			err = c.Do(ctx, c.B().JsonSet().Key(memberS).Path("$").Value(fmt.Sprintf("{\"%s\":\"%f,%f\"}", fieldName, lon, lat)).Build()).Error()
		case REDIS_TYPE_HASH:
			err = c.Do(ctx, c.B().Hset().Key(memberS).FieldValue().FieldValue(fieldName, fmt.Sprintf("%f,%f", lon, lat)).Build()).Error()
		case REDIS_TYPE_GENERIC:
			fallthrough
		case REDIS_TYPE_GEO:
			fallthrough
		default:
			err = c.Do(ctx, c.B().Geoadd().Key(redisGeoKeyname).LongitudeLatitudeMember().LongitudeLatitudeMember(lon, lat, memberS).Build()).Error()
		}
		endT := time.Now()

		duration := endT.Sub(startT)
		datapointsChan <- datapoint{!(err != nil), duration.Microseconds(), 0}

	}
	// Let the main process know we're done.
	complete <- true
}

func loadWorkerGeoshape(uri string, queue chan string, complete chan bool, ops *uint64, datapointsChan chan datapoint, totalDatapoints uint64, db string, fieldName string) {
	c, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress: []string{uri},
	})
	if err != nil {
		panic(err)
	}
	defer c.Close()
	ctx := context.Background()
	for line := range queue {
		polygon := lineToPolygon(line)
		previousOpsVal := atomic.LoadUint64(ops)
		if previousOpsVal >= totalDatapoints {
			break
		}
		atomic.AddUint64(ops, 1)

		opsVal := atomic.LoadUint64(ops)
		memberS := fmt.Sprintf("%d", opsVal)
		startT := time.Now()
		switch db {
		case REDIS_TYPE_JSON:
			err = c.Do(ctx, c.B().JsonSet().Key(memberS).Path("$").Value(fmt.Sprintf("{\"%s\":\"%s\"}", fieldName, polygon)).Build()).Error()
		case REDIS_TYPE_HASH:
			fallthrough
		default:
			err = c.Do(ctx, c.B().Hset().Key(memberS).FieldValue().FieldValue(fieldName, polygon).Build()).Error()

		}
		endT := time.Now()

		duration := endT.Sub(startT)
		datapointsChan <- datapoint{!(err != nil), duration.Microseconds(), 0}

	}
	// Let the main process know we're done.
	complete <- true
}
