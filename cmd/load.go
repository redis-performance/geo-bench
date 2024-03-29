/*
Copyright © 2022 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"bufio"
	"bytes"
	"context"
	elastic "filipecosta90/geo-bench/cmd/elasticsearch"
	"filipecosta90/geo-bench/cmd/redis"
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
var activeClients uint64
var totalErrors uint64
var latencies *hdrhistogram.Histogram

// loadCmd represents the load command
var loadCmd = &cobra.Command{
	Use:   "load",
	Short: "Benchmark indexing operations of geographic coordinates.",
	Long:  `This command covers indexing operations of geographic coordinates.`,
	Run: func(cmd *cobra.Command, args []string) {
		pflags := cmd.Flags()
		db, _ := pflags.GetString("db")
		input, _ := pflags.GetString("input")
		inputType, _ := pflags.GetString("input-type")
		concurrency, _ := pflags.GetInt("concurrency")
		requests, _ := pflags.GetInt("requests")
		debugLevel, _ := pflags.GetInt("debug")
		cmdTimeout, _ := pflags.GetInt64(redis.REDIS_COMMAND_TIMEOUT)
		connWriteTimeout := time.Duration(cmdTimeout) * time.Millisecond

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

		var finishedCommands uint64 = 0
		var issuedCommands uint64 = 0
		var activeConns int64 = 0
		// TODO: agnostic setup and benchmark
		// ElasticSearch
		if strings.Compare(db, ELASTIC_TYPE_GENERIC) == 0 {
			log.Printf("Detected ElasticSearch DB\n")
			c := elastic.ElasticCreator{}
			_, err := c.Create(pflags, "load")
			if err != nil {
				log.Fatal(err)
			}

			// Redis
		} else {
			log.Printf("Detected Redis DB\n")
			indexSearch, _ := pflags.GetBool(REDIS_IDX_PROPERTY)
			indexSearchName, _ := pflags.GetString(redis.REDIS_IDX_NAME_PROPERTY)
			uri, _ := pflags.GetString(redis.REDIS_URI_PROPERTY)
			password, _ := pflags.GetString(redis.REDIS_PASSWORD_PROPERTY)
			if strings.Compare(inputType, INPUT_TYPE_GEOSHAPE) == 0 {
				setupStageGeoShape(uri, password, db, indexSearch, indexSearchName, INDEX_FIELDNAME_GEOSHAPE, connWriteTimeout)
				// geopoint
			} else {
				setupStageGeoPoint(uri, password, db, indexSearch, indexSearchName, INDEX_FIELDNAME_GEOPOINT, connWriteTimeout)
			}
		}

		redisGeoKeyname, _ := pflags.GetString(REDIS_GEO_KEYNAME_PROPERTY)
		password, _ := pflags.GetString(redis.REDIS_PASSWORD_PROPERTY)
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
				if strings.Compare(db, ELASTIC_TYPE_GENERIC) == 0 {
					var elasticWrapper *elastic.ElasticWrapper = nil
					c := elastic.ElasticCreator{}
					elasticWrapper, err = c.Create(pflags, "run")
					if err != nil {
						log.Fatal(err)
					}
					go loadWorkerGeoshapeElastic(elasticWrapper, workQueue, complete, &issuedCommands, &finishedCommands, &activeConns, datapointsChan, uint64(nDatapoints), INDEX_FIELDNAME_GEOSHAPE)
				} else {
					uri, _ := pflags.GetString(redis.REDIS_URI_PROPERTY)
					go loadWorkerGeoshape(uri, password, workQueue, complete, &finishedCommands, datapointsChan, uint64(nDatapoints), db, INDEX_FIELDNAME_GEOSHAPE, debugLevel, connWriteTimeout)
				}
				// geopoint
			} else {
				uri, _ := pflags.GetString(redis.REDIS_URI_PROPERTY)
				go loadWorkerGeopoint(uri, password, workQueue, complete, &finishedCommands, datapointsChan, uint64(nDatapoints), db, redisGeoKeyname, INDEX_FIELDNAME_GEOPOINT, connWriteTimeout)
			}

			// delay the creation 1ms for each additional client
			time.Sleep(time.Millisecond * 1)
		}

		_, _, duration, totalMessages, _, _, _ := updateCLI(tick, controlC, uint64(nDatapoints), &activeConns, false, datapointsChan, start, 0)
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
		fmt.Println(fmt.Sprintf("Finished sending %d commands for input type %s", finishedCommands, inputType))
	},
}

func validateDB(db string) {
	switch db {
	case ELASTIC_TYPE_GENERIC:
		log.Printf("Using %s db...", db)
	case REDIS_TYPE_JSON:
		log.Printf("Using %s db...", db)
	case REDIS_TYPE_HASH:
		log.Printf("Using %s db...", db)
	case REDIS_TYPE_GENERIC:
		log.Printf("Using %s db...", db)
	case REDIS_TYPE_GEO:
		log.Printf("Using %s db...", db)
	default:
		log.Fatal(fmt.Sprintf("DB was not recognized during validation stage. Exiting..."))
	}
}

func setupStageGeoShape(uri, password, db string, indexSearch bool, indexName, fieldName string, connWriteTimeout time.Duration) {
	c, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress:      []string{uri},
		DisableCache:     true,
		AlwaysRESP2:      true,
		Password:         password,
		ConnWriteTimeout: connWriteTimeout,
	})
	defer c.Close()
	ctx := context.Background()
	fieldPath := fmt.Sprintf("$.%s", fieldName)
	log.Printf("Checking we can connect to the DB. Sending PING commands...\n")
	c.Do(ctx, c.B().Ping().Build()).Error()
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

func setupStageGeoPoint(uri, password, db string, indexSearch bool, indexName, fieldname string, connWriteTimeout time.Duration) {
	c, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress:      []string{uri},
		DisableCache:     true,
		AlwaysRESP2:      true,
		Password:         password,
		ConnWriteTimeout: connWriteTimeout,
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

func updateCLI(tick *time.Ticker, c chan os.Signal, message_limit uint64, activeConnsPtr *int64, loop bool, datapointsChan chan datapoint, start time.Time, testTime int) (bool, time.Time, time.Duration, uint64, []float64, map[int64]int64, float64) {
	var currentErr uint64 = 0
	var currentCount uint64 = 0
	var currentReplySize int64 = 0
	prevTime := time.Now()
	prevMessageCount := uint64(0)
	messageRateTs := []float64{}
	var histogram = make(map[int64]int64)
	var dp datapoint
	fmt.Printf("%26s %7s %25s %25s %7s %25s %25s %25s\n", "Test time", " ", "Total Commands", "Total Errors", "", "Command Rate", "p50 lat. (msec)", "Active Conns")
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
				histogram[int64(dp.resultset_size)]++
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

				activeConns := atomic.LoadInt64(activeConnsPtr)

				fmt.Printf("%25.0fs %s %25d %25d [%3.1f%%] %25.2f %25.2f %25d\t", time.Since(start).Seconds(), completionPercentStr, totalCommands, totalErrors, errorPercent, messageRate, p50, activeConns)
				fmt.Printf("\r")
				if message_limit > 0 && totalCommands >= message_limit && !loop {
					return true, start, time.Since(start), totalCommands, messageRateTs, histogram, float64(currentReplySize / int64(totalCommands))
				}

				break
			}

		case <-c:
			fmt.Println("\nreceived Ctrl-c - shutting down")
			avgReplySize := 0.0
			if totalCommands > 0 {
				avgReplySize = float64(currentReplySize / int64(totalCommands))
			}
			return true, start, time.Since(start), totalCommands, messageRateTs, histogram, avgReplySize
		}
	}
}

func init() {
	rootCmd.AddCommand(loadCmd)
	flags := loadCmd.Flags()
	flags.StringP("db", "", REDIS_TYPE_GEO, fmt.Sprintf("Database to load the data to. One of %s", strings.Join([]string{REDIS_TYPE_GEO, ELASTIC_TYPE_GENERIC, REDIS_TYPE_JSON, REDIS_TYPE_HASH}, ",")))
	flags.StringP("input", "i", "documents.json", "Input json file")
	flags.StringP("input-type", "", DEFAULT_INPUT_TYPE, "Input type. One of 'geopoint' or 'geoshape'")
	flags.IntP("concurrency", "c", 50, "Concurrency")
	flags.IntP("requests", "n", -1, "Requests. If -1 then it will use all input datapoints")
	redis.RegisterRedisLoadFlags(flags)
	elastic.RegisterElasticLoadFlags(flags)
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

func loadWorkerGeopoint(uri, password string, queue chan string, complete chan bool, ops *uint64, datapointsChan chan datapoint, totalDatapoints uint64, db string, redisGeoKeyname string, fieldName string, connWriteTimeout time.Duration) {
	c, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress:      []string{uri},
		DisableCache:     true,
		AlwaysRESP2:      true,
		Password:         password,
		ConnWriteTimeout: connWriteTimeout,
	})
	if err != nil {
		panic(err)
	}
	defer c.Close()
	var r = redis.RedisWrapper{
		c,
		db,
	}
	ctx := context.Background()
	for line := range queue {
		lon, lat := lineToLonLat(line)
		previousOpsVal := atomic.LoadUint64(ops)
		if previousOpsVal >= totalDatapoints {
			break
		}
		opsVal := atomic.AddUint64(ops, 1)
		documentId := fmt.Sprintf("%d", opsVal)
		startT := time.Now()
		err = r.InsertGeoPoint(ctx, documentId, fieldName, lon, lat)
		endT := time.Now()

		duration := endT.Sub(startT)
		datapointsChan <- datapoint{!(err != nil), duration.Microseconds(), 0}

	}
	// Let the main process know we're done.
	complete <- true
}

func loadWorkerGeoshapeElastic(ec *elastic.ElasticWrapper, queue chan string, complete chan bool, docId, ops *uint64, activeConns *int64, datapointsChan chan datapoint, totalDatapoints uint64, fieldName string) {
	var err error = nil
	atomic.AddInt64(activeConns, 1)
	ctx := context.Background()
	for line := range queue {
		polygon := lineToPolygon(line)

		previousOpsVal := atomic.LoadUint64(ops)
		if previousOpsVal >= totalDatapoints {
			break
		}
		opsVal := atomic.AddUint64(docId, 1)
		documentId := fmt.Sprintf("%d", opsVal)
		startT := time.Now()
		err = ec.InsertPolygon(ctx, documentId, fieldName, polygon, ops)
		endT := time.Now()
		duration := endT.Sub(startT)
		datapointsChan <- datapoint{!(err != nil), duration.Microseconds(), 0}
	}
	ec.CleanupThread(ctx)
	atomic.AddInt64(activeConns, -1)
	// Let the main process know we're done.
	complete <- true
}

func queryWorkerGeoshapeElastic(ec *elastic.ElasticWrapper, queue chan string, complete chan bool, sentCommands, finishedCommands *uint64, activeConns *int64, datapointsChan chan datapoint, totalDatapoints uint64, queryType, fieldName string, debug int) {
	relation := "contains"
	if strings.Compare(queryType, "geoshape-within") == 0 {
		relation = "within"
	}
	atomic.AddInt64(activeConns, 1)
	ctx := context.Background()
	for line := range queue {
		polygon := lineToPolygon(line)
		previousOpsVal := atomic.LoadUint64(sentCommands)
		if previousOpsVal >= totalDatapoints {
			break
		}
		startT := time.Now()
		err, resSize := ec.QueryPolygon(ctx, relation, fieldName, polygon, finishedCommands, debug)
		endT := time.Now()
		duration := endT.Sub(startT)
		datapointsChan <- datapoint{!(err != nil), duration.Microseconds(), resSize}
	}
	ec.CleanupThread(ctx)
	atomic.AddInt64(activeConns, -1)
	// Let the main process know we're done.
	complete <- true
}

func loadWorkerGeoshape(uri, password string, queue chan string, complete chan bool, ops *uint64, datapointsChan chan datapoint, totalDatapoints uint64, db string, fieldName string, debugLevel int, connWriteTimeout time.Duration) {
	c, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress:      []string{uri},
		DisableCache:     true,
		AlwaysRESP2:      true,
		Password:         password,
		ConnWriteTimeout: connWriteTimeout,
	})
	if err != nil {
		panic(err)
	}
	defer c.Close()
	var r = redis.RedisWrapper{
		c,
		db,
	}
	ctx := context.Background()
	for line := range queue {
		polygon := lineToPolygon(line)
		previousOpsVal := atomic.LoadUint64(ops)
		if previousOpsVal >= totalDatapoints {
			break
		}
		opsVal := atomic.AddUint64(ops, 1)
		documentId := fmt.Sprintf("%d", opsVal)
		startT := time.Now()
		err = r.InsertPolygon(ctx, documentId, fieldName, polygon, debugLevel)
		endT := time.Now()
		duration := endT.Sub(startT)

		datapointsChan <- datapoint{!(err != nil), duration.Microseconds(), 0}
	}
	// Let the main process know we're done.
	complete <- true
}
