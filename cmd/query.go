/*
Copyright © 2022 Redis Ltd <performance@redis.com>
*/
package cmd

import (
	"bufio"
	"context"
	"encoding/csv"
	elastic "filipecosta90/geo-bench/cmd/elasticsearch"
	"filipecosta90/geo-bench/cmd/redis"
	"fmt"
	hdrhistogram "github.com/HdrHistogram/hdrhistogram-go"
	"github.com/redis/rueidis"
	"github.com/spf13/cobra"
	"log"
	"math"
	"math/rand"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// queryCmd represents the query command
var queryCmd = &cobra.Command{
	Use:   "query",
	Short: "Benchmark query operations of geographic coordinates.",
	Long:  `This command covers querying operations of geographic coordinates.`,
	Run: func(cmd *cobra.Command, args []string) {
		pflags := cmd.Flags()
		db, _ := pflags.GetString("db")
		input, _ := pflags.GetString("input")
		uri, _ := pflags.GetString(redis.REDIS_URI_PROPERTY)
		concurrency, _ := pflags.GetInt("concurrency")
		testTime, _ := pflags.GetInt("test.time")
		requests, _ := pflags.GetInt("requests")
		seed, _ := pflags.GetInt("random.seed")
		debugLevel, _ := pflags.GetInt("debug")
		queryTimeout, _ := pflags.GetInt64(redis.REDIS_COMMAND_TIMEOUT)
		redisGeoKeyname, _ := pflags.GetString(REDIS_GEO_KEYNAME_PROPERTY)
		indexSearchName, _ := pflags.GetString(redis.REDIS_IDX_NAME_PROPERTY)
		password, _ := pflags.GetString(redis.REDIS_PASSWORD_PROPERTY)
		inputType, _ := pflags.GetString("input-type")
		queryType, _ := pflags.GetString("query-type")
		replyHistogram, _ := pflags.GetString("reply-histogram-csv")

		validateDB(db)
		log.Printf("Using %d concurrent workers", concurrency)
		if testTime > 0 {
			log.Printf("Will run test for %d seconds", testTime)
		} else {
			log.Printf("Running test with %d queries", requests)

		}
		log.Printf("Using %d random seed", seed)
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
				workQueue <- scanner.Text()
				n = n + 1
				if n >= nDatapoints {
					break
				}
			}

			// Close the channel so everyone reading from it knows we're done.
			close(workQueue)
		}()

		var r = rand.New(rand.NewSource(int64(seed)))
		var mu sync.Mutex

		var finishedCommands uint64 = 0
		var issuedCommands uint64 = 0
		var activeConns int64 = 0
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
					go queryWorkerGeoshapeElastic(elasticWrapper, workQueue, complete, &issuedCommands, &finishedCommands, &activeConns, datapointsChan, uint64(nDatapoints), queryType, INDEX_FIELDNAME_GEOSHAPE, debugLevel)
				} else {
					go queryWorkerGeoShape(uri, password, workQueue, complete, &issuedCommands, datapointsChan, uint64(nDatapoints), db, indexSearchName, INDEX_FIELDNAME_GEOSHAPE, queryType, testTime, queryTimeout, debugLevel)
				}
				// geopoint
			} else {
				go queryWorkerGeoPoint(uri, password, workQueue, complete, &issuedCommands, datapointsChan, uint64(nDatapoints), db, mu, r, redisGeoKeyname, indexSearchName, INDEX_FIELDNAME_GEOPOINT, testTime)
			}
			// delay the creation 1ms for each additional client
			time.Sleep(time.Millisecond * 1)
		}

		_, _, duration, totalMessages, _, histogramReplySize, avgReplySize := updateCLI(tick, controlC, uint64(nDatapoints), &activeConns, false, datapointsChan, start, testTime)
		messageRate := float64(totalMessages) / float64(duration.Seconds())
		avgMs := float64(latencies.Mean()) / 1000.0
		p50IngestionMs := float64(latencies.ValueAtQuantile(50.0)) / 1000.0
		p95IngestionMs := float64(latencies.ValueAtQuantile(95.0)) / 1000.0
		p99IngestionMs := float64(latencies.ValueAtQuantile(99.0)) / 1000.0
		log.Printf("Finished query stage for %s DB\n", db)
		log.Printf("Average reply size in #elements %f\n", math.Round(avgReplySize))

		fmt.Printf("\n")
		fmt.Printf("#################################################\n")
		fmt.Printf("Total Duration %.3f Seconds\n", duration.Seconds())
		fmt.Printf("Total sent queries %d\n", totalCommands)
		fmt.Printf("Total Errors %d\n", totalErrors)
		fmt.Printf("Throughput summary: %.0f requests per second\n", messageRate)
		fmt.Printf("Latency summary (msec):\n")
		fmt.Printf("    %9s %9s %9s %9s\n", "avg", "p50", "p95", "p99")
		fmt.Printf("    %9.3f %9.3f %9.3f %9.3f\n", avgMs, p50IngestionMs, p95IngestionMs, p99IngestionMs)
		fmt.Println(fmt.Sprintf("Finished sending %d queries of type %s", finishedCommands, queryType))

		if replyHistogram != "" {
			saveHistogram(replyHistogram, histogramReplySize)
		}
	},
}

func saveHistogram(replyHistogram string, histogramReplySize map[int64]int64) {
	// sort reply sizes to be able to produce an ordered csv
	var replySizes []int
	for k := range histogramReplySize {
		replySizes = append(replySizes, int(k))
	}
	sort.Sort(sort.Reverse(sort.IntSlice(replySizes)))

	fmt.Println(fmt.Sprintf("Storing csv file with reply size histogram in %s", replyHistogram))
	csvFile, err := os.Create(replyHistogram)

	if err != nil {
		log.Fatalf("Failed creating csv file: %s", err)
	}
	csvwriter := csv.NewWriter(csvFile)
	_ = csvwriter.Write([]string{"reply_size", "count"})
	for value := range replySizes {
		ocurrences := histogramReplySize[int64(value)]
		if ocurrences > 0 {
			_ = csvwriter.Write([]string{fmt.Sprintf("%d ", value), fmt.Sprintf(" %d", ocurrences)})
		}
	}
	csvwriter.Flush()
	csvFile.Close()
}

func init() {
	rootCmd.AddCommand(queryCmd)
	pflags := queryCmd.Flags()
	pflags.StringP("db", "", "redis", "Database to load the data to")
	pflags.StringP("reply-histogram-csv", "", "", "Store reply size data into a csv file. If empty wont store it.")
	redis.PrepareRedisQueryCommandFlags(pflags)
	elastic.RegisterElasticRunFlags(pflags)
}

func queryWorkerGeoShape(uri, password string, queue chan string, complete chan bool, ops *uint64, datapointsChan chan datapoint, totalDatapoints uint64, db string, indexSearchName, fieldName, queryType string, testDuration int, queryTimeoutMillis int64, debugLevel int) {
	c, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress:  []string{uri},
		DisableCache: true,
		AlwaysRESP2:  true,
		Password:     password,
	})
	if err != nil {
		panic(err)
	}
	defer c.Close()
	testStartThread := time.Now()
	ctx := context.Background()
	for line := range queue {
		polygon := lineToPolygon(line)
		previousOpsVal := atomic.LoadUint64(ops)
		if previousOpsVal >= totalDatapoints {
			break
		}
		threadDuration := time.Now().Sub(testStartThread).Seconds()
		if testDuration > 0 && threadDuration > float64(testDuration) {
			break
		}

		querySearch := ""
		switch queryType {
		case QUERY_TYPE_GEOSHAPE_CONTAINS:
			querySearch = fmt.Sprintf("@%s:[contains $poly]", fieldName)
			break
		case QUERY_TYPE_GEOSHAPE_WITHIN:
			querySearch = fmt.Sprintf("@%s:[within $poly]", fieldName)
			break
		default:
			log.Fatal(fmt.Sprintf("Invalid query-type. Exiting..."))
		}
		var resultSetSize int64 = 0
		startT := time.Now()
		err, resultSetSize = queryPolygon(db, c, ctx, indexSearchName, querySearch, queryTimeoutMillis, polygon, debugLevel)
		endT := time.Now()
		duration := endT.Sub(startT)

		if debugLevel > 0 && err != nil {
			log.Printf("Error reply: %v. with the following query %s and polygon: %s", err.Error(), querySearch, polygon)
		}
		atomic.AddUint64(ops, 1)
		datapointsChan <- datapoint{!(err != nil), duration.Microseconds(), resultSetSize}

	}
	// Let the main process know we're done.
	complete <- true
}

func queryPolygon(db string, c rueidis.Client, ctx context.Context, indexSearchName string, querySearch string, queryTimeoutMillis int64, polygon string, debuglevel int) (err error, resultSetSize int64) {
	resultSetSize = 0
	var redisReply []rueidis.RedisMessage
	switch db {
	case REDIS_TYPE_JSON:
		redisReply, err = c.Do(ctx, c.B().FtSearch().Index(indexSearchName).Query(querySearch).Timeout(queryTimeoutMillis).Params().Nargs(2).NameValue().NameValue("poly", polygon).Dialect(3).Build()).ToArray()
		if len(redisReply) > 0 {
			resultSetSize, err = redisReply[0].ToInt64()
		}
		if debuglevel > 1 {
			verbosePrintRediSearchReply(querySearch, polygon, resultSetSize, redisReply)
		}
	case REDIS_TYPE_HASH:
		query := c.B().FtSearch().Index(indexSearchName).Query(querySearch).Timeout(queryTimeoutMillis).Params().Nargs(2).NameValue().NameValue("poly", polygon).Dialect(3).Build()
		redisReply, err = c.Do(ctx, query).ToArray()
		if len(redisReply) > 0 {
			resultSetSize, err = redisReply[0].ToInt64()
		}
		if debuglevel > 1 {
			verbosePrintRediSearchReply(querySearch, polygon, resultSetSize, redisReply)
		}
	default:
		log.Fatal(fmt.Sprintf("DB was not recognized. Exiting..."))
	}
	return err, resultSetSize
}

func verbosePrintRediSearchReply(querySearch string, polygon string, resultSetSize int64, redisReply []rueidis.RedisMessage) {
	fmt.Printf("QUERY: %s PARAMS %s %s. Reply (size %d):\n", querySearch, "poly", polygon, resultSetSize)
	docsReply := redisReply[1:]
	ndocs := len(docsReply) / 2
	for i := 0; i < ndocs; i++ {
		docId, _ := docsReply[i*2].ToString()
		docArr, _ := docsReply[i*2+1].ToArray()
		fmt.Printf("\tDoc: %s\n", docId)
		for _, ele := range docArr {
			eleS, _ := ele.ToString()
			fmt.Printf("\t\t%s\n", eleS)
		}
	}
}

func queryWorkerGeoPoint(uri, password string, queue chan string, complete chan bool, ops *uint64, datapointsChan chan datapoint, totalDatapoints uint64, db string, mu sync.Mutex, r *rand.Rand, redisGeoKeyname string, indexSearchName string, fieldName string, testDuration int) {

	c, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress:  []string{uri},
		DisableCache: true,
		AlwaysRESP2:  true,
		Password:     password,
	})
	if err != nil {
		panic(err)
	}
	defer c.Close()
	testStartThread := time.Now()
	ctx := context.Background()
	for line := range queue {
		lon, lat := lineToLonLat(line)
		previousOpsVal := atomic.LoadUint64(ops)
		if previousOpsVal >= totalDatapoints {
			break
		}
		threadDuration := time.Now().Sub(testStartThread).Seconds()
		if testDuration > 0 && threadDuration > float64(testDuration) {
			break
		}

		atomic.AddUint64(ops, 1)

		// lock/unlock when accessing the rand from a goroutine
		mu.Lock()
		radius := math.Round(r.Float64()*3000.0 + 100.0)
		mu.Unlock()
		var res []string
		var resultSetSize int64 = 0
		querySearch := fmt.Sprintf("@location:[%f %f %f m]", lon, lat, radius)
		startT := time.Now()
		switch db {
		case REDIS_TYPE_JSON:
			innerRes, err1 := c.Do(ctx, c.B().FtSearch().Index(indexSearchName).Query(querySearch).Return("1").Identifier(fieldName).Limit().OffsetNum(0, 100000).Build()).ToArray()
			err = err1
			if len(innerRes) > 0 {
				resultSetSize, err = innerRes[0].ToInt64()
			}
		case REDIS_TYPE_HASH:
			innerRes, err1 := c.Do(ctx, c.B().FtSearch().Index(indexSearchName).Query(querySearch).Return("1").Identifier(fieldName).Limit().OffsetNum(0, 100000).Build()).ToArray()
			err = err1
			if len(innerRes) > 0 {
				resultSetSize, err = innerRes[0].ToInt64()
			}
		case REDIS_TYPE_GENERIC:
			fallthrough
		case REDIS_TYPE_GEO:
			fallthrough
		default:
			res, err = c.Do(ctx, c.B().Geosearch().Key(redisGeoKeyname).Fromlonlat(lon, lat).Byradius(radius).M().Withcoord().Build()).AsStrSlice()
			resultSetSize = int64(len(res))
		}
		endT := time.Now()

		duration := endT.Sub(startT)
		datapointsChan <- datapoint{!(err != nil), duration.Microseconds(), resultSetSize}

	}
	// Let the main process know we're done.
	complete <- true
}
