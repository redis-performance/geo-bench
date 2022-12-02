package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/rueian/rueidis"
	"github.com/schollz/progressbar/v3"
	"io"
	"log"
	"os"
)

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

var concurrency = 100

func main() {
	file, err := os.Open("documents.json")
	nLines, err := LineCounter(file)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(nLines)
	file.Close()

	workQueue := make(chan string)

	// We need to know when everyone is done so we can exit.
	complete := make(chan bool)

	// Read the lines into the work queue.
	go func() {
		file, err = os.Open("documents.json")
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

	bar := progressbar.Default(int64(nLines))

	// Now read them all off, concurrently.
	for i := 0; i < concurrency; i++ {
		go startWorking(i+1, bar, workQueue, complete)
	}

	// Wait for everyone to finish.
	for i := 0; i < concurrency; i++ {
		<-complete
	}
}

func startWorking(workerNumber int, b *progressbar.ProgressBar, queue chan string, complete chan bool) {
	c, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress: []string{"127.0.0.1:6379"},
	})
	if err != nil {
		panic(err)
	}
	defer c.Close()
	ctx := context.Background()
	cpos := 0
	for line := range queue {
		var geo GeoPoint
		json.Unmarshal([]byte(line), &geo)
		lon := geo.LatLon[0]
		lat := geo.LatLon[1]
		memberS := fmt.Sprintf("w%d-%d", workerNumber, cpos)
		c.Do(ctx, c.B().Geoadd().Key("key").LongitudeLatitudeMember().LongitudeLatitudeMember(lon, lat, memberS).Build()).Error()
		cpos = cpos + 1
		b.Add(1)

	}
	// Let the main process know we're done.
	complete <- true
}

/*


	bar := progressbar.Default(int64(nLines))

	cpos := 1
	for scanner.Scan() {
		bar.Add(1)
		var geo GeoPoint
		json.Unmarshal([]byte(scanner.Text()), &geo)
		lon := geo.LatLon[0]
		lat := geo.LatLon[1]
		memberS := fmt.Sprintf("%d", cpos)
		c.Do(ctx, c.B().Geoadd().Key("key").LongitudeLatitudeMember().LongitudeLatitudeMember(lon, lat, memberS).Build()).Error()
		cpos = cpos + 1
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

*/
