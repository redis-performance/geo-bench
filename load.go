package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/rueian/rueidis"
	progressbar "github.com/schollz/progressbar/v3"
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

func main() {
	c, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress: []string{"127.0.0.1:6379"},
	})
	if err != nil {
		panic(err)
	}
	defer c.Close()
	ctx := context.Background()

	file, err := os.Open("documents.json")
	nLines, err := LineCounter(file)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(nLines)
	file.Close()
	file, err = os.Open("documents.json")
	if err != nil {
		log.Fatal(err)
	}
	scanner := bufio.NewScanner(file)

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

}
