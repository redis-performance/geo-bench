/*
Copyright © 2023 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/encoding/wkt"
	"github.com/paulmach/orb/simplify"
	"github.com/schollz/progressbar/v3"
	"github.com/spf13/cobra"
	"log"
	"os"
	"strings"
)

// simplifyCmd represents the simplify command
var simplifyCmd = &cobra.Command{
	Use:   "simplify",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		pflags := cmd.Flags()
		input, _ := pflags.GetString("input")
		output, _ := pflags.GetString("output")
		inputType, _ := pflags.GetString("input-type")
		histogramOriginalFilename, _ := pflags.GetString("input-histogram-csv")
		histogramFinalFilename, _ := pflags.GetString("output-histogram-csv")
		padBound, _ := pflags.GetFloat64("pad-bound")
		outerRingOnly, _ := pflags.GetBool("outer-ring-only")
		useBoundsOnly, _ := pflags.GetBool("use-bounds-only")
		douglasPeuckerThreshold, _ := pflags.GetFloat64("douglas-peucker-threshold")
		if strings.Compare(inputType, INPUT_TYPE_GEOSHAPE) != 0 {
			log.Printf("Only %s type can be simplified. Exiting...", INPUT_TYPE_GEOSHAPE)
		}
		if !useBoundsOnly && padBound > 0.0 {
			log.Printf("You've speficied a pad bound of %f but this is only applied when --%s", padBound, "use-bounds-only")
		}

		requests, _ := pflags.GetInt("requests")
		file, err := os.Open(input)
		nLines, err := LineCounter(file)
		if err != nil {
			log.Fatal(err)
		}
		file.Close()
		nDatapoints := requests
		if nLines < nDatapoints || nDatapoints < 0 {
			nDatapoints = nLines
		}
		if outerRingOnly {
			log.Printf("Will preserve only the outer rings of the polygons given --%s was specified", "outer-ring-only")
		}
		log.Printf("There are a total of %d datapoints in %s", nLines, input)

		file, err = os.Open(input)
		if err != nil {
			log.Fatal(err)
		}
		// Close when the functin returns
		defer file.Close()

		ofile, err := os.OpenFile(output, os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatal(err)
		}
		// Close when the functin returns
		defer ofile.Close()

		scanner := bufio.NewScanner(file)
		buf := make([]byte, 512*1024*1024)
		scanner.Buffer(buf, 512*1024*1024)
		var histogramOriginal = make(map[int64]int64)
		var histogramFinal = make(map[int64]int64)
		var originalPointCount int64 = 0
		var finalPointCount int64 = 0

		bar := progressbar.Default(int64(nDatapoints))
		n := 0
		for scanner.Scan() {
			finalInputLine := scanner.Text()
			polygon := lineToPolygon(finalInputLine)
			original, err := wkt.UnmarshalPolygon(polygon)
			totalPointsOriginal := getNPoints(original)
			if err != nil {
				fmt.Printf("can't unmarshal: %v. Input was %s\n", err, polygon)
				continue
			}
			var final orb.Polygon
			final = original
			if outerRingOnly {
				final = getOuterRing(original)
			}
			if douglasPeuckerThreshold > 0.0 {
				final = simplify.DouglasPeucker(douglasPeuckerThreshold).Simplify(final.Clone()).(orb.Polygon)
			}
			totalPointsFinal := getNPoints(final)
			if useBoundsOnly {
				final = final.Bound().Pad(padBound).ToPolygon()
				totalPointsFinal = getNPoints(final)
			}
			finalOutputPolygon := wkt.Marshal(final)

			finalShape := GeoShape{
				Shape:   string(finalOutputPolygon),
				NPoints: totalPointsFinal,
			}
			finalBytes, err := json.Marshal(finalShape)
			if err != nil {
				fmt.Printf("can't marshal: %v. Input was %v\n", err, finalShape)
				continue
			}
			ofile.Write(append(finalBytes, '\n'))
			originalPointCount += totalPointsOriginal
			finalPointCount += totalPointsFinal
			histogramOriginal[totalPointsOriginal]++
			histogramFinal[totalPointsFinal]++
			bar.Add(1)
			n = n + 1
			if n >= nDatapoints {
				break
			}
		}
		log.Printf("Avg input polygon size %.0f", float64(originalPointCount)/(float64(n)))
		log.Printf("Avg output polygon size %.0f", float64(finalPointCount)/(float64(n)))

		if histogramOriginalFilename != "" {
			saveHistogram(histogramOriginalFilename, histogramOriginal)
		}
		if histogramFinalFilename != "" {
			saveHistogram(histogramFinalFilename, histogramFinal)
		}
	},
}

func getOuterRing(original orb.Polygon) (outer orb.Polygon) {
	// Polygon is a closed area. The first LineString is the outer ring.
	// The others are the holes. Each LineString is expected to be closed
	// ie. the first point matches the last.
	outer = make([]orb.Ring, 1, 1)
	outer[0] = original[0]
	return
}
func getNPoints(original orb.Polygon) (totalPointsOriginal int64) {
	// Polygon is a closed area. The first LineString is the outer ring.
	// The others are the holes. Each LineString is expected to be closed
	// ie. the first point matches the last.
	totalPointsOriginal = 0
	for _, ring := range original {
		for _, linestring := range ring {
			totalPointsOriginal += int64(len(linestring))
		}
	}
	return
}

func init() {
	rootCmd.AddCommand(simplifyCmd)
	flags := simplifyCmd.Flags()
	flags.StringP("input", "i", "polygons.json", "Input json file")
	flags.StringP("output", "o", "polygons.simplified.json", "Output json file")
	flags.StringP("input-type", "", INPUT_TYPE_GEOSHAPE, "Input type. One of 'geopoint' or 'geoshape'")
	flags.IntP("requests", "n", -1, "Requests. If -1 then it will use all input datapoints")
	flags.Float64P("pad-bound", "", 0.0, "extends the bound in all directions by the given value. Only used when --use-bounds-only=true")
	flags.BoolP("outer-ring-only", "", false, "Polygon is a closed area. The first LineString is the outer ring. The others are the holes. When enabled this preserves only the outer ring on the output file.")
	flags.BoolP("use-bounds-only", "", false, "When enabled this preserves only bound around the input polygon on the output file.")
	flags.Float64P("douglas-peucker-threshold", "", 0.0, " distance (in units of input coordinates) of a vertex to other segments to be removed.")
	flags.StringP("input-histogram-csv", "", "input-npoints-histogram.csv", "Store input polygon's number of points histogram into a csv file. If empty wont store it.")
	flags.StringP("output-histogram-csv", "", "output-npoints-histogram.csv", "Store output polygon's number of points histogram into a csv file. If empty wont store it.")
}
