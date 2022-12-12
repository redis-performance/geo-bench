/*
Copyright © 2022 Redis Ltd <performance@redis.com>
*/
package cmd

import (
	"os"

	"github.com/spf13/cobra"
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "geo-bench",
	Short: "geopoint data benchmark.",
	Long: `Geopoints are a data type used to represent geographic coordinates in various databases, including Redis, ElasticSearch, and MongoDB.
This repository contains a set of scripts and tools for running benchmarks on geopoint data in these databases. 
The benchmarks in this repository cover a range of common operations, such as indexing, searching, and querying data based on geographic coordinates. 
The results of the benchmarks can be used to compare the performance of Redis, ElasticSearch, and MongoDB when working with geopoint data, and to identify potential bottlenecks or areas for optimization. 

By providing benchmarks for these popular databases, this repository can be a useful resource for developers and data professionals working with geopoint data.`,
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
}
