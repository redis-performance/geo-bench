module filipecosta90/geo-bench

go 1.19

require (
	github.com/HdrHistogram/hdrhistogram-go v1.1.2
	github.com/cenkalti/backoff/v4 v4.1.3
	github.com/elastic/go-elasticsearch/v8 v8.7.1
	github.com/redis/rueidis v0.0.89
	github.com/spf13/cobra v1.6.1
	github.com/spf13/pflag v1.0.5
)

require (
	github.com/elastic/elastic-transport-go/v8 v8.2.0 // indirect
	github.com/inconshreveable/mousetrap v1.0.1 // indirect
	github.com/stretchr/testify v1.8.1 // indirect
)

replace github.com/redis/rueidis => github.com/filipecosta90/rueidis v0.0.0-20230514183601-25fb0c71c8a5
