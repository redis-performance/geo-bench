module filipecosta90/geo-bench

go 1.19

require (
	github.com/HdrHistogram/hdrhistogram-go v1.1.2
	github.com/cenkalti/backoff/v4 v4.1.3
	github.com/elastic/go-elasticsearch/v8 v8.7.1
	github.com/paulmach/orb v0.9.2
	github.com/redis/rueidis v0.0.89
	github.com/schollz/progressbar/v3 v3.13.1
	github.com/spf13/cobra v1.6.1
	github.com/spf13/pflag v1.0.5
)

require (
	github.com/elastic/elastic-transport-go/v8 v8.2.0 // indirect
	github.com/inconshreveable/mousetrap v1.0.1 // indirect
	github.com/mattn/go-runewidth v0.0.14 // indirect
	github.com/mitchellh/colorstring v0.0.0-20190213212951-d06e56a500db // indirect
	github.com/rivo/uniseg v0.4.4 // indirect
	github.com/stretchr/testify v1.8.1 // indirect
	golang.org/x/sys v0.8.0 // indirect
	golang.org/x/term v0.8.0 // indirect
)

replace github.com/redis/rueidis => github.com/filipecosta90/rueidis v0.0.0-20230731125225-704e76e542e7
