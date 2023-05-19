package elastic

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"github.com/cenkalti/backoff/v4"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/elastic/go-elasticsearch/v8/esutil"
	_ "github.com/elastic/go-elasticsearch/v8/typedapi/types"

	"github.com/spf13/pflag"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"time"
)

const (
	elasticUrl                                 = "es.hosts.list"
	elasticUrlDefault                          = "https://127.0.0.1:9200"
	elasticInsecureSSLProp                     = "es.insecure.ssl"
	elasticInsecureSSLPropDefault              = true
	elasticExitOnIndexCreateFailureProp        = "es.exit.on.index.create.fail"
	elasticExitOnIndexCreateFailurePropDefault = true
	elasticShardCountProp                      = "es.number_of_shards"
	elasticShardCountPropDefault               = 1
	elasticReplicaCountProp                    = "es.number_of_replicas"
	elasticReplicaCountPropDefault             = 0
	elasticUsername                            = "es.username"
	elasticUsernameDefault                     = "elastic"
	elasticPassword                            = "es.password"
	elasticPasswordPropDefault                 = ""
	elasticFlushInterval                       = "es.flush_interval"
	bulkIndexerNumberOfWorkers                 = "es.bulk.num_workers"
	bulkIndexerBatchSize                       = "es.bulk.batch.size"
	bulkIndexerBatchSizeDefault                = 1
	elasticMaxRetriesProp                      = "es.max_retires"
	elasticMaxRetriesPropDefault               = 10
	bulkIndexerFlushBytesProp                  = "es.bulk.flush_bytes"
	bulkIndexerFlushBytesDefault               = 5e+6
	bulkIndexerFlushIntervalSecondsProp        = "es.bulk.flush_interval_secs"
	bulkIndexerFlushIntervalSecondsPropDefault = 1
	elasticIndexNameDefault                    = "geo"
	elasticIndexName                           = "es.index"
	elasticIndexFieldName                      = "es.index.fieldname"
	elasticIndexDryRun                         = "es.index.dryrun"
	elasticIndexDryRunDefault                  = false
	elasticIndexFieldNameDefault               = "shape"
)

type ElasticWrapper struct {
	cli         *elasticsearch.Client
	typedClient *elasticsearch.TypedClient
	bi          esutil.BulkIndexer
	indexName   string
	verbose     bool
}

func (m *ElasticWrapper) CleanupThread(ctx context.Context) {
	m.bi.Close(ctx)
}

// Insert a document.
func (m *ElasticWrapper) QueryPolygon(ctx context.Context, relation string, fieldname string, polygon string, finishedCommands *uint64, debug int) (err error, resultSetSize int64) {
	resultSetSize = 0
	if err != nil {
		if m.verbose {
			fmt.Printf("Cannot encode document %s: %s\n", fieldname, err.Error())
		}
		return
	}
	query := []byte(`{
  "query": {
    "bool": {
      "must": {
        "match_all": {}
      },
      "filter": {
        "geo_shape": {
          "shape": {
            "shape": "` + polygon + `",
            "relation": "` + relation + `"
          }
        }
      }
    }
  }
}`)
	// convert byte slice to io.Reader
	reader := bytes.NewReader(query)
	res, err := m.typedClient.Search().Raw(reader).Do(ctx)
	if err != nil {
		fmt.Printf("Unexpected error while querying: %s\n", err.Error())
		return
	}
	resultSetSize = res.Hits.Total.Value

	if debug > 0 {
		hits := res.Hits.Hits
		fmt.Printf("Send query: %s.\n\tReceived Hits %d\n", query, resultSetSize)
		for _, hit := range hits {
			fmt.Printf("\t\thit: %s\n", hit.Source_)
		}
	}

	atomic.AddUint64(finishedCommands, 1)
	return
}

// Insert a document.
func (m *ElasticWrapper) InsertPolygon(ctx context.Context, documentId string, fieldname string, polygon string, ops *uint64) error {
	data, err := json.Marshal(map[string]string{fieldname: polygon})
	if err != nil {
		if m.verbose {
			fmt.Printf("Cannot encode document %s: %s\n", fieldname, err.Error())
		}
		return err
	}
	// Add an item to the BulkIndexer
	err = m.bi.Add(
		ctx,
		esutil.BulkIndexerItem{
			// Action field configures the operation to perform (index, create, delete, update)
			Action: "index",

			// DocumentID is the (optional) document ID
			DocumentID: documentId,

			// Body is an `io.Reader` with the payload
			Body: bytes.NewReader(data),

			// OnSuccess is called for each successful operation
			OnSuccess: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem) {
				atomic.AddUint64(ops, 1)
			},
			// OnFailure is called for each failed operation
			OnFailure: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
				if err != nil {
					fmt.Printf("ERROR BULK INSERT: %s\n", err.Error())
				} else {
					fmt.Printf("ERROR BULK INSERT: %s: %s\n", res.Error.Type, res.Error.Reason)
				}
			},
		},
	)
	if err != nil {
		if m.verbose {
			fmt.Printf("Unexpected error while bulk inserting: %s\n", err.Error())
		}
		return err
	}
	return nil
}

type ElasticCreator struct {
}

func RegisterElasticRunFlags(flags *pflag.FlagSet) {
	flags.IntP(elasticMaxRetriesProp, "", elasticMaxRetriesPropDefault, "'")
	flags.StringP(elasticUrl, "", elasticUrlDefault, "")
	flags.StringP(elasticUsername, "", elasticUsernameDefault, "")
	flags.StringP(elasticPassword, "", elasticPasswordPropDefault, "")
	flags.StringP(elasticIndexName, "", elasticIndexNameDefault, "")
	flags.StringP(elasticIndexFieldName, "", elasticIndexFieldNameDefault, "")
	flags.BoolP(elasticInsecureSSLProp, "", elasticInsecureSSLPropDefault, "")
	flags.BoolP("verbose", "", false, "")
}

func RegisterElasticLoadFlags(flags *pflag.FlagSet) {
	flags.IntP(bulkIndexerBatchSize, "", bulkIndexerBatchSizeDefault, "Batch size for ElasticWrapper ingestion. > 1 uses Bulk API. For optimal indexing speed please increase this value property.")
	flags.IntP(bulkIndexerNumberOfWorkers, "", 1, "")
	flags.IntP(elasticMaxRetriesProp, "", elasticMaxRetriesPropDefault, "'")
	flags.IntP(bulkIndexerFlushBytesProp, "", bulkIndexerFlushBytesDefault, "")
	flags.IntP(bulkIndexerFlushIntervalSecondsProp, "", bulkIndexerFlushIntervalSecondsPropDefault, "")
	flags.IntP(elasticReplicaCountProp, "", elasticReplicaCountPropDefault, "")
	flags.IntP(elasticShardCountProp, "", elasticShardCountPropDefault, "")
	flags.StringP(elasticUrl, "", elasticUrlDefault, "")
	flags.StringP(elasticUsername, "", elasticUsernameDefault, "")
	flags.StringP(elasticPassword, "", elasticPasswordPropDefault, "")
	flags.StringP(elasticIndexName, "", elasticIndexNameDefault, "")
	flags.StringP(elasticIndexFieldName, "", elasticIndexFieldNameDefault, "")
	flags.BoolP(elasticInsecureSSLProp, "", elasticInsecureSSLPropDefault, "")
	flags.BoolP("verbose", "", false, "")
	flags.BoolP(elasticIndexDryRun, "", elasticIndexDryRunDefault, "Skip indexing the expensive geo_shape field, meaning only index doc without mappings")
	flags.BoolP(elasticExitOnIndexCreateFailureProp, "", elasticExitOnIndexCreateFailurePropDefault, "")
}

func (c ElasticCreator) Create(p *pflag.FlagSet, command string) (*ElasticWrapper, error) {
	bulkIndexerRefresh := "false"
	verbose, _ := p.GetBool("verbose")
	shouldDryRun, _ := p.GetBool(elasticIndexDryRun)

	batchSize, _ := p.GetInt(bulkIndexerBatchSize)
	if batchSize <= 1 {
		bulkIndexerRefresh = "wait_for"
		if verbose {
			fmt.Printf("Bulk API is disable given the property `%s`=1. For optimal indexing speed please increase this value property\n", bulkIndexerBatchSize)
		}
	}
	bulkIndexerNumCpus, _ := p.GetInt(bulkIndexerNumberOfWorkers)
	elasticMaxRetries, _ := p.GetInt(elasticMaxRetriesProp)
	bulkIndexerFlushBytes, _ := p.GetInt(bulkIndexerFlushBytesProp)
	flushIntervalSeconds, _ := p.GetInt(bulkIndexerFlushIntervalSecondsProp)
	elasticReplicaCount, _ := p.GetInt(elasticReplicaCountProp)
	elasticShardCount, _ := p.GetInt(elasticShardCountProp)

	addressesS, _ := p.GetString(elasticUrl)
	insecureSSL, _ := p.GetBool(elasticInsecureSSLProp)
	failOnCreate, _ := p.GetBool(elasticExitOnIndexCreateFailureProp)
	esUser, _ := p.GetString(elasticUsername)
	esPass, _ := p.GetString(elasticPassword)
	iname, _ := p.GetString(elasticIndexName)
	fieldName, _ := p.GetString(elasticIndexFieldName)
	addresses := strings.Split(addressesS, ",")

	retryBackoff := backoff.NewExponentialBackOff()

	cfg := elasticsearch.Config{
		Addresses: addresses,
		// Retry on 429 TooManyRequests statuses
		RetryOnStatus: []int{502, 503, 504, 429},

		// Configure the backoff function
		RetryBackoff: func(i int) time.Duration {
			if i == 1 {
				retryBackoff.Reset()
			}
			return retryBackoff.NextBackOff()
		},
		MaxRetries: elasticMaxRetries,
		Username:   esUser,
		Password:   esPass,
		// Transport / SSL
		Transport: &http.Transport{
			MaxIdleConnsPerHost:   10,
			ResponseHeaderTimeout: time.Second,
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: insecureSSL,
			},
		},
	}
	es, err := elasticsearch.NewClient(cfg)
	tes, err := elasticsearch.NewTypedClient(cfg)
	if err != nil {
		fmt.Println(fmt.Sprintf("Error creating the ElasticWrapper client: %s", err))
		return nil, err
	}
	if verbose {
		fmt.Println("Connected to Elastic!")
		fmt.Println(es.Info())
	}
	// Create the BulkIndexer
	var flushIntervalTime = flushIntervalSeconds * int(time.Second)

	bi, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Index:         iname,                            // The default index name
		Client:        es,                               // The Elasticsearch client
		NumWorkers:    bulkIndexerNumCpus,               // The number of worker goroutines
		FlushBytes:    bulkIndexerFlushBytes,            // The flush threshold in bytes
		FlushInterval: time.Duration(flushIntervalTime), // The periodic flush interval
		// If true, Elasticsearch refreshes the affected
		// shards to make this operation visible to search
		// if wait_for then wait for a refresh to make this operation visible to search,
		// if false do nothing with refreshes. Valid values: true, false, wait_for. Default: false.
		Refresh: bulkIndexerRefresh,
	})
	if err != nil {
		fmt.Printf("Error creating the ElasticWrapper indexer: %s\n", err.Error())
		return nil, err
	}

	if strings.Compare("load", command) == 0 {
		fmt.Println("Ensuring that if the index exists we recreate it")
		// Re-create the index
		var res *esapi.Response
		res, err = es.Indices.Delete([]string{iname}, es.Indices.Delete.WithIgnoreUnavailable(true))
		if err != nil {
			fmt.Printf("Cannot delete index: %s\n", err.Error())
			return nil, err
		}
		if res.IsError() {
			fmt.Println(fmt.Sprintf("Cannot delete index: %s", res.String()))
			return nil, fmt.Errorf("Cannot delete index: %s", res.String())
		}
		res.Body.Close()
		mapping := map[string]interface{}{
			"settings": map[string]interface{}{"index": map[string]interface{}{"number_of_shards": elasticShardCount, "number_of_replicas": elasticReplicaCount}},
			"mappings": map[string]interface{}{},
		}
		// Define index mapping.
		if !shouldDryRun {
			mapping["mappings"] = map[string]interface{}{"properties": map[string]interface{}{fieldName: map[string]interface{}{"type": "geo_shape"}}}
		} else {
			fmt.Println(fmt.Sprintf("Index mappings has 0 fields. This is purely to undex index performance without geo fields"))
		}

		data, err := json.Marshal(mapping)
		if err != nil {
			if verbose {
				fmt.Println(fmt.Sprintf("Cannot encode index mapping %v: %s", mapping, err))
			}
			return nil, err
		}
		res, err = es.Indices.Create(iname, es.Indices.Create.WithBody(strings.NewReader(string(data))))
		if err != nil && failOnCreate {
			fmt.Println(fmt.Sprintf("Cannot create index: %s", err))
			return nil, err
		}
		if res.IsError() && failOnCreate {
			fmt.Println(fmt.Sprintf("Cannot create index: %s", res))
			return nil, fmt.Errorf("Cannot create index: %s", res)
		}
		res.Body.Close()
	}

	m := &ElasticWrapper{
		cli:         es,
		typedClient: tes,
		bi:          bi,
		indexName:   iname,
		verbose:     verbose,
	}
	return m, nil
}
