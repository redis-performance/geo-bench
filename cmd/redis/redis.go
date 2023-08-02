package redis

import (
	"context"
	"fmt"
	"github.com/redis/rueidis"
	"github.com/spf13/pflag"
	"log"
)

const REDIS_TYPE_GEO = "redis-geo"
const REDIS_TYPE_GENERIC = "redis"
const REDIS_TYPE_JSON = "redisearch-json"
const REDIS_TYPE_HASH = "redisearch-hash"
const INPUT_TYPE_GEOPOINT = "geopoint"
const INPUT_TYPE_GEOSHAPE = "geoshape"
const DEFAULT_INPUT_TYPE = INPUT_TYPE_GEOPOINT
const REDIS_DEFAULT_IDX_NAME = "idx"
const REDIS_GEO_DEFAULT_KEYNAME = "key"
const REDIS_IDX_PROPERTY = "redisearch.index"
const REDIS_GEO_KEYNAME_PROPERTY = "redis.geo.keyname"

const INDEX_FIELDNAME_GEOSHAPE = "shape"
const INDEX_FIELDNAME_GEOPOINT = "location"
const QUERY_TYPE_GEODIST_RADIUS = "geodist-radius"
const QUERY_TYPE_GEODIST_BBOX = "geodist-bbox"
const QUERY_TYPE_GEOSHAPE_WITHIN = "geoshape-within"
const QUERY_TYPE_GEOSHAPE_CONTAINS = "geoshape-contains"
const DEFAULT_QUERY_TYPE = QUERY_TYPE_GEOSHAPE_CONTAINS
const DEFAULT_COMMAND_TIMEOUT = 300000
const REDIS_IDX_NAME_PROPERTY = "redisearch.index.name"
const REDIS_URI_PROPERTY = "redis.uri"
const REDIS_PASSWORD_PROPERTY = "redis.password"
const REDIS_COMMAND_TIMEOUT = "redis.command.timeout"
const REDIS_PASSWORD_PROPERTY_DEFAULT = ""
const REDIS_CLUSTER_PROPERTY = "redis.cluster"
const REDIS_URI_PROPERTY_DEFAULT = "localhost:6379"

func RegisterRedisLoadFlags(flags *pflag.FlagSet) {
	flags.StringP(REDIS_URI_PROPERTY, "u", REDIS_URI_PROPERTY_DEFAULT, "Server URI")
	flags.StringP(REDIS_PASSWORD_PROPERTY, "", REDIS_PASSWORD_PROPERTY_DEFAULT, "Server Password")
	flags.BoolP(REDIS_CLUSTER_PROPERTY, "", false, "Enable cluster mode")
	flags.BoolP(REDIS_IDX_PROPERTY, "", true, "Enable redisearch secondary index on HASH and JSON datatypes")
	flags.StringP(REDIS_IDX_NAME_PROPERTY, "", REDIS_DEFAULT_IDX_NAME, "redisearch secondary index name")
	flags.StringP(REDIS_GEO_KEYNAME_PROPERTY, "", REDIS_GEO_DEFAULT_KEYNAME, "redis GEO keyname")
	flags.Int64P(REDIS_COMMAND_TIMEOUT, "", DEFAULT_COMMAND_TIMEOUT, "Command timeout in millis.")
	flags.IntP("debug", "", 0, "debug level. O no debug.")

}

func PrepareRedisQueryCommandFlags(pflags *pflag.FlagSet) {
	pflags.StringP("input", "i", "documents.json", "Input json file")
	pflags.StringP("input-type", "", DEFAULT_INPUT_TYPE, "Input type. One of 'geopoint' or 'geoshape'")
	pflags.StringP("query-type", "", DEFAULT_QUERY_TYPE, "Query type. Only used for 'geoshape' inputs. One of 'geoshape-contains' or 'geoshape-within'")
	pflags.Int64P(REDIS_COMMAND_TIMEOUT, "", DEFAULT_COMMAND_TIMEOUT, "Command timeout in millis.")
	pflags.IntP("concurrency", "c", 50, "Concurrency")
	pflags.IntP("debug", "", 0, "debug level. O no debug.")
	pflags.IntP("random.seed", "", 12345, "Random seed")
	pflags.IntP("test.time", "", -1, "Number of seconds to run the test. . If -1 then it will use requests property")
	pflags.IntP("requests", "n", -1, "Requests. If -1 then it will use all input datapoints")
	pflags.StringP(REDIS_URI_PROPERTY, "u", REDIS_URI_PROPERTY_DEFAULT, "Server URI")
	pflags.StringP(REDIS_PASSWORD_PROPERTY, "", REDIS_PASSWORD_PROPERTY_DEFAULT, "Server Password")
	pflags.BoolP(REDIS_CLUSTER_PROPERTY, "", false, "Enable cluster mode")
	pflags.StringP(REDIS_IDX_NAME_PROPERTY, "", REDIS_DEFAULT_IDX_NAME, "redisearch secondary index name")
	pflags.StringP(REDIS_GEO_KEYNAME_PROPERTY, "", REDIS_GEO_DEFAULT_KEYNAME, "redis GEO keyname")
}

type RedisWrapper struct {
	Client   rueidis.Client
	Datatype string
}

func (r *RedisWrapper) InsertPolygon(ctx context.Context, documentId string, fieldName string, polygon string, debugLevel int) (err error) {
	var cmd rueidis.Completed
	switch r.Datatype {
	case REDIS_TYPE_JSON:
		cmd = r.Client.B().JsonSet().Key(documentId).Path("$").Value(fmt.Sprintf("{\"%s\":\"%s\"}", fieldName, polygon)).Build()
	case REDIS_TYPE_HASH:
		fallthrough
	default:
		cmd = r.Client.B().Hset().Key(documentId).FieldValue().FieldValue(fieldName, polygon).Build()
	}
	err = r.Client.Do(ctx, cmd).Error()
	if debugLevel > 0 && err != nil {
		log.Printf("Error reply: %v. while setting key %s, fieldname %s, polygon: %s.", err.Error(), documentId, fieldName, polygon)
	}
	return
}

func (r *RedisWrapper) InsertGeoPoint(ctx context.Context, documentId string, element string, lon, lat float64) (err error) {
	switch r.Datatype {
	case REDIS_TYPE_JSON:
		err = r.Client.Do(ctx, r.Client.B().JsonSet().Key(documentId).Path("$").Value(fmt.Sprintf("{\"%s\":\"%f,%f\"}", element, lon, lat)).Build()).Error()
	case REDIS_TYPE_HASH:
		err = r.Client.Do(ctx, r.Client.B().Hset().Key(documentId).FieldValue().FieldValue(element, fmt.Sprintf("%f,%f", lon, lat)).Build()).Error()
	case REDIS_TYPE_GENERIC:
		fallthrough
	case REDIS_TYPE_GEO:
		fallthrough
	default:
		err = r.Client.Do(ctx, r.Client.B().Geoadd().Key(documentId).LongitudeLatitudeMember().LongitudeLatitudeMember(lon, lat, element).Build()).Error()
	}
	return
}
