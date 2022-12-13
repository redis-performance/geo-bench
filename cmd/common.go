package cmd

import "encoding/json"

const REDIS_TYPE_GEO = "redis-geo"
const REDIS_TYPE_GENERIC = "redis"
const REDIS_TYPE_JSON = "redisearch-json"
const REDIS_TYPE_HASH = "redisearch-hash"
const REDIS_DEFAULT_IDX_NAME = "idx"
const REDIS_GEO_DEFAULT_KEYNAME = "key"
const REDIS_IDX_NAME_PROPERTY = "redisearch.index.name"
const REDIS_IDX_PROPERTY = "redisearch.index"
const REDIS_GEO_KEYNAME_PROPERTY = "redis.geo.keyname"

type datapoint struct {
	success        bool
	duration_ms    int64
	resultset_size int64
}

type GeoPoint struct {
	LatLon []float64 `json:"location"`
}

func lineToLonLat(line string) (float64, float64) {
	var geo GeoPoint
	json.Unmarshal([]byte(line), &geo)
	lon := geo.LatLon[0]
	lat := geo.LatLon[1]
	return lon, lat
}
