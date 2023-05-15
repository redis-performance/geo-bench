package cmd

import "encoding/json"

const REDIS_TYPE_GEO = "redis-geo"
const REDIS_TYPE_GENERIC = "redis"
const REDIS_TYPE_JSON = "redisearch-json"
const REDIS_TYPE_HASH = "redisearch-hash"
const INPUT_TYPE_GEOPOINT = "geopoint"
const INPUT_TYPE_GEOSHAPE = "geoshape"
const REDIS_DEFAULT_IDX_NAME = "idx"
const REDIS_GEO_DEFAULT_KEYNAME = "key"
const REDIS_IDX_NAME_PROPERTY = "redisearch.index.name"
const REDIS_IDX_PROPERTY = "redisearch.index"
const REDIS_GEO_KEYNAME_PROPERTY = "redis.geo.keyname"
const QUERY_TYPE_GEODIST_RADIUS = "geodist-radius"
const QUERY_TYPE_GEODIST_BBOX = "geodist-bbox"

type datapoint struct {
	success        bool
	duration_ms    int64
	resultset_size int64
}

type GeoPoint struct {
	LatLon []float64 `json:"location"`
}

type GeoShape struct {
	Shape string `json:"shape"`
}

func lineToLonLat(line string) (float64, float64) {
	var geo GeoPoint
	json.Unmarshal([]byte(line), &geo)
	lon := geo.LatLon[0]
	lat := geo.LatLon[1]
	return lon, lat
}

func lineToPolygon(line string) string {
	var geo GeoShape
	json.Unmarshal([]byte(line), &geo)
	return geo.Shape
}
