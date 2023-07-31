package cmd

import "encoding/json"

const REDIS_TYPE_GEO = "redis-geo"
const REDIS_TYPE_JSON = "redisearch-json"
const REDIS_TYPE_HASH = "redisearch-hash"
const REDIS_TYPE_GENERIC = "redis"
const ELASTIC_TYPE_GENERIC = "elasticsearch"
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
const DEFAULT_QUERY_TIMEOUT = 10000

type datapoint struct {
	success        bool
	duration_ms    int64
	resultset_size int64
}

type GeoPoint struct {
	LatLon []float64 `json:"location"`
}

type GeoShape struct {
	Shape   string `json:"shape"`
	NPoints int64  `json:"npoints"`
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
