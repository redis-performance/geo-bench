package cmd

import "context"

// DB is the layer to access the database to be benchmarked.
type DB interface {
	InsertPolygon(ctx context.Context, key string, field string, polygon string, ops *uint64) error
}
