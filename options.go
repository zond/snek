package snek

import (
	"context"
	"log"
	"math/rand"

	"github.com/jmoiron/sqlx"
	"github.com/zond/snek/synch"
)

// Options defines the options to use when opening a store.
type Options struct {
	Path       string
	RandomSeed int64
	Logger     *log.Logger
	LogExec    bool
	LogQuery   bool
}

// DefaultOptions returns default options with the provided path as file storage.
func DefaultOptions(path string) Options {
	return Options{
		Path: path,
	}
}

// Open returns a store using the provided options.
func (o Options) Open() (*Snek, error) {
	db, err := sqlx.Open("sqlite3", o.Path)
	if err != nil {
		return nil, err
	}
	db.MapperFunc(func(s string) string {
		return s
	})
	return &Snek{
		ctx:           context.Background(),
		db:            db,
		options:       o,
		rng:           rand.New(rand.NewSource(o.RandomSeed)),
		subscriptions: synch.NewSMap[string, *synch.SMap[string, subscription]](),
		permissions:   map[string]permissions{},
	}, nil
}
