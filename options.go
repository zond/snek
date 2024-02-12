package snek

import (
	"context"
	"log"
	"math/rand"

	"github.com/jmoiron/sqlx"
)

type Options struct {
	Path       string
	RandomSeed int64
	Logger     *log.Logger
	LogExec    bool
	LogQuery   bool
}

func DefaultOptions(path string) Options {
	return Options{
		Path: path,
	}
}

func (o Options) Open() (*Snek, error) {
	db, err := sqlx.Open("sqlite3", o.Path)
	if err != nil {
		return nil, err
	}
	db.MapperFunc(func(s string) string {
		return s
	})
	return &Snek{
		ctx:     context.Background(),
		db:      db,
		options: o,
		rng:     rand.New(rand.NewSource(o.RandomSeed)),
	}, nil
}
