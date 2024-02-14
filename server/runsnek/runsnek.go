package main

import (
	"log"

	"github.com/zond/snek"
	"github.com/zond/snek/server"
)

type Message struct {
	ID     snek.ID
	Sender string
	Body   string
}

func main() {
	opts := server.DefaultOptions("0.0.0.0:8080", "snek.db", server.AnonymousIdentifier{})
	s, err := opts.Open()
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Opened %q, will listen to %q", opts.Path, opts.Addr)
	if err := s.Run(); err != nil {
		log.Fatal(err)
	}
}
