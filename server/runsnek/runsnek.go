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
	snekOpts := snek.DefaultOptions("snek.db")
	s, err := snekOpts.Open()
	if err != nil {
		log.Fatal(err)
	}
	if err := snek.Register(s, &Message{}, snek.UncontrolledQueries, snek.UncontrolledUpdates(&Message{})); err != nil {
		log.Fatal(err)
	}
	serverOpts := server.DefaultOptions("0.0.0.0:8080", s, server.AnonymousIdentifier{})
	log.Printf("Opened %q, will listen to %q", snekOpts.Path, serverOpts.Addr)
	if err := serverOpts.Run(); err != nil {
		log.Fatal(err)
	}
}
