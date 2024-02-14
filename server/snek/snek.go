package main

import (
	"log"

	"github.com/zond/snek"
	"github.com/zond/snek/server"
)

func main() {
	snekOpts := snek.DefaultOptions("snek.db")
	snek, err := snekOpts.Open()
	if err != nil {
		log.Fatal(err)
	}
	serverOpts := server.DefaultOptions("0.0.0.0:8080", snek)
	log.Printf("Opened %q, will listen to %q", snekOpts.Path, serverOpts.Addr)
	if err := serverOpts.Run(); err != nil {
		log.Fatal(err)
	}
}
