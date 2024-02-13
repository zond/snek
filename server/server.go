package server

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
	"github.com/zond/snek"
)

type Message struct {
	Type string
}

type server struct {
	snek *snek.Snek
	conn *websocket.Conn
	opts Options
	out  chan Message
}

func (s *server) readPump() {
	defer s.conn.Close()
	s.conn.SetReadDeadline(time.Now().Add(s.opts.PongWait))
	s.conn.SetPongHandler(func(string) error {
		s.conn.SetReadDeadline(time.Now().Add(s.opts.PongWait))
		return nil
	})
	for {
		_, message, err := s.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("unexpected close: %v", err)
			}
			break
		}
		fmt.Println("received", message)
	}
}

func (s *server) writePump() {
	ticker := time.NewTicker(s.opts.PingPeriod)
	defer func() {
		ticker.Stop()
		s.conn.Close()
	}()
	for {
		select {
		case message, ok := <-s.out:
			s.conn.SetWriteDeadline(time.Now().Add(s.opts.WriteWait))
			if !ok {
				s.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			b, err := json.Marshal(message)
			if err != nil {
				log.Printf("while marshalling %+v: %v", message, err)
				s.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			if err := s.conn.WriteMessage(websocket.BinaryMessage, b); err != nil {
				log.Printf("while sending %+v: %v", message, err)
				return
			}
		case <-ticker.C:
			s.conn.SetWriteDeadline(time.Now().Add(s.opts.WriteWait))
			if err := s.conn.WriteMessage(websocket.PingMessage, []byte{}); err != nil {
				return
			}
		}
	}
}

type Options struct {
	Addr       string
	Snek       *snek.Snek
	WriteWait  time.Duration
	PongWait   time.Duration
	PingPeriod time.Duration
}

func DefaultOptions(addr string, snek *snek.Snek) Options {
	return Options{
		Addr:       addr,
		Snek:       snek,
		WriteWait:  10 * time.Second,
		PongWait:   60 * time.Second,
		PingPeriod: 50 * time.Second,
	}
}

func (o Options) Run() error {
	upgrader := websocket.Upgrader{
		EnableCompression: true,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Printf("while upgrading %+v, %+v: %v", w, r, err)
			return
		}
		s := &server{
			conn: conn,
			snek: o.Snek,
			opts: o,
			out:  make(chan Message),
		}
		go s.readPump()
		go s.writePump()
	})
	httpServer := &http.Server{
		Addr:    o.Addr,
		Handler: mux,
	}
	return httpServer.ListenAndServe()
}
