package server

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/zond/snek"
	"github.com/zond/snek/synch"
)

type Match struct {
	And  []Match
	Or   []Match
	Cond *snek.Cond
}

func (m *Match) validate() error {
	nonNilFields := 0
	if len(m.And) > 0 {
		nonNilFields++
	}
	if len(m.Or) > 0 {
		nonNilFields++
	}
	if m.Cond != nil {
		nonNilFields++
	}
	if nonNilFields != 1 {
		return fmt.Errorf("exactly one of Match.And, Match.Or, or Match.Cond must be populated, not %+v", m)
	}
	return nil
}

func (m *Match) toSet() (snek.Set, error) {
	if err := m.validate(); err != nil {
		return nil, err
	}
	makeSubSet := func(subMatches []Match) ([]snek.Set, error) {
		result := []snek.Set{}
		for _, subMatch := range subMatches {
			subSet, err := subMatch.toSet()
			if err != nil {
				return nil, err
			}
			result = append(result, subSet)
		}
		return result, nil
	}
	switch {
	case len(m.And) > 0:
		subSet, err := makeSubSet(m.And)
		return snek.And(subSet), err
	case len(m.Or) > 0:
		subSet, err := makeSubSet(m.And)
		return snek.Or(subSet), err
	default:
		return *m.Cond, nil
	}
}

// Sent from client to server.
type Subscription struct {
	TypeName string
	Order    []snek.Order `json:",omitempy"`
	Limit    int          `json:",omitempty"`
	Match    Match        `json:",omitempty"`
}

// Sent by server after initial Subscription and every time the data matching set of data is modified.
type Data struct {
	CauseMessageID []byte
	Blob           []byte
}

// Sent from client to server.
type Update struct {
	TypeName string
	Blob     []byte
}

// Sent from server as response to Update and Subscription.
type Result struct {
	CauseMessageID []byte
	Error          *string
}

// Sent in both directions.
type Message struct {
	ID           snek.ID
	Subscription *Subscription
	Data         *Data
	Update       *Update
	Result       *Result
}

func (s *server) errResponse(m *Message, err error) *Message {
	errString := err.Error()
	errMessage := &Message{
		ID: s.snek.NewID(),
		Result: &Result{
			Error: &errString,
		},
	}
	if m != nil {
		errMessage.Result.CauseMessageID = s.snek.NewID()
	}
	return errMessage
}

func (m *Message) validate() error {
	nonNilFields := 0
	if m.Subscription != nil {
		nonNilFields++
	}
	if m.Data != nil {
		nonNilFields++
	}
	if m.Update != nil {
		nonNilFields++
	}
	if m.Result != nil {
		nonNilFields++
	}
	if nonNilFields != 1 {
		return fmt.Errorf("exactly one of Message.Subscription, Message.Data, Message.Update, and Message.Result must be populated, not %+v", m)
	}
	return nil
}

type server struct {
	snek   *snek.Snek
	conn   *websocket.Conn
	opts   Options
	lock   synch.Lock
	closed int32
}

func (s *server) readLoop() {
	atomic.StoreInt32(&s.closed, 0)
	for atomic.LoadInt32(&s.closed) == 0 {
		if _, b, err := s.conn.ReadMessage(); err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("unexpected close: %v", err)
			} else {
				log.Printf("connection closed: %v", err)
			}
			atomic.StoreInt32(&s.closed, 1)
		} else {
			go func() {
				message := &Message{}
				if err := json.Unmarshal(b, message); err != nil {
					s.send(s.errResponse(nil, err))
					return
				}
				if err := message.validate(); err != nil {
					s.send(s.errResponse(message, err))
					return
				}
				log.Printf("received message %+v", message)

				switch {
				case message.Subscription != nil:
					log.Printf("received subscription %+v", message.Subscription)
				case message.Update != nil:
					log.Printf("received update %+v", message.Update)
				}
			}()
		}
	}
	s.conn.Close()
}

func (s *server) send(m *Message) error {
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	err = s.lock.Sync(func() error {
		s.conn.SetWriteDeadline(time.Now().Add(s.opts.WriteWait))
		return s.conn.WriteMessage(websocket.TextMessage, b)
	})
	if err != nil {
		log.Printf("while sending %+v: %v", m, err)
		atomic.StoreInt32(&s.closed, 1)
	}
	return err
}

func (s *server) pingLoop() {
	s.conn.SetReadDeadline(time.Now().Add(s.opts.PongWait))
	s.conn.SetPongHandler(func(string) error {
		s.conn.SetReadDeadline(time.Now().Add(s.opts.PongWait))
		return nil
	})
	for atomic.LoadInt32(&s.closed) == 0 {
		time.Sleep(s.opts.PingPeriod)
		s.conn.SetWriteDeadline(time.Now().Add(s.opts.WriteWait))
		if err := s.lock.Sync(func() error {
			return s.conn.WriteMessage(websocket.PingMessage, []byte{})
		}); err != nil {
			log.Printf("while sending ping to client: %v", err)
			atomic.StoreInt32(&s.closed, 1)
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
		}
		go s.pingLoop()
		go s.readLoop()
		log.Printf("%v connected", conn.RemoteAddr())
	})
	httpServer := &http.Server{
		Addr:    o.Addr,
		Handler: mux,
	}
	return httpServer.ListenAndServe()
}
