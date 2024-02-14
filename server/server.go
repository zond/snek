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
		return fmt.Errorf("exactly one of the nullable fields of Match must be populated, not %+v", m)
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

// Sent from client to server to attain a caller identity.
type Identity struct {
	Provider string
	Token    string
}

// Sent in both directions.
type Message struct {
	ID           snek.ID
	Subscription *Subscription `json:",omitempty"`
	Data         *Data         `json:",omitempty"`
	Update       *Update       `json:",omitempty"`
	Result       *Result       `json:",omitempty"`
	Identity     *Identity     `json:",omitempty"`
}

func (s *server) response(m *Message, err error) *Message {
	errMessage := &Message{
		ID:     s.snek.NewID(),
		Result: &Result{},
	}
	if m != nil {
		errMessage.Result.CauseMessageID = m.ID
	}
	if err != nil {
		errString := err.Error()
		errMessage.Result.Error = &errString
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
	if m.Identity != nil {
		nonNilFields++
	}
	if nonNilFields != 1 {
		return fmt.Errorf("exactly one of the nullable fields of Message must be populated, not %+v", m)
	}
	return nil
}

type server struct {
	snek   *snek.Snek
	conn   *websocket.Conn
	opts   Options
	lock   synch.Lock
	caller *synch.S[snek.Caller]
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
					s.send(s.response(nil, err))
					return
				}
				if err := message.validate(); err != nil {
					s.send(s.response(message, err))
					return
				}
				log.Printf("received message %+v", message)

				switch {
				case message.Subscription != nil:
					log.Printf("received subscription %+v", message.Subscription)
				case message.Update != nil:
					log.Printf("received update %+v", message.Update)
				case message.Identity != nil:
					caller, err := s.opts.Identifier.Identify(message.Identity)
					if err != nil {
						s.send(s.response(message, err))
						return
					}
					log.Printf("caller identified as %+v", caller)
					s.caller.Set(caller)
					s.send(s.response(message, nil))
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

type anonymousCaller struct{}

func (a anonymousCaller) UserID() snek.ID {
	return nil
}

func (a anonymousCaller) IsAdmin() bool {
	return false
}

func (a anonymousCaller) IsSystem() bool {
	return false
}

type AnonymousIdentifier struct{}

func (a AnonymousIdentifier) Identify(*Identity) (snek.Caller, error) {
	return anonymousCaller{}, nil
}

type Identifier interface {
	Identify(*Identity) (snek.Caller, error)
}

type Options struct {
	Addr       string
	Snek       *snek.Snek
	WriteWait  time.Duration
	PongWait   time.Duration
	PingPeriod time.Duration
	Identifier Identifier
}

func DefaultOptions(addr string, snek *snek.Snek, identifier Identifier) Options {
	return Options{
		Addr:       addr,
		Snek:       snek,
		WriteWait:  10 * time.Second,
		PongWait:   60 * time.Second,
		PingPeriod: 50 * time.Second,
		Identifier: identifier,
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
			conn:   conn,
			snek:   o.Snek,
			opts:   o,
			caller: synch.New[snek.Caller](nil),
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
