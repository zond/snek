package server

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"reflect"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/zond/snek"
	"github.com/zond/snek/synch"
)

// Match represents a serializable snek.Set.
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

// Sent from client to server. Represents a serializable snek.Query for a given type.
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
	Insert   []byte
	Update   []byte
	Remove   []byte
}

type updateOp string

const (
	insert updateOp = "insert"
	update updateOp = "update"
	remove updateOp = "remove"
)

func (u *Update) execute(c *client) error {
	var op updateOp
	var b []byte
	nonNilFields := 0
	if len(u.Insert) > 0 {
		op = insert
		b = u.Insert
		nonNilFields++
	}
	if len(u.Update) > 0 {
		op = update
		b = u.Update
		nonNilFields++
	}
	if len(u.Remove) > 0 {
		op = remove
		b = u.Remove
		nonNilFields++
	}
	if nonNilFields != 1 {
		return fmt.Errorf("exactly one of the nullable fields of Update must be populated, not %+v", u)
	}
	typ, found := c.server.types[u.TypeName]
	if !found {
		return fmt.Errorf("%q not registered", u.TypeName)
	}
	instance := reflect.New(typ).Interface()
	if err := json.Unmarshal(b, instance); err != nil {
		return err
	}
	return c.server.snek.Update(c.caller.Get(), func(u *snek.Update) error {
		switch op {
		case insert:
			return u.Insert(instance)
		case update:
			return u.Insert(instance)
		default:
			return u.Insert(instance)
		}
	})
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

func (c *client) response(m *Message, err error) *Message {
	errMessage := &Message{
		ID:     c.server.snek.NewID(),
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

type client struct {
	server *Server
	conn   *websocket.Conn
	lock   synch.Lock
	caller *synch.S[snek.Caller]
	closed int32
}

func (c *client) readLoop() {
	atomic.StoreInt32(&c.closed, 0)
	for atomic.LoadInt32(&c.closed) == 0 {
		if _, b, err := c.conn.ReadMessage(); err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("unexpected close: %v", err)
			} else {
				log.Printf("connection closed: %v", err)
			}
			atomic.StoreInt32(&c.closed, 1)
		} else {
			go func() {
				message := &Message{}
				if err := json.Unmarshal(b, message); err != nil {
					c.send(c.response(nil, err))
					return
				}
				if err := message.validate(); err != nil {
					c.send(c.response(message, err))
					return
				}
				log.Printf("received message %+v", message)

				switch {
				case message.Subscription != nil:
					log.Printf("received subscription %+v", message.Subscription)
				case message.Update != nil:
					c.send(c.response(message, message.Update.execute(c)))
				case message.Identity != nil:
					caller, err := c.server.opts.Identifier.Identify(message.Identity)
					if err != nil {
						c.send(c.response(message, err))
					} else {
						log.Printf("caller identified as %+v", caller)
						c.caller.Set(caller)
						c.send(c.response(message, nil))
					}
				}
			}()
		}
	}
	c.conn.Close()
}

func (c *client) send(m *Message) error {
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	err = c.lock.Sync(func() error {
		c.conn.SetWriteDeadline(time.Now().Add(c.server.opts.WriteWait))
		return c.conn.WriteMessage(websocket.TextMessage, b)
	})
	if err != nil {
		log.Printf("while sending %+v: %v", m, err)
		atomic.StoreInt32(&c.closed, 1)
	}
	return err
}

func (c *client) pingLoop() {
	c.conn.SetReadDeadline(time.Now().Add(c.server.opts.PongWait))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(c.server.opts.PongWait))
		return nil
	})
	for atomic.LoadInt32(&c.closed) == 0 {
		time.Sleep(c.server.opts.PingPeriod)
		c.conn.SetWriteDeadline(time.Now().Add(c.server.opts.WriteWait))
		if err := c.lock.Sync(func() error {
			return c.conn.WriteMessage(websocket.PingMessage, []byte{})
		}); err != nil {
			log.Printf("while sending ping to client: %v", err)
			atomic.StoreInt32(&c.closed, 1)
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

// An Identifier that always identifies as anonymous callers.
type AnonymousIdentifier struct{}

func (a AnonymousIdentifier) Identify(*Identity) (snek.Caller, error) {
	return anonymousCaller{}, nil
}

// Identifier allows verifying identities into callers.
type Identifier interface {
	Identify(*Identity) (snek.Caller, error)
}

// Options contains server configuration.
type Options struct {
	Path       string
	Addr       string
	Snek       *snek.Snek
	WriteWait  time.Duration
	PongWait   time.Duration
	PingPeriod time.Duration
	Identifier Identifier
}

// DefaultOptions returns default options for the given interface address, database path, and identifier.
func DefaultOptions(addr string, path string, identifier Identifier) Options {
	return Options{
		Addr:       addr,
		Path:       path,
		WriteWait:  10 * time.Second,
		PongWait:   60 * time.Second,
		PingPeriod: 50 * time.Second,
		Identifier: identifier,
	}
}

// Server serves websockets to a snek database.
type Server struct {
	snek  *snek.Snek
	opts  Options
	types map[string]reflect.Type
}

// Open returns a server using the provided options.
func (o Options) Open() (*Server, error) {
	s, err := snek.DefaultOptions(o.Path).Open()
	if err != nil {
		return nil, err
	}
	return &Server{
		opts:  o,
		snek:  s,
		types: map[string]reflect.Type{},
	}, nil
}

// Register registers the type of the example structPointer in the server and store and ensures there is a table for the type.
func Register[T any](s *Server, structPointer *T, queryControl snek.QueryControl, updateControl snek.UpdateControl[T]) error {
	err := snek.Register(s.snek, structPointer, queryControl, updateControl)
	if err != nil {
		return err
	}
	structType := reflect.TypeOf(structPointer).Elem()
	s.types[structType.Name()] = structType
	return nil
}

// Run starts the server.
func (s *Server) Run() error {
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
		c := &client{
			conn:   conn,
			server: s,
			caller: synch.New[snek.Caller](nil),
		}
		go c.pingLoop()
		go c.readLoop()
		log.Printf("%v connected", conn.RemoteAddr())
	})
	httpServer := &http.Server{
		Addr:    s.opts.Addr,
		Handler: mux,
	}
	return httpServer.ListenAndServe()
}
