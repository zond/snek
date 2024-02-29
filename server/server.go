package server

import (
	"encoding/hex"
	"fmt"
	"log"
	"net/http"
	"reflect"
	"sync/atomic"
	"time"

	"github.com/fxamacker/cbor/v2"
	"github.com/gorilla/websocket"
	"github.com/zond/snek"
	"github.com/zond/snek/synch"
)

// Match represents a serializable snek.Set.
type Match struct {
	And  []Match    `sbor:",omitempty"`
	Or   []Match    `sbor:",omitempty"`
	Cond *snek.Cond `sbor:",omitempty"`
}

func (m *Match) String() string {
	return fmt.Sprintf("%+v", *m)
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
	if nonNilFields > 1 {
		return fmt.Errorf("at most one of the nullable fields of Match must be populated, not %+v", m)
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
	case m.Cond != nil:
		return m.Cond, nil
	default:
		return snek.All{}, nil
	}
}

// Sent from client to server. Represents a serializable snek.Query for a given type.
type Subscribe struct {
	TypeName string
	Order    []snek.Order `sbor:",omitempty"`
	Limit    uint         `sbor:",omitempty"`
	Distinct bool         `sbor:",omitempty"`
	Match    Match        `sbor:",omitempty"`
}

func (s *Subscribe) toQuery() (*snek.Query, error) {
	set, err := s.Match.toSet()
	if err != nil {
		return nil, err
	}
	return &snek.Query{
		Set:      set,
		Limit:    s.Limit,
		Distinct: s.Distinct,
		Order:    s.Order,
	}, nil
}

func (s *Subscribe) String() string {
	return fmt.Sprintf("%+v", *s)
}

var (
	errType = reflect.TypeOf(new(error)).Elem()
	anyType = reflect.TypeOf(new(any)).Elem()
)

func (s *Subscribe) execute(c *client, causeMessageID snek.ID) error {
	typ, found := c.server.types[s.TypeName]
	if !found {
		return fmt.Errorf("%q not registered", s.TypeName)
	}
	query, err := s.toQuery()
	if err != nil {
		return err
	}
	subscriptionFunc := reflect.MakeFunc(reflect.FuncOf([]reflect.Type{anyType, errType}, []reflect.Type{errType}, false), func(args []reflect.Value) []reflect.Value {
		var err error
		switch v := args[1].Interface().(type) {
		case error:
			err = v
		}
		b := []byte{}
		if err == nil {
			b, err = cbor.Marshal(args[0].Interface())
		}
		errString := ""
		if err != nil {
			errString = err.Error()
		}
		msg := &Message{
			ID: c.server.Snek.NewID(),
			Data: &Data{
				CauseMessageID: causeMessageID,
				Error:          errString,
				Blob:           b,
			},
		}
		if err := c.send(msg); err != nil {
			return []reflect.Value{reflect.ValueOf(err)}
		}
		return []reflect.Value{reflect.Zero(reflect.TypeOf((*error)(nil)).Elem())}
	})
	subscription, err := snek.Subscribe(c.server.Snek, c.caller.Get(), query, snek.AnySubscriber(typ, subscriptionFunc.Interface().(func(any, error) error)))
	if err != nil {
		return err
	}
	idString := string(causeMessageID)
	if sub, found := c.subscriptions[idString]; found {
		sub.Close()
	}
	c.subscriptions[idString] = subscription
	return nil
}

// Sent by server after initial Subscribe and every time the data matching set of data is modified.
type Data struct {
	CauseMessageID snek.ID
	Error          string      `sbor:",omitempty"`
	Blob           PrettyBytes `sbor:",omitempty"`
}

func (d *Data) String() string {
	return fmt.Sprintf("%+v", *d)
}

// PrettyBytes are bytes that default print as hex encoded.
type PrettyBytes []byte

func (p PrettyBytes) String() string {
	return hex.EncodeToString([]byte(p))
}

// Sent from client to server.
type Update struct {
	TypeName string
	Insert   PrettyBytes `sbor:",omitempty"`
	Update   PrettyBytes `sbor:",omitempty"`
	Remove   PrettyBytes `sbor:",omitempty"`
}

func (u *Update) String() string {
	return fmt.Sprintf("%+v", *u)
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
	if err := cbor.Unmarshal(b, instance); err != nil {
		return err
	}
	return c.server.Snek.Update(c.caller.Get(), func(upd *snek.Update) error {
		switch op {
		case insert:
			return upd.Insert(instance)
		case update:
			return upd.Update(instance)
		default:
			return upd.Remove(instance)
		}
	})
}

// Sent from server as response to every message from the client.
type Result struct {
	CauseMessageID snek.ID
	Error          string      `sbor:",omitempty"`
	Aux            PrettyBytes `sbor:",omitempty"`
}

func (r *Result) String() string {
	return fmt.Sprintf("%+v", *r)
}

// Sent from client to server to attain a caller identity.
type Identity struct {
	Token snek.ID
}

func (i *Identity) String() string {
	return fmt.Sprintf("%+v", *i)
}

// Sent from client to server to cancel the subscription whose Response message had the ID defined by SubscriptionID.
type Unsubscribe struct {
	SubscriptionID snek.ID
}

func (u *Unsubscribe) String() string {
	return fmt.Sprintf("%+v", *u)
}

// Sent in both directions.
type Message struct {
	ID snek.ID

	// From client to server.
	Subscribe   *Subscribe   `sbor:",omitempty"`
	Unsubscribe *Unsubscribe `sbor:",omitempty"`
	Update      *Update      `sbor:",omitempty"`
	Identity    *Identity    `sbor:",omitempty"`

	// From server to client.
	Data   *Data   `sbor:",omitempty"`
	Result *Result `sbor:",omitempty"`
}

func (c *client) response(m *Message, aux PrettyBytes, err error) *Message {
	resp := &Message{
		ID:     c.server.Snek.NewID(),
		Result: &Result{},
	}
	if m != nil {
		resp.Result.CauseMessageID = m.ID
	}
	if err != nil {
		resp.Result.Error = err.Error()
	}
	if aux != nil {
		resp.Result.Aux = aux
	}
	return resp
}

func (m *Message) validate() error {
	nonNilFields := 0
	if m.Subscribe != nil {
		nonNilFields++
	}
	if m.Unsubscribe != nil {
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
	server        *Server
	conn          *websocket.Conn
	lock          synch.Lock
	caller        *synch.S[snek.Caller]
	closed        int32
	subscriptions map[string]snek.Subscription
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
				if err := cbor.Unmarshal(b, message); err != nil {
					log.Printf("while unmarshalling message: %v", err)
					c.send(c.response(nil, nil, fmt.Errorf("unable to parse message: %v", err)))
					return
				}
				if err := message.validate(); err != nil {
					log.Printf("while validating message: %v", err)
					c.send(c.response(message, nil, err))
					return
				}
				log.Printf("received message %+v", message)

				switch {
				case message.Subscribe != nil:
					c.send(c.response(message, nil, message.Subscribe.execute(c, message.ID)))
				case message.Unsubscribe != nil:
					stringID := string(message.Unsubscribe.SubscriptionID)
					if sub, found := c.subscriptions[stringID]; found {
						sub.Close()
						delete(c.subscriptions, stringID)
						c.send(c.response(message, nil, nil))
					} else {
						c.send(c.response(message, nil, fmt.Errorf("subscription %v not found", message.Unsubscribe.SubscriptionID)))
					}
				case message.Update != nil:
					c.send(c.response(message, nil, message.Update.execute(c)))
				case message.Identity != nil:
					caller, aux, err := c.server.opts.Identifier.Identify(message.Identity)
					if err != nil {
						log.Printf("caller failed to identify: %v", err)
						c.send(c.response(message, nil, err))
					} else {
						log.Printf("caller identified as %+v", caller)
						c.caller.Set(caller)
						c.send(c.response(message, aux, nil))
					}
				default:
					log.Printf("received unexpected message %+v", message)
				}
			}()
		}
	}
	c.conn.Close()
}

func (c *client) send(m *Message) error {
	b, err := cbor.Marshal(m)
	if err != nil {
		return err
	}
	err = c.lock.Sync(func() error {
		c.conn.SetWriteDeadline(time.Now().Add(c.server.opts.WriteWait))
		return c.conn.WriteMessage(websocket.BinaryMessage, b)
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

func (a AnonymousIdentifier) Identify(*Identity) (snek.Caller, PrettyBytes, error) {
	return anonymousCaller{}, nil, nil
}

// Identifier allows verifying identities into callers.
type Identifier interface {
	Identify(*Identity) (snek.Caller, PrettyBytes, error)
}

// Options contains server configuration.
type Options struct {
	Path        string
	Addr        string
	SnekOptions snek.Options
	WriteWait   time.Duration
	PongWait    time.Duration
	PingPeriod  time.Duration
	Identifier  Identifier
}

// DefaultOptions returns default options for the given interface address, database path, and identifier.
func DefaultOptions(addr string, path string, identifier Identifier) Options {
	snekOpts := snek.DefaultOptions(path)
	return Options{
		SnekOptions: snekOpts,
		Addr:        addr,
		Path:        path,
		WriteWait:   10 * time.Second,
		PongWait:    60 * time.Second,
		PingPeriod:  50 * time.Second,
		Identifier:  identifier,
	}
}

// Server serves websockets to a snek database.
type Server struct {
	Snek       *snek.Snek
	opts       Options
	types      map[string]reflect.Type
	mux        *http.ServeMux
	httpServer *http.Server
	Upgrader   *websocket.Upgrader
}

// Open returns a server using the provided options.
func (o Options) Open() (*Server, error) {
	s, err := o.SnekOptions.Open()
	if err != nil {
		return nil, err
	}
	result := &Server{
		Snek:  s,
		opts:  o,
		types: map[string]reflect.Type{},
		mux:   http.NewServeMux(),
		Upgrader: &websocket.Upgrader{
			EnableCompression: true,
		},
	}
	result.httpServer = &http.Server{
		Addr:    o.Addr,
		Handler: result.mux,
	}
	result.mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		conn, err := result.Upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Printf("while upgrading %+v, %+v: %v", w, r, err)
			return
		}
		c := &client{
			conn:          conn,
			server:        result,
			subscriptions: map[string]snek.Subscription{},
			caller:        synch.New[snek.Caller](snek.AnonCaller{}),
		}
		go c.pingLoop()
		go c.readLoop()
		log.Printf("%v connected", conn.RemoteAddr())
	})
	return result, nil
}

// Mux returns the mux for this server.
func (s *Server) Mux() *http.ServeMux {
	return s.mux
}

// Register registers the type of the example structPointer in the server and store and ensures there is a table for the type.
func Register[T any](s *Server, structPointer *T, queryControl snek.QueryControl, updateControl snek.UpdateControl[T]) error {
	err := snek.Register(s.Snek, structPointer, queryControl, updateControl)
	if err != nil {
		return err
	}
	structType := reflect.TypeOf(structPointer).Elem()
	s.types[structType.Name()] = structType
	return nil
}

// Run starts the server.
func (s *Server) Run() error {
	return s.httpServer.ListenAndServe()
}
