// runsnek starts a demo service, hosting some data in an SQLite
// database and serving it via a WebSocket API supporting updates
// and subscriptions.
package main

import (
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/zond/snek"
	"github.com/zond/snek/server"
)

// Group defines group of members.
type Group struct {
	ID      snek.ID
	OwnerID snek.ID
}

// queryControlGroup gatekeeps view access to Group instances.
func queryControlGroup(v *snek.View, query *snek.Query) error {
	return snek.SetIncludes(snek.Cond{"OwnerID", snek.EQ, v.Caller().UserID()}, query.Set)
}

// updateControlGroup gatekeeps update access to Group instances.
func updateControlGroup(u *snek.Update, prev, next *Group) error {
	if prev == nil && next != nil {
		if !next.OwnerID.Equal(u.Caller().UserID()) {
			return fmt.Errorf("can only insert your own groups")
		}
		return nil
	} else if prev != nil && next == nil {
		if !prev.OwnerID.Equal(u.Caller().UserID()) {
			return fmt.Errorf("can only remove your own groups")
		}
		return nil
	} else {
		return fmt.Errorf("can't update groups")
	}
}

// Member defines memberships in chat groups.
type Member struct {
	ID      snek.ID
	GroupID snek.ID
	UserID  snek.ID
}

// queryControlMember gatekeeps view access to Member instances.
func queryControlMember(v *snek.View, query *snek.Query) error {
	query.Joins = append(query.Joins, snek.NewJoin(&Member{}, snek.Cond{"UserID", snek.EQ, v.Caller().UserID()}, []snek.On{{"GroupID", snek.EQ, "GroupID"}}))
	return nil
}

// updateControlMember gatekeeps update access to Member instances.
func updateControlMember(u *snek.Update, prev, next *Member) error {
	if prev == nil && next != nil {
		return snek.QueryHasResults(u.View, []Group{}, &snek.Query{Set: snek.And{snek.Cond{"ID", snek.EQ, next.GroupID}, snek.Cond{"OwnerID", snek.EQ, u.Caller().UserID()}}})
	} else if prev != nil && next == nil {
		return snek.QueryHasResults(u.View, []Group{}, &snek.Query{Set: snek.And{snek.Cond{"ID", snek.EQ, prev.GroupID}, snek.Cond{"OwnerID", snek.EQ, u.Caller().UserID()}}})
	} else {
		return fmt.Errorf("can't update memberships")
	}
}

// Message defines a chat message in a chat group.
type Message struct {
	ID      snek.ID
	GroupID snek.ID
	Sender  snek.ID
	Body    string
}

// queryControlMessage gatekeeps view access to Message instances.
func queryControlMessage(v *snek.View, query *snek.Query) error {
	query.Joins = append(query.Joins, snek.NewJoin(&Member{}, snek.Cond{"UserID", snek.EQ, v.Caller().UserID()}, []snek.On{{"GroupID", snek.EQ, "GroupID"}}))
	return nil
}

// updateControlMessage gatekeeps update access to Message instances.
func updateControlMessage(u *snek.Update, prev, next *Message) error {
	if prev == nil && next != nil {
		if !next.Sender.Equal(u.Caller().UserID()) {
			return fmt.Errorf("can only insert messages from yourself")
		}
		return snek.QueryHasResults(u.View, []Member{}, &snek.Query{Set: snek.And{snek.Cond{"GroupID", snek.EQ, next.GroupID}, snek.Cond{"UserID", snek.EQ, u.Caller().UserID()}}})
	} else {
		return fmt.Errorf("can only insert messages")
	}
}

// trustingIdentifier is used to verify user claimed identities.
type trustingIdentifier struct{}

// Identify will return a Caller (trusted user identity) which just
// assume whatever the user claimed was true.
func (t trustingIdentifier) Identify(i *server.Identity) (snek.Caller, error) {
	return simpleCaller{userID: i.Token}, nil
}

// simpleCaller is a container for a userID.
type simpleCaller struct {
	userID snek.ID
}

// UserID returns the user ID of the caller.
func (s simpleCaller) UserID() snek.ID {
	return s.userID
}

// IsAdmin always returns false, since the example app doesn't use admin access.
func (s simpleCaller) IsAdmin() bool {
	return false
}

// IsSystem always returns false, since the example app doesn't use system access.
func (s simpleCaller) IsSystem() bool {
	return false
}

func main() {
	// Create options for a WebSocket listning at :8080, using an SQLite databas at snek.db,
	// that simply trusts all connecting users to identify themselves correctly.
	opts := server.DefaultOptions("0.0.0.0:8080", "snek.db", trustingIdentifier{})
	opts.SnekOptions.Logger = log.Default()
	opts.SnekOptions.LogSQL = os.Getenv("VERBOSE_SNEK") == "true"
	s, err := opts.Open()
	if err != nil {
		log.Fatal(err)
	}
	s.Mux().HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, html)
	})
	// Register the Member and Message types, along with the control methods to gatekeep them.
	if err := server.Register(s, &Member{}, queryControlMember, updateControlMember); err != nil {
		log.Fatal(err)
	}
	if err := server.Register(s, &Message{}, queryControlMessage, updateControlMessage); err != nil {
		log.Fatal(err)
	}
	if err := server.Register(s, &Group{}, queryControlGroup, updateControlGroup); err != nil {
		log.Fatal(err)
	}
	log.Printf("opened %q, will listen to %q", opts.Path, opts.Addr)
	// Finally start the server.
	if err := s.Run(); err != nil {
		log.Fatal(err)
	}
}

var html = `<html>
<head>
<title>snek demo</title>
<script>
document.addEventListener('DOMContentLoaded', (ev) => {
  const newID = () => {
    const res = new Uint8Array(32);
	window.crypto.getRandomValues(res);
    const now = Date.now();
    res[0] = now >> 24;
    res[1] = (now >> 16) & 0xff;
    res[2] = (now >> 8) & 0xff;
    res[3] = now & 0xff;
	return btoa(String.fromCharCode.apply(null, res));
  };
  const log = (text) => {
    const div = document.createElement('div');
    const textNode = document.createTextNode(new Date().toLocaleTimeString() + ' ' + text);
	div.appendChild(textNode)
    document.getElementById('log').prepend(div);
  };
  let backoff = 1;
  const identityField = document.getElementById('identity');
  let identityChangeHandler = (ev) => {};
  identityField.addEventListener('change', (ev) => { identityChangeHandler(ev); });
  const connect = () => {
    identityField.value = '';
	identityChangeHandler = (ev) => {};
	console.log('connecting in ' + backoff);
    setTimeout(() => {
	  const awaitingResponse = {'': () => {
	    log('response to unknown message?');
	  },
	  null: () => {
	    log('response to unknown message?');
	  }};
	  const subscriptions = {};
	  try {
	    log('connecting socket');
        const socket = new WebSocket('ws://localhost:8080/ws');
	    socket.addEventListener('error', (ev) => {
          log('socket error ', ev);
		  connect();
	    });
        socket.addEventListener('open', (ev) => {
          log('socket opened');
	      socket.addEventListener('close', (ev) => {
	        log('socket closed');
		    connect();
	      });
          socket.addEventListener('message', (ev) => {
            log('message received: ' + ev.data);
	    	const obj = JSON.parse(ev.data);
			if (obj.Result && obj.Result.CauseMessageID in awaitingResponse) {
	    	  awaitingResponse[obj.Result.CauseMessageID](obj);
	    	  delete awaitingResponse[obj.Result.CauseMessageID];
			}
			if (obj.Data && obj.Data.CauseMessageID in subscriptions) {
			  subscriptions[obj.Data.CauseMessageID](JSON.parse(atob(obj.Data.Blob)));
			}
          });
          const send = (msg) => {
	        return new Promise((res, rej) => {
              msg.ID = newID();
			  if ('Subscribe' in msg) {
			    subscriptions[msg.ID] = (obj) => {
				  log('got subscription result ' + JSON.stringify(obj));
				};
			  }
          	  const json = JSON.stringify(msg);
          	  log('sending ' + json);
	    	  awaitingResponse[msg.ID] = (resp) => {
	    	    if (resp.Result.Error) {
	    		  rej(resp);
	    		} else {
	    	      res(resp);
	    		}
	    	  };
          	  socket.send(json);
	    	});
          };
	      const subscribe = (typeName, match) => {
			send({Subscribe: {TypeName: typeName, Match: match}}).then((resp) => {
	    	  log('subscribed to ' + typeName);
	    	});
	      };
          identityChangeHandler = (ev) => {
            const userID = btoa(identityField.value);
            send({Identity: {Token: userID}});
			subscribe('Group', {Cond: {Field: 'OwnerID', Comparator: '=', Value: userID}});
          };
        });
	  } catch (e) {
	    log('connection failed');
		connect();
	  }
	}, backoff);
    backoff = Math.max(1000, Math.min(30*1000, backoff)) * 2;
  };
  connect();
});
</script>
<style>
#log {
  font-size: x-small;
  overflow: auto;
  height: 10em;
  border: 1px solid grey;
}
</style>
</head>
<body>
<h1>snek demo</h1>
<div id='log'></div>
<input type='text' id='identity' placeholder='identity' />
</body>
</html>
`
