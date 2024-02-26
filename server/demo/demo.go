// demo starts a demo service, hosting some data in an SQLite
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

func (m Member) Unique() [][]string {
	return [][]string{{"GroupID", "UserID"}}
}

// queryControlMember gatekeeps view access to Member instances.
func queryControlMember(v *snek.View, query *snek.Query) error {
	ownedGroups := []Group{}
	if err := v.Select(&ownedGroups, &snek.Query{Set: snek.Cond{"OwnerID", snek.EQ, v.Caller().UserID()}}); err != nil {
		return err
	}
	memberships := []Member{}
	if err := v.Select(&memberships, &snek.Query{Set: snek.Cond{"UserID", snek.EQ, v.Caller().UserID()}}); err != nil {
		return err
	}
	okCond := snek.Or{}
	for _, ownedGroup := range ownedGroups {
		okCond = append(okCond, snek.Cond{"GroupID", snek.EQ, ownedGroup.ID})
	}
	for _, membership := range memberships {
		okCond = append(okCond, snek.Cond{"GroupID", snek.EQ, membership.GroupID})
	}
	onlyOwned, err := okCond.Includes(query.Set)
	if err != nil {
		return err
	}
	if onlyOwned {
		return nil
	}
	return fmt.Errorf("can only query memberships of owned or member groups")
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
<script src='https://cdn.jsdelivr.net/npm/cbor-js@0.1.0/cbor.js'></script>
<script>
document.addEventListener('DOMContentLoaded', (ev) => {
  const utf8enc = new TextEncoder();
  const utf8dec = new TextDecoder();
  const simplify = (o) => {
    const res = {};
	Object.keys(o).forEach((key) => {
	  const value = o[key];
	  if (value instanceof Uint8Array) {
	    res[key] = btoa(value);
	  } else if (value instanceof ArrayBuffer) {
	    res[key] = btoa(value);
	  } else if (value instanceof Object) {
	    res[key] = simplify(value);
	  } else {
	    res[key] = value;
	  }
	});
    return res;
  };
  const toArrayBuffer = (uint8Array) => {
    return uint8Array.buffer.slice(uint8Array.byteOffset, uint8Array.byteLength + uint8Array.byteOffset);
  };
  const byteSerialize = (obj) => {
    return new Uint8Array(CBOR.encode(obj));
  };
  const byteUnserialize = (uint8Array) => {
    return CBOR.decode(toArrayBuffer(uint8Array));
  };
  const pp = (o) => {
    return JSON.stringify(simplify(o));
  };
  const newID = () => {
    const res = new Uint8Array(32);
	window.crypto.getRandomValues(res);
    const now = Date.now();
    res[0] = now >> 24;
    res[1] = (now >> 16) & 0xff;
    res[2] = (now >> 8) & 0xff;
    res[3] = now & 0xff;
	return res;
  };
  const log = (msg) => {
    const div = document.createElement('div');
    const text = document.createTextNode(new Date().toLocaleTimeString() + ' ' + msg);
	div.appendChild(text)
    document.getElementById('log').prepend(div);
  };
  let backoff = 1;
  const identityField = document.getElementById('identity');
  let identityChangeHandler = (ev) => {};
  identityField.addEventListener('change', (ev) => { identityChangeHandler(ev); });
  const newGroupField = document.getElementById('new_group');
  let newGroupChangeHandler = (ev) => {};
  newGroupField.addEventListener('change', (ev) => { newGroupChangeHandler(ev); });
  const ownedGroupsSpan = document.getElementById('owned_groups');
  const newMemberField = document.getElementById('new_member');
  let newMemberChangeHandler = (ev) => {};
  newMemberField.addEventListener('change', (ev) => { newMemberChangeHandler(ev); });
  const groupMembersSpan = document.getElementById('group_members');
  let groupMemberUnsubscribe = (() => {});
  const connect = () => {
    identityField.value = '';
	newGroupField.setAttribute('disabled', true);
	identityChangeHandler = (ev) => {};
	newGroupChangeHandler = (ev) => {};
	ownedGroupsSpan.innerHTML = '';
	groupMembersSpan.innerHTML = '';
	newMemberField.setAttribute('disable', true);
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
		socket.binaryType = 'arraybuffer';
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
	    	const obj = CBOR.decode(ev.data);
            log('message received: ' + pp(obj));
			if (obj.Result && obj.Result.CauseMessageID in awaitingResponse) {
	    	  awaitingResponse[obj.Result.CauseMessageID](obj);
	    	  delete awaitingResponse[obj.Result.CauseMessageID];
			}
			if (obj.Data && obj.Data.CauseMessageID in subscriptions) {
			  subscriptions[obj.Data.CauseMessageID](byteUnserialize(obj.Data.Blob));
			}
          });
          const send = (msg, opts = {}) => {
	        return new Promise((res, rej) => {
              msg.ID = newID();
			  if ('Subscribe' in msg) {
			    subscriptions[msg.ID] = (opts['subscriber'] || (() => {}));
			  }
          	  log('sending ' + pp(msg));
	    	  awaitingResponse[msg.ID] = (resp) => {
	    	    if (resp.Result.Error) {
	    		  rej(resp);
	    		} else {
				  if ('Subscribe' in msg) {
				    resp['unsubscribe'] = () => {
					  send({ID: newID(), Unsubscribe: {SubscriptionID: msg.ID}}).then((res) => {
					    if (resp.Result.Error) {
						  log('error unsubscribing: ', resp.Result.Error);
						} else {
						  log('unsubscribed from ' + msg.Subscribe.TypeName);
						}
					  });
					};
				  }
	    	      res(resp);
	    		}
	    	  };
          	  socket.send(CBOR.encode(msg));
	    	});
          };
	      const subscribe = (typeName, match, handler) => {
		    return new Promise((res, rej) => {
			  send({Subscribe: {TypeName: typeName, Match: match}}, { subscriber: handler }).then((resp) => {
	    	    log('subscribed to ' + typeName);
				res(resp.unsubscribe);
			  }).catch((err) => {
			    rej(err);
			  });
			});
	      };
          identityChangeHandler = (ev) => {
            const userID = utf8enc.encode(identityField.value);
            send({Identity: {Token: userID}}).then(() => {
			  newGroupField.removeAttribute('disabled');
			  newGroupChangeHandler = (ev) => {
			    if (newGroupField.value) {
			      const newGroup = {ID: utf8enc.encode(newGroupField.value), OwnerID: userID};
				  log('creating ' + pp(newGroup));
			 	  send({Update: {TypeName: 'Group', Insert: byteSerialize(newGroup)}}).then((res) => {
				    newGroupField.value = '';
				  }).catch((err) => {
				    log('failed creating group: ' + pp(err));
				  });
				}
			  };
			  subscribe('Group', {Cond: {Field: 'OwnerID', Comparator: '=', Value: userID}}, (res) => {
	            ownedGroupsSpan.innerHTML = '';
				res.forEach((group) => {
				  const groupName = utf8dec.decode(group.ID);
				  const span = document.createElement('span');
				  span.setAttribute('class', 'group');
				  const button = document.createElement('button');
				  button.setAttribute('id', 'group_' + groupName);
				  const text = document.createTextNode(groupName);
				  button.appendChild(text);
				  span.appendChild(button);
				  ownedGroupsSpan.appendChild(span);
				  button.addEventListener('click', (ev) => {
				    newMemberField.removeAttribute('disabled');
					newMemberChangeHandler = (ev) => {
					  if (newMemberField.value) {
					    const newMember = {ID: newID(), GroupID: group.ID, UserID: utf8enc.encode(newMemberField.value)};
					    log('creating ' + pp(newMember));
					    send({Update: {TypeName: 'Member', Insert: byteSerialize(newMember)}}).then((res) => {
						  newMemberField.value = '';
						}).catch((err) => {
						  log('failed creating member: ' + pp(err));
						});
					  }
					};
					groupMemberUnsubscribe();
					subscribe('Member', {Cond: {Field: 'GroupID', Comparator: '=', Value: group.ID}}, (res) => {
					  groupMembersSpan.innerHTML = '';
					  res.forEach((member) => {
					    const memberName = utf8dec.decode(member.UserID);
						const span = document.createElement('span');
						span.setAttribute('class', 'member');
						const removeButton = document.createElement('button');
						removeButton.setAttribute('class', 'remove_button');
						removeButton.addEventListener('click', (ev) => {
						  send({Update: {TypeName: 'Member', Remove: byteSerialize(member)}});
						});
						const trashcan = document.createTextNode('🗑️');
						const text = document.createTextNode(memberName);
						span.appendChild(text);
						removeButton.appendChild(trashcan);
						span.appendChild(removeButton);
						groupMembersSpan.appendChild(span);
					  });
					}).then((unsub) => {
					  groupMemberUnsubscribe = unsub;
					});
				  });
				});
			  });
			});
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
#identity {
  margin: 0.5em;
}
#log {
  font-size: x-small;
  overflow: auto;
  height: 10em;
  border: 1px solid grey;
}
.group {
  margin-left: 1em;
  display: inline-block;
  overflow: hidden;
  white-space: nowrap;
  text-overflow: ellipsis;
  vertical-align: middle;
}
.member {
  margin-left: 1em;
  display: inline-block;
  overflow: hidden;
  white-space: nowrap;
  text-overflow: ellipsis;
  vertical-align: middle;
}
.ownership {
  margin: 0.5em;
  border: 1px solid grey;
}
.remove_button {
  font-size: xx-small;
  margin-left: 0.5em;
}
</style>
</head>
<body>
<h1>snek demo</h1>
<div id='log'></div>
<input type='text' id='identity' placeholder='identity' />
<div class='ownership'>
<h4>Owned groups</h4>
<div>
<input disabled type='text' id='new_group' placeholder='new group name' />
<span id='owned_groups'></span>
</div>
<div>
<input disabled type='text' id='new_member' placeholder='new member name' />
<span id='group_members'></span>
</div>
</div>
</body>
</html>
`
