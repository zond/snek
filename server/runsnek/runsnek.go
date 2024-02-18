// runsnek starts a demo service, hosting some data in an SQLite
// database and serving it via a WebSocket API supporting updates
// and subscriptions.
package main

import (
	"fmt"
	"log"
	"os"

	"github.com/zond/snek"
	"github.com/zond/snek/server"
)

// Member defines memberships in chat groups.
type Member struct {
	ID      snek.ID
	GroupID snek.ID
	UserID  snek.ID
}

// queryControlMember gatekeeps view access to Member instances.
// This method only blocks users from loading memberships of other users.
func queryControlMember(v *snek.View, query *snek.Query) error {
	isOK, err := snek.Cond{"UserID", snek.EQ, v.Caller().UserID()}.Includes(query.Set)
	if err != nil {
		return err
	}
	if !isOK {
		return fmt.Errorf("can only query your own memberships")
	}
	return nil
}

// updateControlMember gatekeeps update access to Member instances.
// This method blocks users from updating memberships, or create/remove
// memberships for other users.
func updateControlMember(u *snek.Update, prev, next *Member) error {
	if prev == nil && next != nil {
		if !next.UserID.Equal(u.Caller().UserID()) {
			return fmt.Errorf("can only create your own memberships")
		}
		return nil
	} else if prev != nil && next == nil {
		if !prev.UserID.Equal(u.Caller().UserID()) {
			return fmt.Errorf("can only remove your own memberships")
		}
		return nil
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
// This method appends a JOIN to the query that ensures only the
// users own memberships are returned.
func queryControlMessage(v *snek.View, query *snek.Query) error {
	query.Joins = append(query.Joins, snek.NewJoin(&Member{}, snek.Cond{"UserID", snek.EQ, v.Caller().UserID()}, []snek.On{{"GroupID", snek.EQ, "GroupID"}}))
	return nil
}

// updateControlMessage gatekeeps update access to Message instances.
// This method blocks update or removal of messages, and ensures that
// any inserted message is from the user, and that there is a membership
// of the user in the group of the message.
func updateControlMessage(u *snek.Update, prev, next *Message) error {
	if prev == nil && next != nil {
		if !next.Sender.Equal(u.Caller().UserID()) {
			return fmt.Errorf("can only insert messages from yourself")
		}
		members := []Member{}
		if err := u.Select(&members, &snek.Query{Set: snek.And{snek.Cond{"UserID", snek.EQ, u.Caller().UserID()}, snek.Cond{"GroupID", snek.EQ, next.GroupID}}}); err != nil {
			return err
		}
		if len(members) == 0 {
			return fmt.Errorf("can only insert messages into your own groups")
		}
		return nil
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
	// Register the Member and Message types, along with the control methods to gatekeep them.
	if err := server.Register(s, &Member{}, queryControlMember, updateControlMember); err != nil {
		log.Fatal(err)
	}
	if err := server.Register(s, &Message{}, queryControlMessage, updateControlMessage); err != nil {
		log.Fatal(err)
	}
	log.Printf("Opened %q, will listen to %q", opts.Path, opts.Addr)
	// Finally start the server.
	if err := s.Run(); err != nil {
		log.Fatal(err)
	}
}
