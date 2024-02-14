package main

import (
	"fmt"
	"log"

	"github.com/zond/snek"
	"github.com/zond/snek/server"
)

type Member struct {
	ID      snek.ID
	GroupID snek.ID
	UserID  snek.ID
}

func queryControlMember(v *snek.View, s snek.Set) error {
	isOK, err := snek.Cond{"UserID", snek.EQ, v.Caller().UserID()}.Includes(s)
	if err != nil {
		return err
	}
	if !isOK {
		return fmt.Errorf("can only query your own memberships")
	}
	return nil
}

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

type Message struct {
	ID      snek.ID
	GroupID snek.ID
	Sender  snek.ID
	Body    string
}

func ownGroupsCond(v *snek.View) (snek.Set, error) {
	members := []Member{}
	if err := v.Select(&members, snek.Query{Set: snek.Cond{"UserID", snek.EQ, v.Caller().UserID()}}); err != nil {
		return nil, err
	}
	or := snek.Or{}
	for _, member := range members {
		or = append(or, snek.Cond{"GroupID", snek.EQ, member.GroupID})
	}
	return or, nil
}

func queryControlMessage(v *snek.View, s snek.Set) error {
	ownGroups, err := ownGroupsCond(v)
	if err != nil {
		return err
	}
	isOK, err := ownGroups.Includes(s)
	if err != nil {
		return err
	}
	if !isOK {
		return fmt.Errorf("can only query messages from your own groups")
	}
	return nil
}

func updateControlMessage(u *snek.Update, prev, next *Message) error {
	if prev == nil && next != nil {
		if !next.Sender.Equal(u.Caller().UserID()) {
			return fmt.Errorf("can only insert messages from yourself")
		}
		ownGroups, err := ownGroupsCond(u.View)
		if err != nil {
			return err
		}
		isOK, err := ownGroups.Includes(snek.Cond{"GroupID", snek.EQ, next.GroupID})
		if err != nil {
			return err
		}
		if !isOK {
			return fmt.Errorf("can only insert messages into your own groups")
		}
		return nil
	} else {
		return fmt.Errorf("can only insert messages")
	}
}

type trustingIdentifier struct{}

type simpleCaller struct {
	userID snek.ID
}

func (s simpleCaller) UserID() snek.ID {
	return s.userID
}

func (s simpleCaller) IsAdmin() bool {
	return false
}

func (s simpleCaller) IsSystem() bool {
	return false
}

func (t trustingIdentifier) Identify(i *server.Identity) (snek.Caller, error) {
	return simpleCaller{userID: []byte(i.Token)}, nil
}

func main() {
	opts := server.DefaultOptions("0.0.0.0:8080", "snek.db", trustingIdentifier{})
	s, err := opts.Open()
	if err != nil {
		log.Fatal(err)
	}
	if err := server.Register(s, &Member{}, queryControlMember, updateControlMember); err != nil {
		log.Fatal(err)
	}
	if err := server.Register(s, &Message{}, queryControlMessage, updateControlMessage); err != nil {
		log.Fatal(err)
	}
	log.Printf("Opened %q, will listen to %q", opts.Path, opts.Addr)
	if err := s.Run(); err != nil {
		log.Fatal(err)
	}
}
