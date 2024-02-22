package server

import (
	"encoding/base64"
	"reflect"
	"testing"

	"github.com/fxamacker/cbor/v2"
)

func TestNestedCBOR(t *testing.T) {
	m := &Message{
		ID: []byte("id"),
		Update: &Update{
			TypeName: "typeName",
		},
	}
	b, err := cbor.Marshal(map[string]any{
		"ID":      []byte("id"),
		"OwnerID": []byte("ownerID"),
	})
	if err != nil {
		t.Fatal(err)
	}
	m.Update.Insert = b
	b2, err := cbor.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	m2 := &Message{}
	if err := cbor.Unmarshal(b2, m2); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(m, m2) {
		t.Errorf("%+v != %+v", m, m2)
	}
}

func TestJSCBOR(t *testing.T) {
	/*
		Constructed via:
		btoa(String.fromCharCode.apply(null, new Uint8Array(CBOR.encode({ID: new Uint8Array(32), Update: {TypeName: "typeName", Insert: new Uint8Array(CBOR.encode({ID: new Uint8Array(32), OwnerID: new Uint8Array(32)}))}}))))
	*/
	b64String := "omJJRFggAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABmVXBkYXRlomhUeXBlTmFtZWh0eXBlTmFtZWZJbnNlcnRYUKJiSURYIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAZ093bmVySURYIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	b, err := base64.URLEncoding.DecodeString(b64String)
	if err != nil {
		t.Fatal(err)
	}
	m := &Message{}
	if err := cbor.Unmarshal(b, m); err != nil {
		t.Fatal(err)
	}
}
