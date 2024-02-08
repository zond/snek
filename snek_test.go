package snek

import (
	"os"
	"testing"
)

type testLogger struct {
	t *testing.T
}

func (t testLogger) Errorf(format string, params ...interface{}) {
	t.t.Errorf(format, params...)
}

func (t testLogger) Warningf(format string, params ...interface{}) {
}

func (t testLogger) Infof(format string, params ...interface{}) {
}

func (t testLogger) Debugf(format string, params ...interface{}) {
}

func withSnek(t *testing.T, f func(s *Snek)) {
	dir, err := os.MkdirTemp(os.TempDir(), "snek_test")
	if err != nil {
		t.Fatal(err)
	}
	opts := DefaultOptions(dir)
	opts.Logger = testLogger{}
	s, err := opts.Open()
	defer func() {
		s.Close()
		os.RemoveAll(dir)
	}()
	if err != nil {
		t.Fatal(err)
	}
	f(s)
}

func TestOpen(t *testing.T) {
	withSnek(t, func(s *Snek) {
		if s == nil {
			t.Fatal("wanted non nil")
		}
	})
}

type innerTestStruct struct {
	Float float64
}

type testStruct struct {
	ID     []byte
	Int    int32
	String string
	Bool   bool
	Inner  innerTestStruct
}

func TestInsertGetUpdate(t *testing.T) {
	withSnek(t, func(s *Snek) {
		if err := s.Update(func(u *Update) error {
			return u.Insert(&testStruct{ID: []byte("id"), String: "string"})
		}); err != nil {
			t.Fatal(err)
		}
		ts := &testStruct{ID: []byte("id")}
		if err := s.View(func(v *View) error {
			return v.Get(ts)
		}); err != nil {
			t.Fatal(err)
		}
		if ts.String != "string" {
			t.Errorf("got %v, want 'string'", ts.String)
		}
		if err := s.Update(func(u *Update) error {
			return u.Update(&testStruct{ID: []byte("id"), String: "another string"})
		}); err != nil {
			t.Fatal(err)
		}
		if err := s.View(func(v *View) error {
			return v.Get(ts)
		}); err != nil {
			t.Fatal(err)
		}
		if ts.String != "another string" {
			t.Errorf("got %v, want 'another string'", ts.String)
		}
	})
}
