package snek

import (
	"log"
	"os"
	"path/filepath"
	"testing"
)

func must(t *testing.T, err error) {
	if err != nil {
		t.Fatal(err)
	}
}

func withSnek(t *testing.T, f func(s *Snek)) {
	dir, err := os.MkdirTemp(os.TempDir(), "snek_test")
	if err != nil {
		t.Fatal(err)
	}
	opts := DefaultOptions(filepath.Join(dir, "sqlite.db"))
	opts.Logger = log.Default()
	opts.LogExec = true
	opts.LogQuery = true
	s, err := opts.Open()
	defer func() {
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
	ID     ID
	Int    int32 `snek:"index"`
	String string
	Bool   bool `snek:"index"`
	Inner  innerTestStruct
}

func TestInsertGetUpdate(t *testing.T) {
	withSnek(t, func(s *Snek) {
		ts := &testStruct{ID: s.NewID(), String: "string"}
		must(t, s.AssertTable(ts))
		must(t, s.Update(func(u *Update) error {
			return u.Insert(ts)
		}))
		if err := s.Update(func(u *Update) error {
			return u.Insert(ts)
		}); err == nil {
			t.Errorf("got %v, want some error", err)
		}
		must(t, s.Update(func(u *Update) error {
			ts.String = "another string"
			return u.Update(ts)
		}))
		ts2 := &testStruct{ID: ts.ID}
		if err := s.View(func(v *View) error {
			return v.Get(ts2)
		}); err != nil {
			t.Fatal(err)
		}
		//		if ts.String != "string" {
		//			t.Errorf("got %v, want 'string'", ts.String)
		//		}
		//		if err := s.Update(func(u *Update) error {
		//			return u.Update(&testStruct{ID: []byte("id"), String: "another string"})
		//		}); err != nil {
		//			t.Fatal(err)
		//		}
		//		if err := s.View(func(v *View) error {
		//			return v.Get(ts)
		//		}); err != nil {
		//			t.Fatal(err)
		//		}
		//		if ts.String != "another string" {
		//			t.Errorf("got %v, want 'another string'", ts.String)
		//		}
	})
}

//func TestFieldEQIterator(t *testing.T) {
//	withSnek(t, func(s *Snek) {
//		if err := s.Update(func(u *Update) error {
//			if err := u.Insert(&testStruct{ID: []byte("id1"), String: "string1"}); err != nil {
//				return err
//			}
//			if err := u.Insert(&testStruct{ID: []byte("id2"), String: "string1"}); err != nil {
//				return err
//			}
//			if err := u.Insert(&testStruct{ID: []byte("id3"), String: "string2"}); err != nil {
//				return err
//			}
//			return nil
//		}); err != nil {
//			t.Fatal(err)
//		}
//		if err := s.View(func(v *View) error {
//			iter, err := v.fieldEqIterator(&testStruct{}, "String", "string1")
//			if err != nil {
//				return err
//			}
//			for key, more := iter.Next(); more; key, more = iter.Next() {
//				fmt.Println(key)
//			}
//			return nil
//		}); err != nil {
//			t.Fatal(err)
//		}
//	})
//}
