package snek

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

var (
	Verbose = os.Getenv("VERBOSE_SNEK") == "true"
)

func TestMain(m *testing.M) {
	if !Verbose {
		fmt.Println("For verbose testing:\nVERBOSE_SNEK=true go test -v")
	}
	os.Exit(m.Run())
}

type testSnek struct {
	*Snek
	t *testing.T
}

func mustContain[T any](t *testing.T, elements []T, ids []ID) {
	idSet := map[string]bool{}
	for _, id := range ids {
		idSet[id.String()] = true
	}
	for _, element := range elements {
		stringID := reflect.ValueOf(element).FieldByName("ID").Interface().(ID).String()
		if !idSet[stringID] {
			t.Helper()
			t.Errorf("wanted %+v to contain exactly %+v, but found %v", elements, ids, stringID)
		}
		delete(idSet, stringID)
	}
	if len(idSet) > 0 {
		t.Helper()
		t.Errorf("wanted %+v to contain exactly %+v, but didn't contain %v", elements, ids, len(idSet))
	}
}

func mustList[T any](t *testing.T, elements []T, ids []ID) {
	for i := range elements {
		if reflect.ValueOf(elements[i]).FieldByName("ID").Interface().(ID).String() != ids[i].String() {
			t.Helper()
			t.Errorf("wanted %+v to contain exactly %+v", elements, ids)
		}
	}
}

func (t *testSnek) mustTrue(b bool, err error) {
	if err != nil {
		t.t.Helper()
		t.t.Errorf("got %v, wanted no error", err)
	}
	if !b {
		t.t.Helper()
		t.t.Errorf("got %v, wanted true", b)
	}
}

func (t *testSnek) mustFalse(b bool, err error) {
	if err != nil {
		t.t.Helper()
		t.t.Errorf("got %v, wanted no error", err)
	}
	if b {
		t.t.Helper()
		t.t.Errorf("got %v, wanted false", b)
	}
}

func (t *testSnek) must(err error) {
	if err != nil {
		t.t.Helper()
		t.t.Errorf("got %v, wanted no error", err)
	}
}

func (t *testSnek) mustNot(err error) {
	if err == nil {
		t.t.Helper()
		t.t.Errorf("got nil, wanted some error")
	}
}

func withSnek(t *testing.T, f func(s *testSnek)) {
	dir, err := os.MkdirTemp(os.TempDir(), "snek_test")
	if err != nil {
		t.Fatal(err)
	}
	opts := DefaultOptions(filepath.Join(dir, "sqlite.db"))
	opts.Logger = log.Default()
	if Verbose {
		opts.LogExec = true
		opts.LogQuery = true
	}
	s, err := opts.Open()
	defer func() {
		os.RemoveAll(dir)
	}()
	if err != nil {
		t.Fatal(err)
	}
	f(&testSnek{
		Snek: s,
		t:    t,
	})
}

func TestOpen(t *testing.T) {
	withSnek(t, func(s *testSnek) {
		if s == nil {
			t.Fatalf("wanted non nil, got %v", s)
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

func TestInsertGetUpdateRemove(t *testing.T) {
	withSnek(t, func(s *testSnek) {
		ts := &testStruct{ID: s.NewID(), String: "string"}
		ts2 := &testStruct{ID: ts.ID}
		s.mustNot(s.View(func(v *View) error {
			return v.Get(ts2)
		}))
		s.must(s.AssertTable(ts))
		s.mustNot(s.View(func(v *View) error {
			return v.Get(ts2)
		}))
		s.must(s.Update(func(u *Update) error {
			return u.Insert(ts)
		}))
		s.must(s.View(func(v *View) error {
			return v.Get(ts2)
		}))
		if ts2.String != ts.String {
			t.Errorf("got %v, want %v", ts2.String, ts.String)
		}
		s.mustNot(s.Update(func(u *Update) error {
			return u.Insert(ts)
		}))
		ts.String = "another string"
		s.must(s.Update(func(u *Update) error {
			return u.Update(ts)
		}))
		s.must(s.View(func(v *View) error {
			return v.Get(ts2)
		}))
		if ts2.String != ts.String {
			t.Errorf("got %v, want %v", ts2.String, ts.String)
		}
		s.must(s.Update(func(u *Update) error {
			return u.Remove(ts)
		}))
		s.mustNot(s.View(func(v *View) error {
			return v.Get(ts)
		}))
	})
}

func TestSelect(t *testing.T) {
	withSnek(t, func(s *testSnek) {
		ts1 := &testStruct{ID: s.NewID(), String: "string1", Int: 1, Inner: innerTestStruct{Float: 1}}
		ts2 := &testStruct{ID: s.NewID(), String: "string2", Int: 2, Inner: innerTestStruct{Float: 1}}
		ts3 := &testStruct{ID: s.NewID(), String: "string3", Int: 3, Inner: innerTestStruct{Float: 2}}
		ts4 := &testStruct{ID: s.NewID(), String: "string4", Int: 4, Inner: innerTestStruct{Float: 2}}
		s.must(s.AssertTable(ts1))
		s.must(s.Update(func(u *Update) error {
			s.must(u.Insert(ts1))
			s.must(u.Insert(ts2))
			s.must(u.Insert(ts3))
			return u.Insert(ts4)
		}))
		s.must(s.View(func(v *View) error {
			res := []testStruct{}
			s.must(v.Select(&res, Query{Set: Or{
				Cond{"String", EQ, "string1"},
				Cond{"String", EQ, "string2"}}}))
			mustContain(t, res, []ID{ts1.ID, ts2.ID})
			s.must(v.Select(&res, Query{Set: And{
				Cond{"String", EQ, "string1"},
				Cond{"Int", EQ, 2}}}))
			mustContain(t, res, []ID{})
			s.must(v.Select(&res, Query{Set: And{
				Or{
					Cond{"String", EQ, "string1"},
					Cond{"String", EQ, "string2"}},
				Cond{"Int", EQ, 2}}}))
			mustContain(t, res, []ID{ts2.ID})
			s.must(v.Select(&res, Query{Set: Or{
				And{
					Cond{"String", EQ, "string1"},
					Cond{"Int", EQ, 2}},
				Cond{"Int", EQ, 2}}}))
			mustContain(t, res, []ID{ts2.ID})
			s.must(v.Select(&res, Query{Set: Cond{"Int", GT, 0}}))
			mustContain(t, res, []ID{ts1.ID, ts2.ID, ts3.ID, ts4.ID})
			s.must(v.Select(&res, Query{
				Limit: 2,
				Order: []Order{{"Int", true}},
				Set:   Cond{"Int", GT, 0}}))
			mustList(t, res, []ID{ts4.ID, ts3.ID})
			s.must(v.Select(&res, Query{
				Limit: 2,
				Order: []Order{{"Int", false}},
				Set:   Cond{"Int", GT, 0}}))
			mustList(t, res, []ID{ts1.ID, ts2.ID})
			s.must(v.Select(&res, Query{
				Limit: 2,
				Order: []Order{{"Inner.Float", true}, {"Int", false}},
				Set:   Cond{"Int", LE, 3}}))
			mustList(t, res, []ID{ts3.ID, ts1.ID})
			return nil
		}))
	})
}

func TestIncludes(t *testing.T) {
	withSnek(t, func(s *testSnek) {
		ts := reflect.ValueOf(&testStruct{ID: s.NewID(), String: "string1", Int: 1, Inner: innerTestStruct{Float: 1}})
		s.mustTrue(Cond{"String", EQ, "string1"}.includes(ts))
		s.mustFalse(Cond{"String", NE, "string1"}.includes(ts))
		s.mustTrue(Or{Cond{"String", NE, "string1"}, Cond{"String", EQ, "string1"}}.includes(ts))
		s.mustTrue(All{}.includes(ts))
	})
}
