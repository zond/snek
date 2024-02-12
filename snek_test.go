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
	t.Helper()
	idSet := map[string]bool{}
	for _, id := range ids {
		idSet[id.String()] = true
	}
	for _, element := range elements {
		stringID := reflect.ValueOf(element).FieldByName("ID").Interface().(ID).String()
		if !idSet[stringID] {
			t.Errorf("wanted %+v to contain exactly %+v, but found %v", elements, ids, stringID)
		}
		delete(idSet, stringID)
	}
	if len(idSet) > 0 {
		t.Errorf("wanted %+v to contain exactly %+v, but didn't contain %v", elements, ids, len(idSet))
	}
}

func mustList[T any](t *testing.T, elements []T, ids []ID) {
	t.Helper()
	for i := range elements {
		if reflect.ValueOf(elements[i]).FieldByName("ID").Interface().(ID).String() != ids[i].String() {
			t.Errorf("wanted %+v to contain exactly %+v", elements, ids)
		}
	}
}

func (t *testSnek) mustTrue(b bool, err error) {
	t.t.Helper()
	if err != nil {
		t.t.Errorf("got %v, wanted no error", err)
	}
	if !b {
		t.t.Errorf("got %v, wanted true", b)
	}
}

func (t *testSnek) mustFalse(b bool, err error) {
	t.t.Helper()
	if err != nil {
		t.t.Errorf("got %v, wanted no error", err)
	}
	if b {
		t.t.Errorf("got %v, wanted false", b)
	}
}

func (t *testSnek) must(err error) {
	t.t.Helper()
	if err != nil {
		t.t.Errorf("got %v, wanted no error", err)
	}
}

func (t *testSnek) mustNot(err error) {
	t.t.Helper()
	if err == nil {
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

func TestSetMatches(t *testing.T) {
	withSnek(t, func(s *testSnek) {
		ts := reflect.ValueOf(&testStruct{ID: s.NewID(), String: "string1", Int: 1, Inner: innerTestStruct{Float: 1}})
		s.mustTrue(Cond{"String", EQ, "string1"}.matches(ts))
		s.mustFalse(Cond{"String", NE, "string1"}.matches(ts))
		s.mustTrue(Or{Cond{"String", NE, "string1"}, Cond{"String", EQ, "string1"}}.matches(ts))
		s.mustTrue(All{}.matches(ts))
	})
}

func testComparatorExcludesContains[T ~int | ~float32](t *testing.T, values []T) {
	TODO(make this complete?)
	comparators := []Comparator{EQ, NE, GT, GE, LT, LE}
	for _, cmp1 := range comparators {
		for _, cmp2 := range comparators {
			excluder, found := excludes[cmp1][cmp2]
			if !found {
				excluder = func(a, b reflect.Value) (bool, error) {
					return false, nil
				}
			}
			container, found := contains[cmp1][cmp2]
			if !found {
				container = func(a, b reflect.Value) (bool, error) {
					return false, nil
				}
			}
			for _, a := range values {
				valA := reflect.ValueOf(a)
				for _, b := range values {
					valB := reflect.ValueOf(b)
					match1, err := cmp1.apply(valA, valB)
					if err != nil {
						t.Fatal(err)
					}
					match2, err := cmp2.apply(valA, valB)
					if err != nil {
						t.Fatal(err)
					}
					gotExclude, err := excluder(valA, valB)
					if err != nil {
						t.Fatal(err)
					}
					gotContain, err := container(valA, valB)
					if err != nil {
						t.Fatal(err)
					}
					if gotExclude && match1 && match2 {
						t.Errorf("%v %v %v (%v) and %v %v %v (%v) should be exclusive", a, cmp1, b, match1, a, cmp2, b, match2)
					}
					if gotContain && match2 && !match1 {
						t.Errorf("%v %v %v (%v) and %v %v %v (%v) should be containing", a, cmp1, b, match1, a, cmp2, b, match2)
					}
				}
			}
		}
	}
}

func TestComparatorExcludesContains(t *testing.T) {
	testComparatorExcludesContains(t, []int{2, 3, 4, 5, 6})
}

//func TestSetExcludes(t *testing.T) {
//	withSnek(t, func(s *testSnek) {
//		s.mustTrue(Cond{"A", EQ, 5}.excludes(Cond{"A", NE, 5}))
//		s.mustTrue(Cond{"String", EQ, "string1"}.matches(ts))
//		s.mustFalse(Cond{"String", NE, "string1"}.matches(ts))
//		s.mustTrue(Or{Cond{"String", NE, "string1"}, Cond{"String", EQ, "string1"}}.matches(ts))
//		s.mustTrue(All{}.matches(ts))
//	})
//}
