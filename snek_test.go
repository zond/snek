package snek

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
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

func (t *testSnek) mustAny(a any, err error) {
	t.t.Helper()
	if err != nil {
		t.t.Errorf("got %v, wanted no error", err)
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

func mustUnavail[T any](t *testing.T, c chan T) {
	t.Helper()
	timer := time.NewTimer(10 * time.Millisecond)
	select {
	case <-timer.C:
	case v := <-c:
		t.Errorf("wanted channel to have no data available, got %v", v)
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
		opts.LogSQL = true
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

type timeTestStruct struct {
	ID       ID
	T        TimeText
	TPointer *TimeText
}

func TestTime(t *testing.T) {
	withSnek(t, func(s *testSnek) {
		tt := &timeTestStruct{ID: s.NewID()}
		s.must(Register(s.Snek, tt, UncontrolledQueries, UncontrolledUpdates(tt)))
		s.must(s.Update(AnonCaller{}, func(u *Update) error {
			return u.Insert(tt)
		}))
		found := &timeTestStruct{ID: tt.ID}
		s.must(s.View(AnonCaller{}, func(v *View) error {
			return v.Get(found)
		}))
		if !found.T.Time().IsZero() {
			t.Errorf("got %v, wanted 0", found.T)
		}
		tt.T = ToText(time.Now())
		s.must(s.Update(AnonCaller{}, func(u *Update) error {
			return u.Update(tt)
		}))
		s.must(s.View(AnonCaller{}, func(v *View) error {
			return v.Get(found)
		}))
		if !found.T.Time().Equal(tt.T.Time()) {
			t.Errorf("got %v, wanted %v", found.T, tt.T)
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
		s.mustNot(s.View(AnonCaller{}, func(v *View) error {
			return v.Get(ts2)
		}))
		s.must(Register(s.Snek, ts, UncontrolledQueries, UncontrolledUpdates(ts)))
		matchingString := make(chan []testStruct)
		s.mustAny(Subscribe(s.Snek, AnonCaller{}, &Query{Set: Cond{"String", EQ, "string"}}, TypedSubscriber(func(res []testStruct, err error) error {
			if err != nil {
				t.Fatal(err)
			}
			matchingString <- res
			return nil
		})))
		if got := <-matchingString; len(got) > 0 {
			t.Errorf("wanted no results, got %+v", got)
		}
		matchingAnotherString := make(chan []testStruct)
		anotherStringSubscription, err := Subscribe(s.Snek, AnonCaller{}, &Query{Set: Cond{"String", EQ, "another string"}}, TypedSubscriber(func(res []testStruct, err error) error {
			if err != nil {
				t.Fatal(err)
			}
			matchingAnotherString <- res
			return nil
		}))
		if err != nil {
			t.Fatal(err)
		}
		if got := <-matchingAnotherString; len(got) > 0 {
			t.Errorf("wanted no results, got %+v", got)
		}
		s.mustNot(s.View(AnonCaller{}, func(v *View) error {
			return v.Get(ts2)
		}))
		s.must(s.Update(AnonCaller{}, func(u *Update) error {
			return u.Insert(ts)
		}))
		if got := <-matchingString; len(got) != 1 || !got[0].ID.Equal(ts.ID) {
			t.Errorf("got %+v, wanted %+v", got, []testStruct{*ts})
		}
		mustUnavail(t, matchingAnotherString)
		s.must(s.View(AnonCaller{}, func(v *View) error {
			return v.Get(ts2)
		}))
		if ts2.String != ts.String {
			t.Errorf("got %v, want %v", ts2.String, ts.String)
		}
		s.mustNot(s.Update(AnonCaller{}, func(u *Update) error {
			return u.Insert(ts)
		}))
		ts.String = "another string"
		s.must(s.Update(AnonCaller{}, func(u *Update) error {
			return u.Update(ts)
		}))
		if got := <-matchingAnotherString; len(got) != 1 || !got[0].ID.Equal(ts.ID) {
			t.Errorf("got %+v, wanted %+v", got, []testStruct{*ts})
		}
		if got := <-matchingString; len(got) != 0 {
			t.Errorf("wanted no results, got %+v", got)
		}
		s.must(s.View(AnonCaller{}, func(v *View) error {
			return v.Get(ts2)
		}))
		if ts2.String != ts.String {
			t.Errorf("got %v, want %v", ts2.String, ts.String)
		}
		s.must(s.Update(AnonCaller{}, func(u *Update) error {
			return u.Remove(ts)
		}))
		if got := <-matchingAnotherString; len(got) != 0 {
			t.Errorf("wanted no results, got %+v", got)
		}
		mustUnavail(t, matchingString)
		s.mustNot(s.View(AnonCaller{}, func(v *View) error {
			return v.Get(ts)
		}))
		s.must(s.Update(AnonCaller{}, func(u *Update) error {
			return u.Insert(ts)
		}))
		if got := <-matchingAnotherString; len(got) != 1 || !got[0].ID.Equal(ts.ID) {
			t.Errorf("got %+v, wanted %+v", got, []testStruct{*ts})
		}
		s.must(anotherStringSubscription.Close())
		ts.Int = 99
		s.must(s.Update(AnonCaller{}, func(u *Update) error {
			return u.Update(ts)
		}))
		mustUnavail(t, matchingAnotherString)
	})
}

func TestSelect(t *testing.T) {
	withSnek(t, func(s *testSnek) {
		ts1 := &testStruct{ID: s.NewID(), String: "string1", Int: 1, Inner: innerTestStruct{Float: 1}}
		ts2 := &testStruct{ID: s.NewID(), String: "string2", Int: 2, Inner: innerTestStruct{Float: 1}}
		ts3 := &testStruct{ID: s.NewID(), String: "string3", Int: 3, Inner: innerTestStruct{Float: 2}}
		ts4 := &testStruct{ID: s.NewID(), String: "string4", Int: 4, Inner: innerTestStruct{Float: 2}}
		s.must(Register(s.Snek, ts1, UncontrolledQueries, UncontrolledUpdates(ts1)))
		s.must(s.Update(AnonCaller{}, func(u *Update) error {
			s.must(u.Insert(ts1))
			s.must(u.Insert(ts2))
			s.must(u.Insert(ts3))
			return u.Insert(ts4)
		}))
		s.must(s.View(AnonCaller{}, func(v *View) error {
			res := []testStruct{}
			s.must(v.Select(&res, &Query{Set: Or{
				Cond{"String", EQ, "string1"},
				Cond{"String", EQ, "string2"}}}))
			mustContain(t, res, []ID{ts1.ID, ts2.ID})
			s.must(v.Select(&res, &Query{Set: And{
				Cond{"String", EQ, "string1"},
				Cond{"Int", EQ, 2}}}))
			mustContain(t, res, []ID{})
			s.must(v.Select(&res, &Query{Set: And{
				Or{
					Cond{"String", EQ, "string1"},
					Cond{"String", EQ, "string2"}},
				Cond{"Int", EQ, 2}}}))
			mustContain(t, res, []ID{ts2.ID})
			s.must(v.Select(&res, &Query{Set: Or{
				And{
					Cond{"String", EQ, "string1"},
					Cond{"Int", EQ, 2}},
				Cond{"Int", EQ, 2}}}))
			mustContain(t, res, []ID{ts2.ID})
			s.must(v.Select(&res, &Query{Set: Cond{"Int", GT, 0}}))
			mustContain(t, res, []ID{ts1.ID, ts2.ID, ts3.ID, ts4.ID})
			s.must(v.Select(&res, &Query{
				Limit: 2,
				Order: []Order{{"Int", true}},
				Set:   Cond{"Int", GT, 0}}))
			mustList(t, res, []ID{ts4.ID, ts3.ID})
			s.must(v.Select(&res, &Query{
				Limit: 2,
				Order: []Order{{"Int", false}},
				Set:   Cond{"Int", GT, 0}}))
			mustList(t, res, []ID{ts1.ID, ts2.ID})
			s.must(v.Select(&res, &Query{
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
		ts := reflect.ValueOf(testStruct{ID: s.NewID(), String: "string1", Int: 1, Inner: innerTestStruct{Float: 1}})
		s.mustTrue(Cond{"String", EQ, "string1"}.matches(ts))
		s.mustFalse(Cond{"String", NE, "string1"}.matches(ts))
		s.mustTrue(Or{Cond{"String", NE, "string1"}, Cond{"String", EQ, "string1"}}.matches(ts))
		s.mustTrue(All{}.matches(ts))
	})
}

func contains[T ~int | ~float32](a, b map[T]bool) bool {
	for k := range b {
		if _, found := a[k]; !found {
			return false
		}
	}
	return true
}

func excludes[T ~int | ~float32](a, b map[T]bool) bool {
	for k := range b {
		if _, found := a[k]; found {
			return false
		}
	}
	return true
}

func testComparatorSetOperations[T ~int | ~float32](t *testing.T, xValues []T, compValues []T) {
	comparators := []Comparator{EQ, NE, GT, GE, LT, LE}
	for _, firstComparator := range comparators {
		// Skip first and last values so that we get brackets.
		for _, a := range compValues {
			// Find all x for which "x [firstComparator] a" => true.
			// E.g.
			// "x > 3":
			// [2,3,4,5,6] > 3 => firstComparatorSet = [4,5,6]
			firstComparatorSet := map[T]bool{}
			for _, x := range xValues {
				if firstComparatorResult, err := firstComparator.apply(reflect.ValueOf(x), reflect.ValueOf(a)); err != nil {
					t.Fatal(err)
				} else if firstComparatorResult {
					firstComparatorSet[x] = true
				}
			}
			for _, secondComparator := range comparators {
				// Skip first and last values so that we get brackets.
				for _, b := range compValues {
					// Find all x for which "x [secondComparator] b" => true.
					// E.g.
					// "x > 2":
					// [2,3,4,5,6] > 2 => secondComparatorSet = [3,4,5,6]
					// "x > 4":
					// [2,3,4,5,6] > 4 => secondComparatorSet = [5,6]
					// "x < 4":
					// [2,3,4,5,6] < 4 => secondComparatorSet = [2,3]
					// "x < 5":
					// [2,3,4,5,6] < 5 => secondComparatorSet = [2,3,4]
					// If secondComparatorSet is fully contained by firstComparatorSet, then "x [firstComparator] a" implies "x [secondComparator] b".
					// If secondComparatorSet is fully excluded by firstComparatorSet, then "x [firstComparator] a" implies "!(x [secondComparator] b)".
					// E.g.:
					// "x > 3" !=> "x > 2"
					// "x > 3" => "x > 4"
					// "x > 3" => "!(x < 4)"
					// "x > 3" !=> "x < 5"
					secondComparatorSet := map[T]bool{}
					for _, x := range xValues {
						if secondComparatorResult, err := secondComparator.apply(reflect.ValueOf(x), reflect.ValueOf(b)); err != nil {
							t.Fatal(err)
						} else if secondComparatorResult {
							secondComparatorSet[x] = true
						}
					}
					firstImpliesSecondFun, firstImpliesNotSecondFun, err := implications(firstComparator, secondComparator)
					if err != nil {
						t.Fatal(err)
					}
					gotFirstImpliesSecond, err := firstImpliesSecondFun(reflect.ValueOf(a), reflect.ValueOf(b))
					if err != nil {
						t.Fatal(err)
					}
					gotFirstImpliesNotSecond, err := firstImpliesNotSecondFun(reflect.ValueOf(a), reflect.ValueOf(b))
					if err != nil {
						t.Fatal(err)
					}
					wantFirstImpliesSecond := contains(secondComparatorSet, firstComparatorSet)
					wantFirstImpliesNotSecond := excludes(firstComparatorSet, secondComparatorSet)
					if wantFirstImpliesSecond != gotFirstImpliesSecond {
						if wantFirstImpliesSecond {
							t.Errorf("%T: x %v %v => x %v %v, but wasn't predicted", *new(T), firstComparator, a, secondComparator, b)
						} else {
							t.Errorf("%T: x %v %v !=> x %v %v, but was predicted", *new(T), firstComparator, a, secondComparator, b)
						}
					}
					if wantFirstImpliesNotSecond != gotFirstImpliesNotSecond {
						if wantFirstImpliesNotSecond {
							t.Errorf("%T: x %v %v => !(x %v %v), but wasn't predicted", *new(T), firstComparator, a, secondComparator, b)
						} else {
							t.Errorf("%T: x %v %v !=> !(x %v %v), but was predicted", *new(T), firstComparator, a, secondComparator, b)
						}
					}
				}
			}
		}
	}
}

func TestComparatorExcludesContains(t *testing.T) {
	// Not comparing to 1 and 8 to avoid empty and full sets.
	testComparatorSetOperations(t, []int{1, 2, 3, 4, 5, 6, 7, 8}, []int{2, 3, 4, 5, 6, 7})
	// Not comparing to consecutive numbers to simulate the possibility of floats between the comparison values.
	testComparatorSetOperations(t, []float32{1, 2, 3, 4, 5, 6, 7}, []float32{2, 4, 6})
}

func TestSetExcludes(t *testing.T) {
	withSnek(t, func(s *testSnek) {
		s.mustTrue(Cond{"A", NE, 5}.Excludes(Cond{"A", EQ, 5}))
		s.mustFalse(Cond{"A", NE, 5}.Excludes(Cond{"B", EQ, 5}))

		s.mustTrue(Cond{"A", EQ, 5}.Excludes(Cond{"A", EQ, 4}))
		s.mustFalse(Cond{"A", EQ, 5}.Excludes(Cond{"A", EQ, 5}))
		s.mustTrue(Cond{"A", EQ, 5}.Excludes(Cond{"A", NE, 5}))
		s.mustFalse(Cond{"A", EQ, 5}.Excludes(Cond{"A", NE, 4}))
		s.mustTrue(Cond{"A", EQ, 5}.Excludes(Cond{"A", GT, 5}))
		s.mustFalse(Cond{"A", EQ, 5}.Excludes(Cond{"A", GT, 4}))
		s.mustTrue(Cond{"A", EQ, 5}.Excludes(Cond{"A", GE, 6}))
		s.mustFalse(Cond{"A", EQ, 5}.Excludes(Cond{"A", GE, 5}))
		s.mustTrue(Cond{"A", EQ, 5}.Excludes(Cond{"A", LT, 5}))
		s.mustFalse(Cond{"A", EQ, 5}.Excludes(Cond{"A", LT, 6}))
		s.mustTrue(Cond{"A", EQ, 5}.Excludes(Cond{"A", LE, 4}))
		s.mustFalse(Cond{"A", EQ, 5}.Excludes(Cond{"A", LE, 5}))

		s.mustTrue(Cond{"A", NE, 5}.Excludes(Cond{"A", EQ, 5}))

		s.mustTrue(Cond{"A", GT, 5}.Excludes(Cond{"A", EQ, 5}))
		s.mustFalse(Cond{"A", GT, 5}.Excludes(Cond{"A", EQ, 6}))
		s.mustTrue(Cond{"A", GT, 5}.Excludes(Cond{"A", LT, 6}))
		s.mustFalse(Cond{"A", GT, 5}.Excludes(Cond{"A", LT, 7}))
		s.mustTrue(Cond{"A", GT, 5.0}.Excludes(Cond{"A", LT, 5.0}))
		s.mustFalse(Cond{"A", GT, 5.0}.Excludes(Cond{"A", LT, 6.0}))
		s.mustTrue(Cond{"A", GT, 5}.Excludes(Cond{"A", LE, 5}))
		s.mustFalse(Cond{"A", GT, 5}.Excludes(Cond{"A", LE, 6}))

		s.mustTrue(Cond{"A", GE, 5}.Excludes(Cond{"A", EQ, 4}))
		s.mustFalse(Cond{"A", GE, 5}.Excludes(Cond{"A", EQ, 5}))
		s.mustTrue(Cond{"A", GE, 5}.Excludes(Cond{"A", LT, 5}))
		s.mustFalse(Cond{"A", GE, 5}.Excludes(Cond{"A", LT, 6}))
		s.mustTrue(Cond{"A", GE, 5}.Excludes(Cond{"A", LE, 4}))
		s.mustFalse(Cond{"A", GE, 5}.Excludes(Cond{"A", LE, 5}))

		s.mustTrue(Cond{"A", LT, 5}.Excludes(Cond{"A", EQ, 5}))
		s.mustFalse(Cond{"A", LT, 5}.Excludes(Cond{"A", EQ, 4}))
		s.mustTrue(Cond{"A", LT, 5}.Excludes(Cond{"A", GT, 4}))
		s.mustFalse(Cond{"A", LT, 5}.Excludes(Cond{"A", GT, 3}))
		s.mustTrue(Cond{"A", LT, 5.0}.Excludes(Cond{"A", GT, 5.0}))
		s.mustFalse(Cond{"A", LT, 5.0}.Excludes(Cond{"A", GT, 4.0}))
		s.mustTrue(Cond{"A", LT, 5}.Excludes(Cond{"A", GE, 5}))
		s.mustFalse(Cond{"A", LT, 5}.Excludes(Cond{"A", GE, 4}))

		s.mustTrue(Cond{"A", LE, 5}.Excludes(Cond{"A", EQ, 6}))
		s.mustFalse(Cond{"A", LE, 5}.Excludes(Cond{"A", EQ, 5}))
		s.mustTrue(Cond{"A", LE, 5}.Excludes(Cond{"A", GT, 5}))
		s.mustFalse(Cond{"A", LE, 5}.Excludes(Cond{"A", GT, 4}))
		s.mustTrue(Cond{"A", LE, 5}.Excludes(Cond{"A", GE, 6}))
		s.mustFalse(Cond{"A", LE, 5}.Excludes(Cond{"A", GE, 5}))

		s.mustTrue(Or{Cond{"A", LT, 5}, Cond{"A", GT, 10}}.Excludes(And{Cond{"A", GE, 5}, Cond{"A", LE, 10}}))
		s.mustFalse(Or{Cond{"A", LT, 5}, Cond{"A", GT, 10}}.Excludes(And{Cond{"A", GE, 4}, Cond{"A", LE, 10}}))

		s.mustTrue(And{Cond{"A", LE, 5}, Cond{"A", LE, 9}}.Excludes(Or{Cond{"A", GT, 9}, Cond{"A", GT, 5}}))
		s.mustFalse(And{Cond{"A", LE, 5}, Cond{"A", LE, 9}}.Excludes(Or{Cond{"A", GT, 9}, Cond{"A", GT, 4}}))

		s.mustTrue(And{Cond{"A", GT, 5}, Cond{"B", LT, 5}}.Excludes(And{Cond{"A", LT, 10}, Cond{"B", GT, 5}}))
		s.mustFalse(And{Cond{"A", GT, 5}, Cond{"B", LT, 5}}.Excludes(And{Cond{"A", LT, 7}, Cond{"B", GT, 3}}))

		s.mustTrue(Or{Cond{"A", GT, 5}, Cond{"B", GT, 5}}.Excludes(And{Cond{"A", LT, 5}, Cond{"B", LT, 5}}))
		s.mustFalse(Or{Cond{"A", GT, 5}, Cond{"B", GT, 5}}.Excludes(Or{Cond{"A", LT, 5}, Cond{"B", LT, 5}}))

		s.mustTrue(Cond{"OwnerID", EQ, 1}.Excludes(None{}))
		s.mustFalse(Cond{"OwnerID", EQ, 1}.Excludes(All{}))

		s.mustTrue(And{Cond{"A", EQ, 1}, Cond{"B", EQ, 1}}.Excludes(Cond{"A", EQ, 2}))
		s.mustTrue(Cond{"A", EQ, 2}.Excludes(And{Cond{"A", EQ, 1}, Cond{"B", EQ, 1}}))
		s.mustFalse(And{Cond{"A", EQ, 1}, Cond{"B", EQ, 1}}.Excludes(Cond{"A", EQ, 1}))
		s.mustFalse(Cond{"A", EQ, 1}.Excludes(And{Cond{"A", EQ, 1}, Cond{"B", EQ, 1}}))

		s.mustFalse(Or{Cond{"A", EQ, 1}, Cond{"B", EQ, 1}}.Excludes(Cond{"A", EQ, 2}))
		s.mustFalse(Cond{"A", EQ, 2}.Excludes(Or{Cond{"A", EQ, 1}, Cond{"B", EQ, 1}}))
		s.mustTrue(Or{Cond{"A", EQ, 1}, Cond{"B", EQ, 1}}.Excludes(And{Cond{"A", EQ, 2}, Cond{"B", EQ, 2}}))
		// Known false negative.
		s.mustFalse(And{Cond{"A", EQ, 2}, Cond{"B", EQ, 2}}.Excludes(Or{Cond{"A", EQ, 1}, Cond{"B", EQ, 1}}))
	})
}

func TestSetIncludes(t *testing.T) {
	withSnek(t, func(s *testSnek) {
		s.mustTrue(Cond{"A", EQ, 5}.Includes(Cond{"A", EQ, 5}))
		s.mustFalse(Cond{"A", EQ, 5}.Includes(Cond{"B", EQ, 5}))

		s.mustTrue(Cond{"A", EQ, 5}.Includes(Cond{"A", EQ, 5}))
		s.mustFalse(Cond{"A", EQ, 5}.Includes(Cond{"A", EQ, 4}))

		s.mustTrue(Cond{"A", NE, 5}.Includes(Cond{"A", NE, 5}))
		s.mustFalse(Cond{"A", NE, 5}.Includes(Cond{"A", NE, 4}))

		s.mustTrue(Cond{"A", GT, 5}.Includes(Cond{"A", NE, 5}))
		s.mustFalse(Cond{"A", GT, 5}.Includes(Cond{"A", NE, 6}))
		s.mustTrue(Cond{"A", GT, 5}.Includes(Cond{"A", GT, 5}))
		s.mustFalse(Cond{"A", GT, 5}.Includes(Cond{"A", GT, 6}))
		s.mustTrue(Cond{"A", GT, 5}.Includes(Cond{"A", GE, 6}))
		s.mustFalse(Cond{"A", GT, 5}.Includes(Cond{"A", GE, 7}))
		s.mustTrue(Cond{"A", GT, 5.0}.Includes(Cond{"A", GE, 5.0}))
		s.mustFalse(Cond{"A", GT, 5.0}.Includes(Cond{"A", GE, 6.0}))

		s.mustTrue(Cond{"A", GE, 5}.Includes(Cond{"A", NE, 4}))
		s.mustFalse(Cond{"A", GE, 5}.Includes(Cond{"A", NE, 5}))
		s.mustTrue(Cond{"A", GE, 5}.Includes(Cond{"A", GT, 4}))
		s.mustFalse(Cond{"A", GE, 5}.Includes(Cond{"A", GT, 5}))
		s.mustTrue(Cond{"A", GE, 5}.Includes(Cond{"A", GE, 5}))
		s.mustFalse(Cond{"A", GE, 5}.Includes(Cond{"A", GE, 6}))

		s.mustTrue(Cond{"A", LT, 5}.Includes(Cond{"A", NE, 5}))
		s.mustFalse(Cond{"A", LT, 5}.Includes(Cond{"A", NE, 4}))
		s.mustTrue(Cond{"A", LT, 5}.Includes(Cond{"A", LT, 5}))
		s.mustFalse(Cond{"A", LT, 5}.Includes(Cond{"A", LT, 4}))
		s.mustTrue(Cond{"A", LT, 5}.Includes(Cond{"A", LE, 4}))
		s.mustFalse(Cond{"A", LT, 5}.Includes(Cond{"A", LE, 3}))
		s.mustTrue(Cond{"A", LT, 5.0}.Includes(Cond{"A", LE, 5.0}))
		s.mustFalse(Cond{"A", LT, 5.0}.Includes(Cond{"A", LE, 4.0}))

		s.mustTrue(Cond{"A", LE, 5}.Includes(Cond{"A", NE, 6}))
		s.mustFalse(Cond{"A", LE, 5}.Includes(Cond{"A", NE, 5}))
		s.mustTrue(Cond{"A", LE, 5}.Includes(Cond{"A", LT, 6}))
		s.mustFalse(Cond{"A", LE, 5}.Includes(Cond{"A", LT, 5}))
		s.mustTrue(Cond{"A", LE, 5}.Includes(Cond{"A", LE, 5}))
		s.mustFalse(Cond{"A", LE, 5}.Includes(Cond{"A", LE, 4}))

		s.mustTrue(And{Cond{"A", LT, 10}, Cond{"A", GT, 4}}.Includes(And{Cond{"A", GT, 6}, Cond{"A", LT, 9}}))
		s.mustFalse(And{Cond{"A", LT, 10}, Cond{"A", GT, 4}}.Includes(Or{Cond{"A", GT, 6}, Cond{"A", LT, 9}}))

		s.mustFalse(Cond{"OwnerID", EQ, 1}.Includes(All{}))
		s.mustTrue(Cond{"OwnerID", EQ, 1}.Includes(None{}))

		s.mustFalse(And{Cond{"A", EQ, 1}, Cond{"B", EQ, 1}}.Includes(Cond{"A", EQ, 1}))
		s.mustTrue(Cond{"A", EQ, 1}.Includes(And{Cond{"A", EQ, 1}, Cond{"B", EQ, 1}}))

		s.mustTrue(Or{Cond{"A", EQ, 1}, Cond{"B", EQ, 1}}.Includes(Cond{"A", EQ, 1}))
		s.mustFalse(Cond{"A", EQ, 1}.Includes(Or{Cond{"A", EQ, 1}, Cond{"B", EQ, 1}}))

		s.mustTrue(Or{Cond{"A", EQ, 1}, Cond{"B", EQ, 1}}.Includes(And{Cond{"A", EQ, 1}, Cond{"B", EQ, 1}}))
		s.mustFalse(Or{Cond{"A", EQ, 1}, Cond{"B", EQ, 1}}.Includes(And{Cond{"A", EQ, 2}, Cond{"B", EQ, 2}}))
		s.mustFalse(Or{Cond{"A", EQ, 1}, Cond{"B", EQ, 1}}.Includes(Or{Cond{"A", EQ, 2}, Cond{"B", EQ, 2}}))
		// Known false negative.
		s.mustFalse(Or{Cond{"A", EQ, 1}, Cond{"B", EQ, 1}}.Includes(Or{Cond{"A", EQ, 1}, Cond{"B", EQ, 1}}))
		s.mustFalse(And{Cond{"A", EQ, 1}, Cond{"B", EQ, 1}}.Includes(Or{Cond{"A", EQ, 1}, Cond{"B", EQ, 1}}))
		s.mustFalse(And{Cond{"A", EQ, 1}, Cond{"B", EQ, 1}}.Includes(And{Cond{"A", EQ, 2}, Cond{"B", EQ, 1}}))
	})
}

type testCaller struct {
	userID  ID
	isAdmin bool
}

func (t testCaller) UserID() ID {
	return t.userID
}

func (t testCaller) IsAdmin() bool {
	return t.isAdmin
}

func (t testCaller) IsSystem() bool {
	return false
}

func TestPermissions(t *testing.T) {
	withSnek(t, func(s *testSnek) {
		var queryError, updateError error
		caller := testCaller{userID: s.NewID()}
		s.must(Register(s.Snek, &testStruct{}, func(view *View, query *Query) error {
			if !view.Caller().UserID().Equal(caller.userID) {
				t.Errorf("got %s, want %s", view.Caller().UserID(), caller.userID)
			}
			return queryError
		}, func(update *Update, prev, next *testStruct) error {
			if !update.Caller().UserID().Equal(caller.userID) {
				t.Errorf("got %s, want %s", update.Caller().UserID(), caller.userID)
			}
			return updateError
		}))
		updateError = fmt.Errorf("not allowed!")
		ts := &testStruct{ID: s.NewID(), String: "string"}
		if err := s.Update(caller, func(u *Update) error {
			return u.Insert(ts)
		}); err != updateError {
			t.Errorf("got %v, want %v", err, updateError)
		}
		updateError = nil
		s.must(s.Update(caller, func(u *Update) error {
			return u.Insert(ts)
		}))
		queryError = fmt.Errorf("not allowed!!!")
		if err := s.View(caller, func(v *View) error {
			return v.Get(ts)
		}); err != queryError {
			t.Errorf("got %v, want %v", err, queryError)
		}
		queryError = nil
		s.must(s.View(caller, func(v *View) error {
			return v.Get(ts)
		}))
	})
}

func TestModifyingPermissions(t *testing.T) {
	withSnek(t, func(s *testSnek) {
		adminCaller := testCaller{isAdmin: true}
		s.must(Register(s.Snek, &testStruct{}, func(view *View, query *Query) error {
			if !view.Caller().IsAdmin() {
				query.Set = And{query.Set, Cond{"String", EQ, "approved"}}
			}
			return nil
		}, func(update *Update, prev, next *testStruct) error {
			if !update.Caller().IsAdmin() {
				next.String = "unapproved"
			}
			return nil
		}))
		ts := &testStruct{ID: s.NewID(), Int: 7, String: "whatever"}
		s.must(s.Update(AnonCaller{}, func(u *Update) error {
			return u.Insert(ts)
		}))
		found := []testStruct{}
		s.must(s.View(AnonCaller{}, func(v *View) error {
			return v.Select(&found, &Query{})
		}))
		if len(found) != 0 {
			t.Errorf("got %+v, wanted no matches", found)
		}
		s.must(s.View(adminCaller, func(v *View) error {
			return v.Select(&found, &Query{})
		}))
		if len(found) != 1 || !found[0].ID.Equal(ts.ID) || found[0].String != "unapproved" {
			t.Errorf("got %+v, wanted %+v", found, []testStruct{*ts})
		}
		ts.String = "approved"
		s.must(s.Update(adminCaller, func(u *Update) error {
			return u.Update(ts)
		}))
		s.must(s.View(AnonCaller{}, func(v *View) error {
			return v.Select(&found, &Query{})
		}))
		if len(found) != 1 || !found[0].ID.Equal(ts.ID) {
			t.Errorf("got %+v, wanted %+v", found, []testStruct{*ts})
		}
	})
}

func TestSubscriptionHash(t *testing.T) {
	withSnek(t, func(s *testSnek) {
		s.must(Register(s.Snek, &testStruct{}, UncontrolledQueries, UncontrolledUpdates(&testStruct{})))
		ts1 := &testStruct{ID: s.NewID(), Int: 1}
		ts2 := &testStruct{ID: s.NewID(), Int: 2}
		ts3 := &testStruct{ID: s.NewID(), Int: 3}
		s.must(s.Update(AnonCaller{}, func(u *Update) error {
			if err := u.Insert(ts1); err != nil {
				return err
			}
			if err := u.Insert(ts2); err != nil {
				return err
			}
			return u.Insert(ts3)
		}))
		inc := make(chan []testStruct)
		s.mustAny(Subscribe(s.Snek, AnonCaller{}, &Query{Limit: 1, Order: []Order{{Field: "Int"}}}, TypedSubscriber(func(res []testStruct, err error) error {
			if err != nil {
				t.Fatal(err)
			}
			inc <- res
			return nil
		})))
		if got := <-inc; len(got) != 1 || !got[0].ID.Equal(ts1.ID) {
			t.Errorf("got %+v, wanted %+v", got, []testStruct{*ts1})
		}
		ts2.String = "string"
		s.must(s.Update(AnonCaller{}, func(u *Update) error {
			return u.Update(ts2)
		}))
		mustUnavail(t, inc)
	})
}

func TestJoin(t *testing.T) {
	withSnek(t, func(s *testSnek) {
		s.must(Register(s.Snek, &testStruct{}, UncontrolledQueries, UncontrolledUpdates(&testStruct{})))
		ts1 := &testStruct{ID: s.NewID(), Int: 7, String: "whatever"}
		ts2 := &testStruct{ID: s.NewID(), Int: 9, String: "whatever"}
		ts3 := &testStruct{ID: s.NewID(), Int: 11, String: "something else"}
		s.must(s.Update(AnonCaller{}, func(u *Update) error {
			if err := u.Insert(ts1); err != nil {
				return err
			}
			if err := u.Insert(ts2); err != nil {
				return err
			}
			return u.Insert(ts3)
		}))
		got := []testStruct{}
		s.must(s.View(AnonCaller{}, func(v *View) error {
			return v.Select(&got, &Query{Set: Cond{"Int", LT, 9}, Joins: []Join{NewJoin(&testStruct{}, Cond{"Int", EQ, 9}, []On{{"String", EQ, "String"}})}})
		}))
		if len(got) != 1 || !got[0].ID.Equal(ts1.ID) {
			t.Errorf("got %+v, wanted %+v", got, []testStruct{*ts1})
		}
		s.must(s.View(AnonCaller{}, func(v *View) error {
			return v.Select(&got, &Query{Set: Cond{"Int", LT, 9}, Joins: []Join{NewJoin(&testStruct{}, Cond{"Int", EQ, 11}, []On{{"String", EQ, "String"}})}})
		}))
		if len(got) != 0 {
			t.Errorf("got %+v, wanted no results", got)
		}
		s.must(s.View(AnonCaller{}, func(v *View) error {
			return v.Select(&got, &Query{Order: []Order{{Field: "Int"}}, Distinct: true, Joins: []Join{NewJoin(&testStruct{}, All{}, []On{{"String", EQ, "String"}, {"ID", NE, "ID"}})}})
		}))
		if len(got) != 2 || !got[0].ID.Equal(ts1.ID) || !got[1].ID.Equal(ts2.ID) {
			t.Errorf("got %+v, wanted %+v", got, []testStruct{*ts1, *ts2})
		}
	})
}
