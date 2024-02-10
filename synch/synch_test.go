package synch

import (
	"maps"
	"testing"
)

func TestSimpleSync(t *testing.T) {
	s := New(0)
	if got := s.Get(); got != 0 {
		t.Errorf("got %v, want 0", got)
	}
	s.Set(4)
	if got := s.Get(); got != 4 {
		t.Errorf("got %v, want 4", got)
	}
}

type testStruct struct {
	a int
	b int
}

func TestPointerSync(t *testing.T) {
	s := New(&testStruct{a: 10, b: 20})
	if got := s.Get().a; got != 10 {
		t.Errorf("got %v, want 10", got)
	}
	if got := s.Get().b; got != 20 {
		t.Errorf("got %v, want 20", got)
	}
	s.Write(func(t *testStruct) {
		t.b += 2
	})
	if got := s.Get().a; got != 10 {
		t.Errorf("got %v, want 10", got)
	}
	if got := s.Get().b; got != 22 {
		t.Errorf("got %v, want 22", got)
	}
	s.Set(&testStruct{a: 2, b: 3})
	if got := s.Get().a; got != 2 {
		t.Errorf("got %v, want 2", got)
	}
	if got := s.Get().b; got != 3 {
		t.Errorf("got %v, want 3", got)
	}
}

func TestSMap(t *testing.T) {
	m := NewSMap[string, string]()
	if got := m.Len(); got != 0 {
		t.Errorf("got %v, want 0", got)
	}
	if got, found := m.Get("a"); got != "" || found {
		t.Errorf("got %v, %v, want '', false", got, found)
	}
	if got, found := m.Set("a", "x"); got != "" || found {
		t.Errorf("got %v, %v, want '', false", got, found)
	}
	if got, found := m.Set("a", "b"); got != "x" || !found {
		t.Errorf("got %v, %v, want 'x', false", got, found)
	}
	if got := m.Len(); got != 1 {
		t.Errorf("got %v, want 1", got)
	}
	if got, found := m.Get("a"); got != "b" || !found {
		t.Errorf("got %v, %v, want 'b', true", got, found)
	}
	if got, found := m.Get("b"); got != "" || found {
		t.Errorf("got %v, %v, want '', false", got, found)
	}
	if got, found := m.SetIfMissing("a", "c"); got != "b" || !found {
		t.Errorf("got %v, %v, want 'b', true", got, found)
	}
	if got, found := m.SetIfMissing("c", "d"); got != "d" || found {
		t.Errorf("got %v, %v, want 'd', false", got, found)
	}
	want := map[string]string{
		"a": "b",
		"c": "d",
	}
	if !maps.Equal(m.Clone(), want) {
		t.Errorf("got %+v, want %+v", m.Clone(), want)
	}
	m.Each(func(k string, v string) {
		if _, found := want[k]; !found {
			t.Errorf("missing key %v", k)
		}
		delete(want, k)
	})
	if len(want) != 0 {
		t.Errorf("didn't find keys %+v", want)
	}
}
