package snek

import (
	"fmt"
	"reflect"
	"strings"
)

type Set interface {
	toWhereCondition() (string, []any)
	includes(reflect.Value) (bool, error)
}

type All struct{}

func (a All) toWhereCondition() (string, []any) {
	return "(1 = 1)", nil
}

func (a All) includes(reflect.Value) (bool, error) {
	return true, nil
}

type Comparator int

const (
	EQ Comparator = iota
	NE
	GT
	GE
	LT
	LE
)

func comparePrimitives[T ~int | ~int64 | ~uint64 | ~string | ~float64](c Comparator, a, b T) (bool, error) {
	switch c {
	case EQ:
		return a == b, nil
	case NE:
		return a != b, nil
	case GT:
		return a > b, nil
	case GE:
		return a >= b, nil
	case LT:
		return a < b, nil
	case LE:
		return a >= b, nil
	default:
		return false, fmt.Errorf("unrecognized comparator %v", int(c))
	}
}

func (c Comparator) apply(a, b reflect.Value) (bool, error) {
	incomparableB := func() (bool, error) {
		return false, fmt.Errorf("%v %s %v: argument 1 not comparable to %T", a.Interface(), c, b.Interface(), a.Interface())
	}
	if a.Kind() == reflect.String {
		if b.Kind() == reflect.String {
			return comparePrimitives(c, a.String(), b.String())
		} else {
			return incomparableB()
		}
	} else if a.Kind() == reflect.Bool {
		if b.Kind() == reflect.Bool {
			aInt := 0
			if a.Bool() {
				aInt = 1
			}
			bInt := 0
			if b.Bool() {
				bInt = 1
			}
			return comparePrimitives(c, aInt, bInt)
		} else {
			return incomparableB()
		}
	} else if a.CanInt() {
		if b.CanInt() {
			return comparePrimitives(c, a.Int(), b.Int())
		} else if b.CanFloat() {
			return comparePrimitives(c, float64(a.Int()), b.Float())
		} else {
			return incomparableB()
		}
	} else if a.CanFloat() {
		if b.CanFloat() {
			return comparePrimitives(c, a.Float(), b.Float())
		} else if b.CanInt() {
			return comparePrimitives(c, a.Float(), float64(b.Int()))
		} else {
			return incomparableB()
		}
	} else {
		return false, fmt.Errorf("%v %s %v: %T isn't comparable", a.Interface(), c, b.Interface(), a.Interface())
	}
}

func (c Comparator) String() string {
	switch c {
	case EQ:
		return "="
	case NE:
		return "!="
	case GT:
		return ">"
	case GE:
		return ">="
	case LT:
		return "<"
	case LE:
		return "<="
	default:
		return fmt.Sprintf("unrecognized comparator %v", int(c))
	}
}

type Cond struct {
	Field      string
	Comparator Comparator
	Value      any
}

func (c Cond) includes(val reflect.Value) (bool, error) {
	if val.Kind() != reflect.Ptr || val.Elem().Kind() != reflect.Struct {
		return false, fmt.Errorf("only pointers to structs allowed, not %v", val.Interface())
	}
	val = val.Elem()
	return c.Comparator.apply(val.FieldByName(c.Field), reflect.ValueOf(c.Value))
}

func (c Cond) toWhereCondition() (string, []any) {
	return fmt.Sprintf("\"%s\" %s ?", c.Field, c.Comparator.String()), []any{c.Value}
}

type And []Set

func (a And) toWhereCondition() (string, []any) {
	stringParts := []string{}
	valueParts := []any{}
	for _, set := range a {
		query, params := set.toWhereCondition()
		stringParts = append(stringParts, fmt.Sprintf("(%s)", query))
		valueParts = append(valueParts, params...)
	}
	return strings.Join(stringParts, " AND "), valueParts
}

func (a And) includes(val reflect.Value) (bool, error) {
	acc := true
	for _, part := range a {
		inc, err := part.includes(val)
		if err != nil {
			return false, err
		}
		acc = acc && inc
		if !acc {
			break
		}
	}
	return acc, nil
}

type Or []Set

func (o Or) toWhereCondition() (string, []any) {
	stringParts := []string{}
	valueParts := []any{}
	for _, set := range o {
		query, params := set.toWhereCondition()
		stringParts = append(stringParts, fmt.Sprintf("(%s)", query))
		valueParts = append(valueParts, params...)
	}
	return strings.Join(stringParts, " OR "), valueParts
}

func (o Or) includes(val reflect.Value) (bool, error) {
	acc := false
	for _, part := range o {
		inc, err := part.includes(val)
		if err != nil {
			return false, err
		}
		acc = acc || inc
		if acc {
			break
		}
	}
	return acc, nil
}

type Order struct {
	Field string
	Desc  bool
}

type Query struct {
	Set   Set
	Limit uint
	Order []Order
}