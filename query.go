package snek

import (
	"fmt"
	"reflect"
	"strings"
)

type Set interface {
	toWhereCondition() (string, []any)
	matches(reflect.Value) (bool, error)
	// Returns true if this set contains the value referred to by structPointer.
	Matches(structPointer any) (bool, error)
	// Returns true if there is no intersection between this set and otherSet.
	Excludes(otherSet Set) (bool, error)
	// Returns true if otherSet is a subset of this set.
	Includes(otherSet Set) (bool, error)
}

type All struct{}

func (a All) toWhereCondition() (string, []any) {
	return "(1 = 1)", nil
}

func (a All) matches(reflect.Value) (bool, error) {
	return true, nil
}

func (a All) Excludes(s Set) (bool, error) {
	return false, nil
}

func (a All) Includes(s Set) (bool, error) {
	return true, nil
}

func (a All) Matches(structPointer any) (bool, error) {
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
		return a <= b, nil
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

type comparison func(reflect.Value, reflect.Value) (bool, error)

func noImplication(a, b reflect.Value) (bool, error) {
	return false, nil
}

func incInt(aDelta, bDelta uint, f comparison) comparison {
	return func(a, b reflect.Value) (bool, error) {
		if a.CanInt() && b.CanInt() {
			aFix := reflect.ValueOf(a.Int() + int64(aDelta))
			bFix := reflect.ValueOf(b.Int() + int64(bDelta))
			return f(aFix, bFix)
		} else if a.CanUint() && b.CanUint() {
			aFix := reflect.ValueOf(a.Uint() + uint64(aDelta))
			bFix := reflect.ValueOf(b.Uint() + uint64(bDelta))
			return f(aFix, bFix)
		} else {
			return f(a, b)
		}
	}
}

func implications(a, b Comparator) (isTrue, isFalse comparison, err error) {
	unrecognizedComparator := func(c Comparator) (comparison, comparison, error) {
		return nil, nil, fmt.Errorf("unrecognized comparator %v", int(c))
	}
	switch a {
	case EQ:
		switch b {
		case EQ:
			return EQ.apply, NE.apply, nil
		case NE:
			return NE.apply, EQ.apply, nil
		case GT:
			return GT.apply, LE.apply, nil
		case GE:
			return GE.apply, LT.apply, nil
		case LT:
			return LT.apply, GE.apply, nil
		case LE:
			return LE.apply, GT.apply, nil
		default:
			return unrecognizedComparator(b)
		}
	case NE:
		switch b {
		case EQ:
			return noImplication, EQ.apply, nil
		case NE:
			return EQ.apply, noImplication, nil
		case GT:
			return noImplication, noImplication, nil
		case GE:
			return noImplication, noImplication, nil
		case LT:
			return noImplication, noImplication, nil
		case LE:
			return noImplication, noImplication, nil
		default:
			return unrecognizedComparator(b)
		}
	case GT:
		switch b {
		case EQ:
			return noImplication, GE.apply, nil
		case NE:
			return GE.apply, noImplication, nil
		case GT:
			return GE.apply, noImplication, nil
		case GE:
			return incInt(1, 0, GE.apply), noImplication, nil
		case LT:
			return noImplication, incInt(1, 0, GE.apply), nil
		case LE:
			return noImplication, GE.apply, nil
		default:
			return unrecognizedComparator(b)
		}
	case GE:
		switch b {
		case EQ:
			return noImplication, GT.apply, nil
		case NE:
			return GT.apply, noImplication, nil
		case GT:
			return GT.apply, noImplication, nil
		case GE:
			return GE.apply, noImplication, nil
		case LT:
			return noImplication, GE.apply, nil
		case LE:
			return noImplication, GT.apply, nil
		default:
			return unrecognizedComparator(b)
		}
	case LT:
		switch b {
		case EQ:
			return noImplication, LE.apply, nil
		case NE:
			return LE.apply, noImplication, nil
		case GT:
			return noImplication, incInt(0, 1, LE.apply), nil
		case GE:
			return noImplication, LE.apply, nil
		case LT:
			return LE.apply, noImplication, nil
		case LE:
			return incInt(0, 1, LE.apply), noImplication, nil
		default:
			return unrecognizedComparator(b)
		}
	case LE:
		switch b {
		case EQ:
			return noImplication, LT.apply, nil
		case NE:
			return LT.apply, noImplication, nil
		case GT:
			return noImplication, LE.apply, nil
		case GE:
			return noImplication, LT.apply, nil
		case LT:
			return LT.apply, noImplication, nil
		case LE:
			return LE.apply, noImplication, nil
		default:
			return unrecognizedComparator(b)
		}
	default:
		return unrecognizedComparator(a)
	}
}

type Cond struct {
	Field      string
	Comparator Comparator
	Value      any
}

func (c Cond) Excludes(s Set) (bool, error) {
	switch other := s.(type) {
	case Cond:
		if other.Field == c.Field {
			if _, cImpliesNotOtherFun, err := implications(c.Comparator, other.Comparator); err != nil {
				return false, err
			} else {
				if cImpliesNotOther, err := cImpliesNotOtherFun(reflect.ValueOf(c.Value), reflect.ValueOf(other.Value)); err != nil {
					return false, err
				} else {
					return cImpliesNotOther, nil
				}
			}
		}
		return false, nil

	}
	return s.Excludes(c)
}

func (c Cond) Includes(s Set) (bool, error) {
	switch other := s.(type) {
	case Cond:
		if other.Field == c.Field {
			if cImpliesOtherFun, _, err := implications(c.Comparator, other.Comparator); err != nil {
				return false, err
			} else {
				if cImpliesOther, err := cImpliesOtherFun(reflect.ValueOf(c.Value), reflect.ValueOf(other.Value)); err != nil {
					return false, err
				} else {
					return cImpliesOther, nil
				}
			}
		}
		return false, nil

	}
	return s.Includes(c)
}

func (c Cond) Matches(structPointer any) (bool, error) {
	return c.matches(reflect.ValueOf(structPointer))
}

func (c Cond) matches(val reflect.Value) (bool, error) {
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

func (a And) Excludes(s Set) (bool, error) {
	acc := false
	for _, part := range a {
		exc, err := part.Excludes(s)
		if err != nil {
			return false, err
		}
		acc = acc || exc
		if acc {
			break
		}
	}
	return acc, nil
}

func (a And) Includes(s Set) (bool, error) {
	acc := false
	for _, part := range a {
		inc, err := part.Includes(s)
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

func (a And) Matches(structPointer any) (bool, error) {
	return a.matches(reflect.ValueOf(structPointer))
}

func (a And) matches(val reflect.Value) (bool, error) {
	acc := true
	for _, part := range a {
		inc, err := part.matches(val)
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

func (o Or) Excludes(s Set) (bool, error) {
	acc := true
	for _, part := range o {
		exc, err := part.Excludes(s)
		if err != nil {
			return false, err
		}
		acc = acc && exc
		if !acc {
			break
		}
	}
	return acc, nil
}

func (o Or) Includes(s Set) (bool, error) {
	acc := true
	for _, part := range o {
		inc, err := part.Includes(s)
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

func (o Or) Matches(structPointer any) (bool, error) {
	return o.matches(reflect.ValueOf(structPointer))
}

func (o Or) matches(val reflect.Value) (bool, error) {
	acc := false
	for _, part := range o {
		inc, err := part.matches(val)
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
