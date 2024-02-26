package snek

import (
	"bytes"
	"fmt"
	"reflect"
	"strings"
)

// Set is a definition of instances matching given criteria.
// Since the implementation is a bit simplistic (it doesn't
// compute intersections, and it doesn't normalize criteria
// to some simiplified form, so it can't generally compare
// set equality) it will return some false negatives to the
// Includes and Excludes methods. No false positives should
// be returned however.
type Set interface {
	toWhereCondition(string) (string, []any)
	matches(reflect.Value) (bool, error)
	// Returns true if this set contains the value referred to by structPointer.
	Matches(structPointer any) (bool, error)
	// Returns true if it's guaranteed that there are no intersection between this set and otherSet.
	// This implementation is a bit simplistic, and some false negatives may arise.
	Excludes(otherSet Set) (bool, error)
	// Returns true if it's guaranteed that the otherSet is a subset of this set.
	// This implementaiton is a bit simplistic, and some false negatives may arise.
	Includes(otherSet Set) (bool, error)
	// Returns the complement of this set.
	Invert() (Set, error)
}

// None matches nothing.
type None struct{}

func (n None) toWhereCondition(_ string) (string, []any) {
	return "1 = 0", nil
}

func (n None) matches(reflect.Value) (bool, error) {
	return false, nil
}

func (n None) Excludes(s Set) (bool, error) {
	return true, nil
}

func (n None) Includes(s Set) (bool, error) {
	return false, nil
}

func (n None) Invert() (Set, error) {
	return All{}, nil
}

func (n None) Matches(structPointer any) (bool, error) {
	return false, nil
}

// All matches everything.
type All struct{}

func (a All) toWhereCondition(_ string) (string, []any) {
	return "1 = 1", nil
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

func (a All) Invert() (Set, error) {
	return None{}, nil
}

func (a All) Matches(structPointer any) (bool, error) {
	return true, nil
}

// Comparator compares two values.
type Comparator string

const (
	EQ Comparator = "="
	NE Comparator = "!="
	GT Comparator = ">"
	GE Comparator = ">="
	LT Comparator = "<"
	LE Comparator = "<="
)

func (c Comparator) unrecognizedErr() error {
	return fmt.Errorf("unrecognized comparator %v", c)
}

func compareBytes(c Comparator, a, b []byte) (bool, error) {
	cmp := bytes.Compare(a, b)
	switch c {
	case EQ:
		return cmp == 0, nil
	case NE:
		return cmp != 0, nil
	case GT:
		return cmp > 0, nil
	case GE:
		return cmp >= 0, nil
	case LT:
		return cmp < 0, nil
	case LE:
		return cmp <= 0, nil
	default:
		return false, c.unrecognizedErr()
	}
}

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
		return false, c.unrecognizedErr()
	}
}

func (c Comparator) invert() (Comparator, error) {
	switch c {
	case EQ:
		return NE, nil
	case NE:
		return EQ, nil
	case GT:
		return LE, nil
	case GE:
		return LT, nil
	case LT:
		return GE, nil
	case LE:
		return GT, nil
	default:
		return "", c.unrecognizedErr()
	}
}

var (
	byteSliceType = reflect.TypeOf([]byte{})
)

func (c Comparator) apply(a, b reflect.Value) (bool, error) {
	incomparableB := func() (bool, error) {
		return false, fmt.Errorf("%v %s %v: %T not comparable to %T", a.Interface(), c, b.Interface(), a.Interface(), b.Interface())
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
	} else if a.CanConvert(byteSliceType) {
		if b.CanConvert(byteSliceType) {
			aBytes := a.Convert(byteSliceType).Interface().([]byte)
			bBytes := b.Convert(byteSliceType).Interface().([]byte)
			return compareBytes(c, aBytes, bBytes)
		} else {
			return incomparableB()
		}
	} else {
		return false, fmt.Errorf("%v %s %v: %T isn't comparable", a.Interface(), c, b.Interface(), a.Interface())
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
		return nil, nil, c.unrecognizedErr()
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

// Cond defines a Set of all structs whose Field [Comparator] Value evaluates to true.
type Cond struct {
	Field      string
	Comparator Comparator
	Value      any
}

func (c *Cond) String() string {
	return fmt.Sprintf("%+v", *c)
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
	case All:
		return false, nil
	case None:
		return true, nil
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
	invertedC, err := c.Invert()
	if err != nil {
		return false, err
	}
	return invertedC.Excludes(s)
}

func (c Cond) Invert() (Set, error) {
	invertedComparator, err := c.Comparator.invert()
	if err != nil {
		return nil, err
	}
	return Cond{c.Field, invertedComparator, c.Value}, nil
}

func (c Cond) Matches(structPointer any) (bool, error) {
	return c.matches(reflect.ValueOf(structPointer))
}

func (c Cond) matches(val reflect.Value) (bool, error) {
	if val.Kind() != reflect.Struct {
		return false, fmt.Errorf("only structs allowed, not %v", val.Interface())
	}
	return c.Comparator.apply(val.FieldByName(c.Field), reflect.ValueOf(c.Value))
}

func (c Cond) toWhereCondition(tablePrefix string) (string, []any) {
	return fmt.Sprintf("\"%s\".\"%s\" %s ?", tablePrefix, c.Field, c.Comparator), []any{c.Value}
}

// And defines a Set of all structs present in all contained Sets.
type And []Set

func (a And) toWhereCondition(tablePrefix string) (string, []any) {
	stringParts := []string{}
	valueParts := []any{}
	for _, set := range a {
		sql, params := getWhereCondition(tablePrefix, set, All{})
		stringParts = append(stringParts, fmt.Sprintf("(%s)", sql))
		valueParts = append(valueParts, params...)
	}
	return strings.Join(stringParts, " AND "), valueParts
}

func (a And) Excludes(s Set) (bool, error) {
	for _, part := range a {
		exc, err := part.Excludes(s)
		if err != nil {
			return false, err
		}
		if exc {
			return true, nil
		}
	}
	return false, nil
}

func (a And) Includes(s Set) (bool, error) {
	for _, part := range a {
		inc, err := part.Includes(s)
		if err != nil {
			return false, err
		}
		if !inc {
			return false, nil
		}
	}
	return true, nil
}

func (a And) Invert() (Set, error) {
	result := Or{}
	for _, part := range a {
		invertedPart, err := part.Invert()
		if err != nil {
			return nil, err
		}
		result = append(result, invertedPart)
	}
	return result, nil
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

// Or defines a Set of all structs contained in any contained Set.
type Or []Set

func (o Or) toWhereCondition(tablePrefix string) (string, []any) {
	stringParts := []string{}
	valueParts := []any{}
	for _, set := range o {
		sql, params := getWhereCondition(tablePrefix, set, None{})
		stringParts = append(stringParts, fmt.Sprintf("(%s)", sql))
		valueParts = append(valueParts, params...)
	}
	return strings.Join(stringParts, " OR "), valueParts
}

func (o Or) Excludes(s Set) (bool, error) {
	for _, part := range o {
		exc, err := part.Excludes(s)
		if err != nil {
			return false, err
		}
		if !exc {
			return false, nil
		}
	}
	return true, nil
}

func (o Or) Includes(s Set) (bool, error) {
	for _, part := range o {
		inc, err := part.Includes(s)
		if err != nil {
			return false, err
		}
		if inc {
			return true, nil
		}
	}
	return false, nil
}

func (o Or) Invert() (Set, error) {
	result := And{}
	for _, part := range o {
		invertedPart, err := part.Invert()
		if err != nil {
			return nil, err
		}
		result = append(result, invertedPart)
	}
	return result, nil
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

// Order defines an order for the structs returned by a query.
type Order struct {
	Field string
	Desc  bool
}

// On represents the ON part of a JOIN.
type On struct {
	MainField  string
	Comparator Comparator
	JoinField  string
}

func NewJoin(structPointer any, set Set, on []On) Join {
	typ := reflect.TypeOf(structPointer)
	for typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}
	return Join{typ: typ, set: set, on: on}
}

type Join struct {
	typ reflect.Type
	set Set
	on  []On
}

func (j Join) toOnCondition(mainTypeName, joinTypeName string) string {
	parts := []string{}
	for _, on := range j.on {
		parts = append(parts, fmt.Sprintf("\"%s\".\"%s\" %s \"%s\".\"%s\"", mainTypeName, on.MainField, on.Comparator, joinTypeName, on.JoinField))
	}
	return strings.Join(parts, " AND ")
}

// Query defines a Set of structs to be returned in a particular amount in a particular order.
type Query struct {
	Set      Set
	Limit    uint
	Distinct bool
	Order    []Order
	Joins    []Join
}

func (q *Query) clone() *Query {
	return &Query{
		Set:      q.Set,
		Limit:    q.Limit,
		Distinct: q.Distinct,
		Order:    append([]Order{}, q.Order...),
		Joins:    append([]Join{}, q.Joins...),
	}
}

func getWhereCondition(tablePrefix string, s Set, def Set) (string, []any) {
	if s == nil {
		return def.toWhereCondition(tablePrefix)
	}
	return s.toWhereCondition(tablePrefix)
}

func (q *Query) toSelectStatement(structType reflect.Type) (string, []any) {
	buf := &bytes.Buffer{}
	distinct := ""
	if q.Distinct {
		distinct = "DISTINCT "
	}
	fmt.Fprintf(buf, "SELECT %s\"%s\".* FROM \"%s\"", distinct, structType.Name(), structType.Name())
	if q.Set == nil {
		q.Set = All{}
	}
	mainSQL, params := q.Set.toWhereCondition(structType.Name())
	sqlParts := []string{mainSQL}
	for joinIndex, join := range q.Joins {
		joinName := fmt.Sprintf("j%d", joinIndex)
		fmt.Fprintf(buf, "\nJOIN \"%s\" %s ON %s", join.typ.Name(), joinName, join.toOnCondition(structType.Name(), joinName))
		joinSQL, joinParams := join.set.toWhereCondition(joinName)
		sqlParts = append(sqlParts, joinSQL)
		params = append(params, joinParams...)
	}
	fmt.Fprintf(buf, "\nWHERE %s", strings.Join(sqlParts, " AND "))
	if len(q.Order) > 0 {
		orderParts := []string{}
		for _, order := range q.Order {
			if order.Desc {
				orderParts = append(orderParts, fmt.Sprintf("\"%s\" DESC", order.Field))
			} else {
				orderParts = append(orderParts, fmt.Sprintf("\"%s\" ASC", order.Field))
			}
		}
		fmt.Fprintf(buf, " ORDER BY %s", strings.Join(orderParts, ", "))
	}
	if q.Limit != 0 {
		fmt.Fprintf(buf, " LIMIT %d", q.Limit)
	}
	fmt.Fprint(buf, ";")
	return buf.String(), params
}

// SetIncludes is a convenience for query control functions that checks if the subset is a subset of the given superset.
func SetIncludes(superset, subset Set) error {
	isSubset, err := superset.Includes(subset)
	if err != nil {
		return err
	}
	if !isSubset {
		return fmt.Errorf("disallowed")
	}
	return nil
}

// QueryHasResults is a convenience for query control functions that checks if the query has results.
func QueryHasResults[T any](v *View, s []T, q *Query) error {
	if err := v.Select(&s, q); err != nil {
		return err
	}
	if len(s) == 0 {
		return fmt.Errorf("disallowed")
	}
	return nil
}
