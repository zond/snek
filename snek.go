package snek

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"math/rand"
	"reflect"
	"strings"
	"time"
	"unsafe"

	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	"github.com/zond/snek/synch"
)

type ID []byte

func (i ID) String() string {
	return hex.EncodeToString(i)
}

var (
	idType = reflect.TypeOf(ID{})
)

type subscription interface {
	push()
	matches(any) bool
	getID() ID
}

type Snek struct {
	ctx           context.Context
	db            *sqlx.DB
	options       Options
	rng           *rand.Rand
	subscriptions *synch.SMap[string, *synch.SMap[string, subscription]]
}

func (s *Snek) getSubscriptions(typ reflect.Type) *synch.SMap[string, subscription] {
	result, _ := s.subscriptions.SetIfMissing(typ.Name(), synch.NewSMap[string, subscription]())
	return result
}

func (s *Snek) AssertTable(a any) error {
	return s.Update(func(u *Update) error {
		info, err := s.getValueInfo(reflect.ValueOf(a))
		if err != nil {
			return err
		}
		return u.exec(info.toCreateStatement())
	})
}

func (s *Snek) NewID() ID {
	result := make(ID, 32)
	*(*[4]uint64)(unsafe.Pointer(&result[0])) = [4]uint64{uint64(time.Now().UnixNano()), s.rng.Uint64(), s.rng.Uint64(), s.rng.Uint64()}
	return result
}

type Options struct {
	Path       string
	RandomSeed int64
	Logger     *log.Logger
	LogExec    bool
	LogQuery   bool
}

func DefaultOptions(path string) Options {
	return Options{
		Path: path,
	}
}

func (o Options) Open() (*Snek, error) {
	db, err := sqlx.Open("sqlite3", o.Path)
	if err != nil {
		return nil, err
	}
	db.MapperFunc(func(s string) string {
		return s
	})
	return &Snek{
		ctx:     context.Background(),
		db:      db,
		options: o,
		rng:     rand.New(rand.NewSource(o.RandomSeed)),
	}, nil
}

type View struct {
	tx   *sqlx.Tx
	snek *Snek
}

type Update struct {
	View
}

func (s *Snek) logIf(condition bool, format string, params ...any) {
	if condition && s.options.Logger != nil {
		s.options.Logger.Printf(format, params...)
	}
}

func (s *Snek) View(f func(*View) error) error {
	tx, err := s.db.BeginTxx(s.ctx, &sql.TxOptions{
		Isolation: sql.LevelSerializable,
		ReadOnly:  true,
	})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	return f(&View{
		tx:   tx,
		snek: s,
	})
}

func (v *View) query(query string, params ...any) (*sqlx.Rows, error) {
	rows, err := v.tx.QueryxContext(v.snek.ctx, query, params...)
	v.snek.logIf(v.snek.options.LogQuery, "QUERY(\"%s\", %+v) => %v", strings.ReplaceAll(query, "\"", "\\\""), params, err)
	return rows, err
}

type Set interface {
	toWhereCondition() (string, []any)
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
		panic(fmt.Errorf("unrecognized comparator %v", int(c)))
	}
}

type Cond struct {
	Field      string
	Comparator Comparator
	Value      any
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

type Order struct {
	Field string
	Desc  bool
}

type Query struct {
	Set   Set
	Limit uint
	Order []Order
}

type Subscriber[T any] func([]T, error) error

type typedSubscription[T any] struct {
	typ        reflect.Type
	id         ID
	query      *Query
	snek       *Snek
	subscriber Subscriber[T]
}

func (s *typedSubscription[T]) getID() ID {
	return s.id
}

func (s *typedSubscription[T]) matches(a any) bool {
	// TODO(zond): Implement.
	return false
}

func (s *typedSubscription[T]) push() {
	results := []T{}
	subscriberErr := s.snek.View(func(v *View) error {
		return v.Select(&results, s.query)
	})
	pushErr := s.subscriber(results, subscriberErr)
	if pushErr != nil {
		subs := s.snek.getSubscriptions(s.typ)
		subs.Del(string(s.id))
	}
}

func Subscribe[T any](s *Snek, query *Query, subscriber Subscriber[T]) {
	sub := &typedSubscription[T]{
		typ:        reflect.TypeOf([]T{}),
		id:         s.NewID(),
		snek:       s,
		query:      query,
		subscriber: subscriber,
	}
	subs := s.getSubscriptions(sub.typ)
	if _, found := subs.Set(string(sub.id), sub); found {
		log.Panicf("found previous subscription with new subscription ID %+v", sub.id)
	}
	go func() {
		sub.push()
	}()
}

func (v *View) Select(a any, query *Query) error {
	typ := reflect.TypeOf(a)
	if typ.Kind() != reflect.Ptr || typ.Elem().Kind() != reflect.Slice || typ.Elem().Elem().Kind() != reflect.Struct {
		return fmt.Errorf("only pointers to slices of structs allowed, not %v", a)
	}
	condition, params := query.Set.toWhereCondition()
	buf := &bytes.Buffer{}
	fmt.Fprintf(buf, "SELECT * FROM \"%s\" WHERE %s", typ.Elem().Elem().Name(), condition)
	if len(query.Order) > 0 {
		orderParts := []string{}
		for _, order := range query.Order {
			if order.Desc {
				orderParts = append(orderParts, fmt.Sprintf("\"%s\" DESC", order.Field))
			} else {
				orderParts = append(orderParts, fmt.Sprintf("\"%s\" ASC", order.Field))
			}
		}
		fmt.Fprintf(buf, " ORDER BY %s", strings.Join(orderParts, ", "))
	}
	if query.Limit != 0 {
		fmt.Fprintf(buf, " LIMIT %d", query.Limit)
	}
	fmt.Fprint(buf, ";")
	queryString := buf.String()
	err := v.tx.SelectContext(v.snek.ctx, a, queryString, params...)
	v.snek.logIf(v.snek.options.LogQuery, "QUERY(\"%s\", %+v) => %v", strings.ReplaceAll(queryString, "\"", "\\\""), params, err)
	return err
}

func (v *View) Get(a any) error {
	info, err := v.snek.getValueInfo(reflect.ValueOf(a))
	if err != nil {
		return err
	}
	query, params := info.toGetStatement()
	err = v.tx.GetContext(v.snek.ctx, a, query, params...)
	v.snek.logIf(v.snek.options.LogQuery, "QUERY(\"%s\", %+v) => %v", strings.ReplaceAll(query, "\"", "\\\""), params, err)
	return err
}

type valueInfo struct {
	val     reflect.Value
	typ     reflect.Type
	id      ID
	_fields fieldInfoMap
}

type fieldInfo struct {
	columnType string
	value      any
	indexed    bool
	primaryKey bool
}

type fieldInfoMap map[string]fieldInfo

func (i *valueInfo) toCreateStatement() string {
	builder := &bytes.Buffer{}
	fmt.Fprintf(builder, "CREATE TABLE IF NOT EXISTS \"%s\" (\n", i.typ.Name())
	fieldParts := []string{}
	createIndexParts := []string{}
	for fieldName, fieldInfo := range i.fields() {
		primaryKey := ""
		if fieldInfo.primaryKey {
			primaryKey = " PRIMARY KEY"
		}
		if fieldInfo.indexed {
			createIndexParts = append(createIndexParts, fmt.Sprintf("CREATE INDEX IF NOT EXISTS \"%s.%s\" ON \"%s\" (\"%s\");", i.typ.Name(), fieldName, i.typ.Name(), fieldName))
		}
		fieldParts = append(fieldParts, fmt.Sprintf("  \"%s\" %s%s", fieldName, fieldInfo.columnType, primaryKey))
	}
	fmt.Fprintf(builder, "%s);", strings.Join(fieldParts, ",\n"))
	if len(createIndexParts) > 0 {
		fmt.Fprintf(builder, "\n%s", strings.Join(createIndexParts, "\n"))
	}
	return builder.String()
}

func (i *valueInfo) toGetStatement() (string, []any) {
	return fmt.Sprintf("SELECT * FROM \"%s\" WHERE \"ID\" = ?;", i.typ.Name()), []any{i.id}
}

func (i *valueInfo) toInsertStatement() (string, []any) {
	builder := &bytes.Buffer{}
	fmt.Fprintf(builder, "INSERT INTO \"%s\"\n  (", i.typ.Name())
	fieldNameParts := []string{}
	fieldQMParts := []string{}
	fieldValueParts := []any{}
	for fieldName, fieldInfo := range i.fields() {
		fieldNameParts = append(fieldNameParts, fmt.Sprintf("\"%s\"", fieldName))
		fieldQMParts = append(fieldQMParts, "?")
		fieldValueParts = append(fieldValueParts, fieldInfo.value)
	}
	fmt.Fprintf(builder, "%s) VALUES\n  (%s);", strings.Join(fieldNameParts, ", "), strings.Join(fieldQMParts, ", "))
	return builder.String(), fieldValueParts
}

func (i *valueInfo) toUpdateStatement() (string, []any) {
	builder := &bytes.Buffer{}
	fmt.Fprintf(builder, "UPDATE \"%s\" SET\n", i.typ.Name())
	fieldNameParts := []string{}
	fieldValueParts := []any{}
	var primaryKey any
	for fieldName, fieldInfo := range i.fields() {
		if fieldInfo.primaryKey {
			primaryKey = fieldInfo.value
		} else {
			fieldNameParts = append(fieldNameParts, fmt.Sprintf("  \"%s\" = ?", fieldName))
			fieldValueParts = append(fieldValueParts, fieldInfo.value)
		}
	}
	fmt.Fprintf(builder, "%s\nWHERE \"ID\" = ?;", strings.Join(fieldNameParts, ",\n"))
	fieldValueParts = append(fieldValueParts, primaryKey)
	return builder.String(), fieldValueParts
}

func (f fieldInfoMap) addFields(prefix string, val reflect.Value) {
	for _, field := range reflect.VisibleFields(val.Type()) {
		fieldVal := val.FieldByIndex(field.Index)
		makeFieldInfo := func(columnType string) fieldInfo {
			return fieldInfo{
				columnType: columnType,
				value:      fieldVal.Interface(),
				indexed:    field.Tag.Get("snek") == "index",
				primaryKey: prefix == "" && field.Name == "ID",
			}

		}
		switch field.Type.Kind() {
		case reflect.Bool:
			f[prefix+field.Name] = makeFieldInfo("BOOLEAN")
		case reflect.Int:
			fallthrough
		case reflect.Int8:
			fallthrough
		case reflect.Int16:
			fallthrough
		case reflect.Int32:
			fallthrough
		case reflect.Int64:
			fallthrough
		case reflect.Uint:
			fallthrough
		case reflect.Uint8:
			fallthrough
		case reflect.Uint16:
			fallthrough
		case reflect.Uint32:
			fallthrough
		case reflect.Uint64:
			f[prefix+field.Name] = makeFieldInfo("INTEGER")
		case reflect.Float32:
			fallthrough
		case reflect.Float64:
			f[prefix+field.Name] = makeFieldInfo("REAL")
		case reflect.Array:
			if field.Type.Elem().Kind() == reflect.Uint8 {
				cpy := make([]uint8, fieldVal.Len())
				reflect.Copy(reflect.ValueOf(cpy), fieldVal)
				f[prefix+field.Name] = fieldInfo{
					columnType: "BLOB",
					value:      cpy,
					indexed:    field.Tag.Get("snek") == "index",
					primaryKey: prefix == "" && field.Name == "ID",
				}
			}
		case reflect.Slice:
			if field.Type.Elem().Kind() == reflect.Uint8 {
				f[prefix+field.Name] = makeFieldInfo("BLOB")
			}
		case reflect.Pointer:
			f.addFields(prefix, fieldVal.Elem())
		case reflect.String:
			f[prefix+field.Name] = makeFieldInfo("TEXT")
		case reflect.Struct:
			f.addFields(prefix+field.Name+".", fieldVal)
		default:
		}
	}
}

func (i *valueInfo) fields() fieldInfoMap {
	if len(i._fields) == 0 {
		i._fields = fieldInfoMap{}
		i._fields.addFields("", i.val)
	}
	return i._fields
}

func (s *Snek) getValueInfo(val reflect.Value) (*valueInfo, error) {
	if val.Kind() != reflect.Ptr || val.Type().Elem().Kind() != reflect.Struct {
		return nil, fmt.Errorf("only pointers to structs allowed, not %v", val.Interface())
	}
	val = val.Elem()
	typ := val.Type()
	if typ.Kind() != reflect.Struct {
		return nil, fmt.Errorf("only struct types allowed, not %v", val.Interface())
	}
	idField, found := typ.FieldByName("ID")
	if !found || idField.Type != idType {
		return nil, fmt.Errorf("only struct types with ID field of type ID allowed, not %v", val.Interface())
	}
	id := val.FieldByIndex(idField.Index).Interface().(ID)
	return &valueInfo{
		val: val,
		typ: val.Type(),
		id:  id,
	}, nil
}

func (s *Snek) Update(f func(*Update) error) error {
	tx, err := s.db.BeginTxx(s.ctx, &sql.TxOptions{
		Isolation: sql.LevelSerializable,
		ReadOnly:  false,
	})
	if err != nil {
		return err
	}
	if err := f(&Update{
		View: View{
			tx:   tx,
			snek: s,
		},
	}); err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			log.Fatal(rollbackErr)
		}
		return err
	}
	return tx.Commit()
}

func (u *Update) Update(a any) error {
	info, err := u.snek.getValueInfo(reflect.ValueOf(a))
	if err != nil {
		return err
	}
	query, params := info.toUpdateStatement()
	return u.exec(query, params...)
}

func (u *Update) Insert(a any) error {
	info, err := u.snek.getValueInfo(reflect.ValueOf(a))
	if err != nil {
		return err
	}
	query, params := info.toInsertStatement()
	return u.exec(query, params...)
}

func (u *Update) exec(query string, params ...any) error {
	_, err := u.tx.ExecContext(u.snek.ctx, query, params...)
	u.snek.logIf(u.snek.options.LogExec, "EXEC(\"%s\", %+v) => %v", strings.ReplaceAll(query, "\"", "\\\""), params, err)
	return err
}
