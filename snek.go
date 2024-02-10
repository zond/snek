package snek

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"reflect"
	"strings"
	"time"
	"unsafe"

	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
)

type ID []byte

var (
	idType = reflect.TypeOf(ID{})
)

type Snek struct {
	ctx     context.Context
	db      *sqlx.DB
	options Options
	rng     *rand.Rand
}

func (s *Snek) AssertTable(a any) error {
	return s.Update(func(u *Update) error {
		return u.exec(s.getValueInfo(a).toCreateStatement())
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

func (v *View) Get(a any) error {
	info := v.snek.getValueInfo(a)
	query, params := info.toGetStatement()
	err := v.tx.GetContext(v.snek.ctx, a, query, params...)
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

func (s *Snek) getValueInfo(a any) *valueInfo {
	val := reflect.ValueOf(a)
	for val.Type().Kind() == reflect.Ptr {
		val = val.Elem()
	}
	typ := val.Type()
	if typ.Kind() != reflect.Struct {
		panic(fmt.Errorf("only struct types allowed, not %v", a))
	}
	idField, found := typ.FieldByName("ID")
	if !found || idField.Type != idType {
		panic(fmt.Errorf("only struct types with ID of type ID allowed, not %v", a))
	}
	id := val.FieldByIndex(idField.Index)
	return &valueInfo{
		val: val,
		typ: val.Type(),
		id:  id.Interface().(ID),
	}
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
	query, params := u.snek.getValueInfo(a).toUpdateStatement()
	return u.exec(query, params...)
}

func (u *Update) Insert(a any) error {
	query, params := u.snek.getValueInfo(a).toInsertStatement()
	return u.exec(query, params...)
}

func (u *Update) exec(query string, params ...any) error {
	_, err := u.tx.ExecContext(u.snek.ctx, query, params...)
	u.snek.logIf(u.snek.options.LogExec, "EXEC(\"%s\", %+v) => %v", strings.ReplaceAll(query, "\"", "\\\""), params, err)
	return err
}
