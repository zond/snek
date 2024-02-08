package snek

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/nutsdb/nutsdb"
)

type Snek struct {
	db      *nutsdb.DB
	options Options
}

type encoderFunc func(any) ([]byte, error)

func (s *Snek) binaryEncode(a any) ([]byte, error) {
	buf := &bytes.Buffer{}
	if err := binary.Write(buf, s.options.Endianness, a); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (s *Snek) stringEncode(a any) ([]byte, error) {
	return []byte(a.(string)), nil
}

func (s *Snek) primitiveTypeInfo(a any) (bool, encoderFunc) {
	switch reflect.TypeOf(a).Kind() {
	case reflect.Bool:
		fallthrough
	case reflect.Int8:
		fallthrough
	case reflect.Int16:
		fallthrough
	case reflect.Int32:
		fallthrough
	case reflect.Int64:
		fallthrough
	case reflect.Uint8:
		fallthrough
	case reflect.Uint16:
		fallthrough
	case reflect.Uint32:
		fallthrough
	case reflect.Uint64:
		fallthrough
	case reflect.Float32:
		fallthrough
	case reflect.Float64:
		fallthrough
	case reflect.Complex64:
		fallthrough
	case reflect.Complex128:
		return true, s.binaryEncode
	case reflect.String:
		return true, s.stringEncode
	}
	return false, nil
}

type Options struct {
	nutsdb.Options
	Endianness binary.ByteOrder
}

func DefaultOptions(path string) Options {
	opts := nutsdb.DefaultOptions
	opts.Dir = path
	return Options{
		Options:    opts,
		Endianness: binary.NativeEndian,
	}
}

func (o Options) Open() (*Snek, error) {
	db, err := nutsdb.Open(o.Options)
	if err != nil {
		return nil, err
	}
	if err := db.Update(func(tx *nutsdb.Tx) error {
		if err := tx.NewKVBucket(itemBucket); err != nil && err != nutsdb.ErrBucketAlreadyExist {
			return err
		}
		if err := tx.NewSetBucket(indexBucket); err != nil && err != nutsdb.ErrBucketAlreadyExist {
			return err
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return &Snek{
		db:      db,
		options: o,
	}, nil
}

func (s *Snek) Close() error {
	return s.db.Close()
}

type View struct {
	tx   *nutsdb.Tx
	snek *Snek
}

var (
	itemBucket    = "items"
	indexBucket   = "index"
	byteArrayType = reflect.TypeOf([]byte{})
)

type typeInfo struct {
	snek        *Snek
	id          []byte
	structType  reflect.Type
	structValue reflect.Value
}

func join(parts ...[]byte) []byte {
	result := []byte{}
	for _, part := range parts {
		result = append(result, part...)
	}
	return result
}

func (i *typeInfo) itemKey() []byte {
	return join([]byte(fmt.Sprintf("%s.%s", i.structType.PkgPath(), i.structType.Name())), i.id)
}

func (i *typeInfo) indexPrefix(fieldName string) []byte {
	return join([]byte(fmt.Sprintf("%s.%s", i.structType.PkgPath(), i.structType.Name())), []byte(fieldName), []byte{0})
}

func (s *Snek) getTypeInfo(a any) *typeInfo {
	ptrVal := reflect.ValueOf(a)
	if ptrVal.Kind() != reflect.Ptr {
		panic(fmt.Errorf("only pointers to structs allowed, not %v", a))
	}
	val := ptrVal.Elem()
	if val.Kind() != reflect.Struct {
		panic(fmt.Errorf("only pointers to structs allowed, not %v", a))
	}
	idField := val.FieldByName("ID")
	if idField.Type() != byteArrayType {
		panic(fmt.Errorf("only structs with a ID of type []byte allowed, not %v", a))
	}
	return &typeInfo{
		snek:        s,
		id:          idField.Interface().([]byte),
		structType:  val.Type(),
		structValue: val,
	}
}

func (i *typeInfo) fields() (map[string][]byte, error) {
	fields := map[string][]byte{}
	var appendFieldsFunc func(string, reflect.Type) error
	appendFieldsFunc = func(prefix string, typ reflect.Type) error {
		for _, field := range reflect.VisibleFields(typ) {
			value := i.structValue.FieldByIndex(field.Index).Interface()
			if isPrimitive, encoder := i.snek.primitiveTypeInfo(value); isPrimitive {
				b, err := encoder(value)
				if err != nil {
					return err
				}
				fields[field.Name] = b
			} else if field.Type.Kind() == reflect.Struct {
				if err := appendFieldsFunc(field.Name+".", field.Type); err != nil {
					return err
				}
			}

		}
		return nil
	}
	if err := appendFieldsFunc("", i.structType); err != nil {
		return nil, err
	}
	return fields, nil
}

type Update struct {
	View
}

func (s *Snek) View(f func(*View) error) error {
	return s.db.View(func(tx *nutsdb.Tx) error {
		view := &View{
			tx:   tx,
			snek: s,
		}
		return f(view)
	})
}

func (v *View) Get(a any) error {
	info := v.snek.getTypeInfo(a)
	data, err := v.tx.Get(itemBucket, info.itemKey())
	if err != nil {
		return err
	}
	return json.Unmarshal(data, a)
}

func (s *Snek) Update(f func(*Update) error) error {
	return s.db.Update(func(tx *nutsdb.Tx) error {
		update := &Update{
			View: View{
				tx:   tx,
				snek: s,
			},
		}
		return f(update)
	})
}

var (
	ErrKeyAlreadyExists = fmt.Errorf("key already exists")
)

func (u *Update) addToIndex(info *typeInfo) error {
	fields, err := info.fields()
	if err != nil {
		return err
	}
	for name, bytes := range fields {
		if err := u.tx.SAdd(indexBucket, join(info.indexPrefix(name), bytes), info.id); err != nil {
			return err
		}
	}
	return nil
}

func (u *Update) removeFromIndex(info *typeInfo) error {
	fields, err := info.fields()
	if err != nil {
		return err
	}
	for name, bytes := range fields {
		if err := u.tx.SRem(indexBucket, join(info.indexPrefix(name), bytes), info.id); err != nil {
			return err
		}
	}
	return nil
}

func (u *Update) Update(a any) error {
	info := u.snek.getTypeInfo(a)
	key := info.itemKey()
	oldData, err := u.tx.Get(itemBucket, key)
	if err != nil {
		return err
	}
	oldValue := reflect.New(info.structType).Interface()
	if err := json.Unmarshal(oldData, oldValue); err != nil {
		return err
	}
	if err := u.removeFromIndex(u.snek.getTypeInfo(oldValue)); err != nil {
		return err
	}
	b, err := json.Marshal(a)
	if err != nil {
		return err
	}
	if err := u.tx.Put(itemBucket, key, b, 0); err != nil {
		return err
	}
	return u.addToIndex(info)
}

func (u *Update) Insert(a any) error {
	info := u.snek.getTypeInfo(a)
	key := info.itemKey()

	if _, err := u.tx.Get(itemBucket, key); err != nutsdb.ErrKeyNotFound {
		return ErrKeyAlreadyExists
	}
	b, err := json.Marshal(a)
	if err != nil {
		return err
	}
	if err := u.tx.Put(itemBucket, key, b, 0); err != nil {
		return err
	}

	return u.addToIndex(info)
}
