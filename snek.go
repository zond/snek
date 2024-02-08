package snek

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/dgraph-io/badger"
)

type Snek struct {
	badger  *badger.DB
	options Options
}

func (s *Snek) encode(a any) ([]byte, error) {
	buf := &bytes.Buffer{}
	if err := binary.Write(buf, s.options.Endianness, a); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (s *Snek) decode(b []byte, a any) error {
	val := reflect.ValueOf(a)
	if err := binary.Read(bytes.NewBuffer(b), s.options.Endianness, val.Interface()); err != nil {
		return err
	}
	return nil
}

type Options struct {
	badger.Options
	Endianness binary.ByteOrder
}

func DefaultOptions(path string) Options {
	return Options{
		Options:    badger.DefaultOptions(path),
		Endianness: binary.NativeEndian,
	}
}

func (o Options) Open() (*Snek, error) {
	b, err := badger.Open(o.Options)
	if err != nil {
		return nil, err
	}
	return &Snek{
		badger:  b,
		options: o,
	}, nil
}

func (s *Snek) Close() error {
	return s.badger.Close()
}

type View struct {
	*badger.Txn
	snek *Snek
}

type ID [64]byte

var (
	itemPrefix     = []byte("items/")
	indexPrefix    = []byte("index/")
	indexSeparator = []byte{0}
	byteArrayType  = reflect.TypeOf([]byte{})
)

func join(bs ...[]byte) []byte {
	result := []byte{}
	for _, b := range bs {
		result = append(result, b...)
	}
	return result
}

type fieldInfo struct {
	getValue func() ([]byte, error)
	setValue func([]byte) error
}

type ifInfo struct {
	snek        *Snek
	typePrefix  []byte
	id          []byte
	structType  reflect.Type
	structValue reflect.Value
}

func (i *ifInfo) fields() map[string]fieldInfo {
	fields := map[string]fieldInfo{}
	var appendFieldsFunc func(string, reflect.Type)
	appendFieldsFunc = func(prefix string, typ reflect.Type) {
		for _, iterField := range reflect.VisibleFields(typ) {
			field := iterField
			switch field.Type.Kind() {
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
				fields[prefix+field.Name] = fieldInfo{
					getValue: func() ([]byte, error) {
						v := i.structValue.FieldByIndex(field.Index).Interface()
						return i.snek.encode(v)
					},
					setValue: func(b []byte) error {
						v := reflect.New(field.Type)
						if err := i.snek.decode(b, v.Interface()); err != nil {
							return err
						}
						i.structValue.FieldByIndex(field.Index).Set(v.Elem())
						return nil
					},
				}
			case reflect.Struct:
				appendFieldsFunc(field.Name+".", field.Type)
			default:

			}
		}
	}
	appendFieldsFunc("", i.structType)
	return fields
}

func (s *Snek) getIFInfo(a any) *ifInfo {
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
		panic(fmt.Errorf("only structs with a []ID allowed, not %v", a))
	}
	typ := val.Type()
	return &ifInfo{
		snek:        s,
		typePrefix:  []byte(fmt.Sprintf("%s.%s/", typ.PkgPath(), typ.Name())),
		id:          idField.Interface().([]byte),
		structType:  typ,
		structValue: val,
	}
}

type Update struct {
	View
}

func (s *Snek) View(f func(*View) error) error {
	txn := s.badger.NewTransaction(false)
	defer txn.Discard()
	return f(&View{
		Txn:  txn,
		snek: s,
	})
}

func (v *View) Get(a any) error {
	info := v.snek.getIFInfo(a)
	data, err := v.Txn.Get(join(itemPrefix, info.typePrefix, info.id))
	if err != nil {
		return err
	}
	return data.Value(func(b []byte) error {
		return json.Unmarshal(b, a)
	})
}

func (s *Snek) Update(f func(*Update) error) error {
	txn := s.badger.NewTransaction(true)
	if err := f(&Update{
		View: View{
			Txn:  txn,
			snek: s,
		},
	}); err != nil {
		txn.Discard()
		return err
	}
	return txn.Commit()
}

var (
	ErrKeyAlreadyExists = fmt.Errorf("key already exists")
)

func (u *Update) addToIndex(info *ifInfo) error {
	for name, fieldInfo := range info.fields() {
		b, err := fieldInfo.getValue()
		if err != nil {
			return err
		}
		if err := u.Txn.Set(join(indexPrefix, info.typePrefix, []byte(name), indexSeparator, b, info.id), info.id); err != nil {
			return err
		}
	}
	return nil
}

func (u *Update) removeFromIndex(info *ifInfo) error {
	for name, fieldInfo := range info.fields() {
		b, err := fieldInfo.getValue()
		if err != nil {
			return err
		}
		if err := u.Txn.Delete(join(indexPrefix, info.typePrefix, []byte(name), indexSeparator, b, info.id)); err != nil {
			return err
		}
	}
	return nil
}

func (u *Update) Update(a any) error {
	info := u.snek.getIFInfo(a)
	key := join(itemPrefix, info.typePrefix, info.id)

	data, err := u.Txn.Get(key)
	if err != nil {
		return err
	}
	dupe := reflect.New(info.structType).Interface()
	if err := data.Value(func(b []byte) error {
		return json.Unmarshal(b, dupe)
	}); err != nil {
		return err
	}

	b, err := json.Marshal(a)
	if err != nil {
		return err
	}
	if err := u.Txn.Set(key, b); err != nil {
		return err
	}

	return u.addToIndex(info)
}

func (u *Update) Insert(a any) error {
	info := u.snek.getIFInfo(a)
	key := join(itemPrefix, info.typePrefix, info.id)
	if _, err := u.Txn.Get(key); err != badger.ErrKeyNotFound {
		return ErrKeyAlreadyExists
	}
	b, err := json.Marshal(a)
	if err != nil {
		return err
	}
	if err := u.Txn.Set(key, b); err != nil {
		return err
	}
	return u.addToIndex(info)
}
