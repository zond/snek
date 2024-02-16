package snek

import (
	"bytes"
	"fmt"
	"reflect"
	"strings"
)

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

func (i *valueInfo) toDelStatement() (string, []any) {
	return fmt.Sprintf("DELETE FROM \"%s\" WHERE \"ID\" = ?;", i.typ.Name()), []any{i.id}
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

func getValueInfo(val reflect.Value) (*valueInfo, error) {
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
