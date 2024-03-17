package snek

import (
	"bytes"
	"fmt"
	"reflect"
	"strings"
)

type valueInfo struct {
	val                  reflect.Value
	typ                  reflect.Type
	id                   ID
	_fieldsWithValues    fieldInfoMap
	_fieldsWithoutValues fieldInfoMap
}

type fieldInfo struct {
	columnType string
	value      any
	indexed    bool
	unique     bool
	primaryKey bool
}

type fieldInfoMap map[string]fieldInfo

// Uniquer are types that have unique combinations of fields.
type Uniquer interface {
	// Unique returns a slice of unique field combinations.
	Unique() [][]string
}

func (i *valueInfo) toCreateStatement() string {
	builder := &bytes.Buffer{}
	fmt.Fprintf(builder, "CREATE TABLE IF NOT EXISTS \"%s\" (\n", i.typ.Name())
	fieldParts := []string{}
	createIndexParts := []string{}
	for fieldName, fieldInfo := range i.fields(false) {
		primaryKey := ""
		if fieldInfo.primaryKey {
			primaryKey = " PRIMARY KEY"
		}
		if fieldInfo.indexed || fieldInfo.unique {
			unique := ""
			if fieldInfo.unique {
				unique = " UNIQUE"
			}
			createIndexParts = append(createIndexParts, fmt.Sprintf("CREATE%s INDEX IF NOT EXISTS \"%s.%s\" ON \"%s\" (\"%s\");", unique, i.typ.Name(), fieldName, i.typ.Name(), fieldName))
		}
		fieldParts = append(fieldParts, fmt.Sprintf("  \"%s\" %s%s", fieldName, fieldInfo.columnType, primaryKey))
	}
	if uniquer, ok := i.val.Interface().(Uniquer); ok {
		for _, combo := range uniquer.(Uniquer).Unique() {
			fieldParts := []string{}
			for _, part := range combo {
				fieldParts = append(fieldParts, fmt.Sprintf("\"%s\"", part))
			}
			createIndexParts = append(createIndexParts, fmt.Sprintf("CREATE UNIQUE INDEX IF NOT EXISTS \"%s.%s\" ON \"%s\" (%s);", i.typ.Name(), strings.Join(combo, "_"), i.typ.Name(), strings.Join(fieldParts, ", ")))
		}
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
	for fieldName, fieldInfo := range i.fields(true) {
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
	for fieldName, fieldInfo := range i.fields(true) {
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

func (f fieldInfoMap) processField(prefix string, field reflect.StructField, typ reflect.Type, fieldVal *reflect.Value) {
	makeFieldInfo := func(columnType string, val *reflect.Value) fieldInfo {
		res := fieldInfo{
			columnType: columnType,
			indexed:    field.Tag.Get("snek") == "index",
			unique:     field.Tag.Get("snek") == "unique",
			primaryKey: prefix == "" && field.Name == "ID",
		}
		if val != nil {
			res.value = (*val).Interface()
		}
		return res
	}
	switch typ.Kind() {
	case reflect.Bool:
		f[prefix+field.Name] = makeFieldInfo("BOOLEAN", fieldVal)
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
		f[prefix+field.Name] = makeFieldInfo("INTEGER", fieldVal)
	case reflect.Float32:
		fallthrough
	case reflect.Float64:
		f[prefix+field.Name] = makeFieldInfo("REAL", fieldVal)
	case reflect.Array:
		if typ.Elem().Kind() == reflect.Uint8 {
			var cpyVal *reflect.Value
			if fieldVal != nil {
				cpy := make([]uint8, (*fieldVal).Len())
				reflect.Copy(reflect.ValueOf(cpy), *fieldVal)
				cpyValMem := reflect.ValueOf(cpy)
				cpyVal = &cpyValMem
			}
			f[prefix+field.Name] = makeFieldInfo("BLOB", cpyVal)
		}
	case reflect.Slice:
		if typ.Elem().Kind() == reflect.Uint8 {
			f[prefix+field.Name] = makeFieldInfo("BLOB", fieldVal)
		}
	case reflect.Pointer:
		var refVal *reflect.Value
		if fieldVal != nil && !fieldVal.IsNil() {
			refValMem := (*fieldVal).Elem()
			refVal = &refValMem
		}
		f.processField(prefix, field, typ.Elem(), refVal)
	case reflect.String:
		f[prefix+field.Name] = makeFieldInfo("TEXT", fieldVal)
	case reflect.Struct:
		f.addFields(prefix+field.Name+".", typ, fieldVal)
	default:
	}
}

func (f fieldInfoMap) addFields(prefix string, typ reflect.Type, val *reflect.Value) {
	for _, field := range reflect.VisibleFields(typ) {
		if !field.IsExported() {
			continue
		}
		var fieldValue *reflect.Value
		if val != nil {
			fieldValMem := (*val).FieldByIndex(field.Index)
			fieldValue = &fieldValMem
		}
		f.processField(prefix, field, field.Type, fieldValue)
	}
}

func (i *valueInfo) fields(values bool) fieldInfoMap {
	if values {
		if len(i._fieldsWithValues) == 0 {
			i._fieldsWithValues = fieldInfoMap{}
			i._fieldsWithValues.addFields("", i.typ, &i.val)
		}
		return i._fieldsWithValues
	} else {
		if len(i._fieldsWithoutValues) == 0 {
			i._fieldsWithoutValues = fieldInfoMap{}
			i._fieldsWithoutValues.addFields("", i.typ, nil)
		}
		return i._fieldsWithoutValues
	}
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
