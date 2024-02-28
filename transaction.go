package snek

import (
	"database/sql"
	"fmt"
	"log"
	"reflect"
	"strings"

	"github.com/jmoiron/sqlx"
)

// View represents a read-only transaction.
type View struct {
	tx        *sqlx.Tx
	snek      *Snek
	caller    Caller
	isControl bool
}

// Caller returns the caller of this view.
func (v *View) Caller() Caller {
	return v.caller
}

func (v *View) queryControl(typ reflect.Type, query *Query) error {
	if v.caller.IsSystem() || v.isControl {
		return nil
	}
	perms, found := v.snek.permissions[typ.Name()]
	if !found || perms.queryControl == nil {
		return fmt.Errorf("%s not registered with query control", typ.Name())
	}
	v.isControl = true
	defer func() { v.isControl = false }()
	return perms.queryControl(v, query)
}

// Update represents a read/write transaction.
type Update struct {
	*View
	subscriptions subscriptionSet
}

func (u *Update) updateControl(typ reflect.Type, prev, next any) error {
	if u.View.caller.IsSystem() || u.View.isControl {
		return nil
	}
	perms, found := u.snek.permissions[typ.Name()]
	if !found || perms.updateControl == nil {
		return fmt.Errorf("%s not registered with update control", typ.Name())
	}
	u.View.isControl = true
	defer func() { u.View.isControl = false }()
	return perms.updateControl(u, prev, next)
}

// Caller identifies the caller of a function.
type Caller interface {
	UserID() ID
	IsAdmin() bool
	IsSystem() bool
}

// View executs f in the context of a read-only transaction.
func (s *Snek) View(caller Caller, f func(*View) error) error {
	tx, err := s.db.BeginTxx(s.ctx, &sql.TxOptions{
		Isolation: sql.LevelSerializable,
		ReadOnly:  true,
	})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	return f(&View{
		tx:     tx,
		snek:   s,
		caller: caller,
	})
}

func (v *View) logSQL(query string, params []any, structSlicePointer any, err error) {
	if !v.snek.options.LogSQL {
		return
	}
	indentedQuery := strings.Join(strings.Split(query, "\n"), "\n  ")
	paramString := ""
	if len(params) > 0 {
		paramParts := []string{}
		for _, param := range params {
			switch v := param.(type) {
			case string:
				paramParts = append(paramParts, fmt.Sprintf("%q", v))
			case ID:
				paramParts = append(paramParts, fmt.Sprintf("%v", []byte(v)))
			default:
				paramParts = append(paramParts, fmt.Sprintf("%+v", v))
			}
		}
		paramString = fmt.Sprintf("\nParameters: %s", strings.Join(paramParts, ", "))
	}
	res := ""
	if structSlicePointer != nil {
		res = fmt.Sprintf("(%d results), ", reflect.ValueOf(structSlicePointer).Elem().Len())
	}
	acl := ""
	if v.isControl {
		acl = "[ACL] "
	}
	v.snek.logIf(v.snek.options.LogSQL, "%sSQL => %s%v\n  %s%s", acl, res, err, indentedQuery, paramString)
}

// Select executs the query and puts the results in structSlicePointer.
func (v *View) Select(structSlicePointer any, query *Query) error {
	if query == nil {
		query = &Query{}
	}
	typ := reflect.TypeOf(structSlicePointer)
	if typ.Kind() != reflect.Ptr || typ.Elem().Kind() != reflect.Slice || typ.Elem().Elem().Kind() != reflect.Struct {
		return fmt.Errorf("only pointers to slices of structs allowed, not %v", typ)
	}
	structType := typ.Elem().Elem()
	queryCopy := query.clone()
	if err := v.queryControl(structType, queryCopy); err != nil {
		return err
	}
	sql, params := queryCopy.toSelectStatement(structType)
	err := v.tx.SelectContext(v.snek.ctx, structSlicePointer, sql, params...)
	v.logSQL(sql, params, structSlicePointer, err)
	return err
}

func (v *View) get(structPointer any, info *valueInfo) error {
	sql, params := info.toGetStatement()
	err := v.tx.GetContext(v.snek.ctx, structPointer, sql, params...)
	v.logSQL(sql, params, nil, err)
	return err
}

// Get populates structPointer with the data at structPointer.ID in the store.
func (v *View) Get(structPointer any) error {
	info, err := getValueInfo(reflect.ValueOf(structPointer))
	if err != nil {
		return err
	}
	query := &Query{Set: &Cond{"ID", EQ, info.id}}
	if err := v.queryControl(info.typ, query); err != nil {
		return err
	}
	sql, params := query.toSelectStatement(info.typ)
	err = v.tx.GetContext(v.snek.ctx, structPointer, sql, params...)
	v.logSQL(sql, params, nil, err)
	return err
}

// Update executs f in the context of a read/write transaction.
func (s *Snek) Update(caller Caller, f func(*Update) error) error {
	tx, err := s.db.BeginTxx(s.ctx, &sql.TxOptions{
		Isolation: sql.LevelSerializable,
		ReadOnly:  false,
	})
	if err != nil {
		return err
	}
	subscriptions := subscriptionSet{}
	if err := f(&Update{
		View: &View{
			tx:     tx,
			snek:   s,
			caller: caller,
		},
		subscriptions: subscriptions,
	}); err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			log.Fatal(rollbackErr)
		}
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	subscriptions.push()
	return nil
}

func (u *Update) loadAndAddSubscriptionsForCurrent(info *valueInfo) (any, error) {
	existingVal := reflect.New(info.typ)
	if err := u.get(existingVal.Interface(), info); err != nil {
		return nil, err
	}
	u.subscriptions.merge(u.snek.getSubscriptionsFor(existingVal.Elem()))
	return existingVal.Interface(), nil
}

// Remove removes the data at structPointer.ID.
func (u *Update) Remove(structPointer any) error {
	info, err := getValueInfo(reflect.ValueOf(structPointer))
	if err != nil {
		return err
	}

	current, err := u.loadAndAddSubscriptionsForCurrent(info)
	if err != nil {
		return err
	}

	if err := u.updateControl(info.typ, current, nil); err != nil {
		return err
	}

	sql, params := info.toDelStatement()
	if err := u.exec(sql, params...); err != nil {
		return err
	}
	return nil
}

// Update replaces the data at structPointer.ID with the data inside structPointer.
func (u *Update) Update(structPointer any) error {
	info, err := getValueInfo(reflect.ValueOf(structPointer))
	if err != nil {
		return err
	}

	current, err := u.loadAndAddSubscriptionsForCurrent(info)
	if err != nil {
		return err
	}

	if err := u.updateControl(info.typ, current, structPointer); err != nil {
		return err
	}

	sql, params := info.toUpdateStatement()
	if err := u.exec(sql, params...); err != nil {
		return err
	}
	u.subscriptions.merge(u.snek.getSubscriptionsFor(info.val))
	return nil
}

// Insert places the data inside structPointer at structPointer.ID.
func (u *Update) Insert(structPointer any) error {
	info, err := getValueInfo(reflect.ValueOf(structPointer))
	if err != nil {
		return err
	}

	if err := u.updateControl(info.typ, nil, structPointer); err != nil {
		return err
	}

	sql, params := info.toInsertStatement()
	if err := u.exec(sql, params...); err != nil {
		return err
	}
	u.subscriptions.merge(u.snek.getSubscriptionsFor(info.val))
	return nil
}

func (u *Update) exec(sql string, params ...any) error {
	_, err := u.tx.ExecContext(u.snek.ctx, sql, params...)
	u.View.logSQL(sql, params, nil, err)
	return err
}
