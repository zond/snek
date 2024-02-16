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
	tx     *sqlx.Tx
	snek   *Snek
	caller Caller
}

// Caller returns the caller of this view.
func (v *View) Caller() Caller {
	return v.caller
}

func (v *View) queryControl(typ reflect.Type, query *Query) error {
	perms, found := v.snek.permissions[typ.Name()]
	if !found || perms.queryControl == nil {
		return fmt.Errorf("%s not registered with query control", typ.Name())
	}
	return perms.queryControl(v, query)
}

// Update represents a read/write transaction.
type Update struct {
	*View
	subscriptions subscriptionSet
}

func (u *Update) updateControl(typ reflect.Type, prev, next any) error {
	perms, found := u.snek.permissions[typ.Name()]
	if !found || perms.updateControl == nil {
		return fmt.Errorf("%s not registered with update control", typ.Name())
	}
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

func logSQL(s *Snek, query string, params []any, err error) {
	if !s.options.LogSQL {
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
			default:
				paramParts = append(paramParts, fmt.Sprintf("%+v", v))
			}
		}
		paramString = fmt.Sprintf("\nParameters: %s", strings.Join(paramParts, ", "))
	}
	s.logIf(s.options.LogSQL, "SQL => %v\n  %s%s", err, indentedQuery, paramString)
}

func (v *View) query(query string, params ...any) (*sqlx.Rows, error) {
	rows, err := v.tx.QueryxContext(v.snek.ctx, query, params...)
	logSQL(v.snek, query, params, err)
	return rows, err
}

// Select executs the query and puts the results in structSlicePointer.
func (v *View) Select(structSlicePointer any, query Query) error {
	typ := reflect.TypeOf(structSlicePointer)
	if typ.Kind() != reflect.Ptr || typ.Elem().Kind() != reflect.Slice || typ.Elem().Elem().Kind() != reflect.Struct {
		return fmt.Errorf("only pointers to slices of structs allowed, not %v", structSlicePointer)
	}
	structType := typ.Elem().Elem()
	if err := v.queryControl(structType, &query); err != nil {
		return err
	}
	sql, params := query.toSelectStatement(structType)
	err := v.tx.SelectContext(v.snek.ctx, structSlicePointer, sql, params...)
	logSQL(v.snek, sql, params, err)
	return err
}

func (v *View) get(structPointer any, info *valueInfo) error {
	sql, params := info.toGetStatement()
	err := v.tx.GetContext(v.snek.ctx, structPointer, sql, params...)
	logSQL(v.snek, sql, params, err)
	return err
}

// Get populates structPointer with the data at structPointer.ID in the store.
func (v *View) Get(structPointer any) error {
	info, err := getValueInfo(reflect.ValueOf(structPointer))
	if err != nil {
		return err
	}
	query := &Query{Set: Cond{"ID", EQ, info.id}}
	if err := v.queryControl(info.typ, query); err != nil {
		return err
	}
	sql, params := query.toSelectStatement(info.typ)
	err = v.tx.GetContext(v.snek.ctx, structPointer, sql, params...)
	logSQL(v.snek, sql, params, err)
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
	return existingVal.Elem().Interface(), nil
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
	logSQL(u.snek, sql, params, err)
	return err
}
