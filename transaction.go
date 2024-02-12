package snek

import (
	"bytes"
	"database/sql"
	"fmt"
	"log"
	"reflect"
	"strings"

	"github.com/jmoiron/sqlx"
)

// View represents a read-only transaction.
type View struct {
	tx   *sqlx.Tx
	snek *Snek
}

// Update represents a read/write transaction.
type Update struct {
	View
}

// View executs f in the context of a read-only transaction.
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

func logSQL(s *Snek, kind string, query string, params []any, err error) {
	s.logIf(s.options.LogQuery, "%s(%q, %+v) => %v", kind, query, params, err)
}

func (v *View) query(query string, params ...any) (*sqlx.Rows, error) {
	rows, err := v.tx.QueryxContext(v.snek.ctx, query, params...)
	logSQL(v.snek, "QUERY", query, params, err)
	return rows, err
}

// Select executs the query and puts the results in structSlicePointer.
func (v *View) Select(structSlicePointer any, query Query) error {
	typ := reflect.TypeOf(structSlicePointer)
	if typ.Kind() != reflect.Ptr || typ.Elem().Kind() != reflect.Slice || typ.Elem().Elem().Kind() != reflect.Struct {
		return fmt.Errorf("only pointers to slices of structs allowed, not %v", structSlicePointer)
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
	err := v.tx.SelectContext(v.snek.ctx, structSlicePointer, queryString, params...)
	logSQL(v.snek, "QUERY", queryString, params, err)
	return err
}

// Get populates structPointer with the data at structPointer.ID in the store.
func (v *View) Get(structPointer any) error {
	info, err := v.snek.getValueInfo(reflect.ValueOf(structPointer))
	if err != nil {
		return err
	}
	query, params := info.toGetStatement()
	err = v.tx.GetContext(v.snek.ctx, structPointer, query, params...)
	logSQL(v.snek, "QUERY", query, params, err)
	return err
}

// Update executs f in the context of a read/write transaction.
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

// Update replaces the data at structPointer.ID with the data inside structPointer.
func (u *Update) Update(structPointer any) error {
	info, err := u.snek.getValueInfo(reflect.ValueOf(structPointer))
	if err != nil {
		return err
	}
	query, params := info.toUpdateStatement()
	return u.exec(query, params...)
}

// Insert places the data inside structPointer at structPointer.ID.
func (u *Update) Insert(structPointer any) error {
	info, err := u.snek.getValueInfo(reflect.ValueOf(structPointer))
	if err != nil {
		return err
	}
	query, params := info.toInsertStatement()
	return u.exec(query, params...)
}

func (u *Update) exec(query string, params ...any) error {
	_, err := u.tx.ExecContext(u.snek.ctx, query, params...)
	logSQL(u.snek, "EXEC", query, params, err)
	return err
}
