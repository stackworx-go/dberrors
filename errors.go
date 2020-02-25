// Package dberrors transform native driver errors into Standardized errors
// Supported dialects: postgres, mysql, sqlite and mssql.

// postgres - https://github.com/lib/pq

// mysql - https://github.com/go-sql-driver/mysql

// sqlite - https://github.com/mattn/go-sqlite3

// mssql - https://github.com/denisenkom/go-mssqldb

// Note: not all dialects support all error fields.
package dberrors

import (
	"fmt"
	"github.com/stackworx-go/dberrors/internal/dialect"
)

// DbError DbError export
type DbError struct {
	err     error
	dialect dialect.Dialect
}

func NewDbError(err error, dialect dialect.Dialect) DbError {
	return DbError{err: err, dialect: dialect}
}

// Unwrap Unwrap implementation
func (e *DbError) Unwrap() error {
	return e.err
}

// DataError DataError export
type DataError struct {
	DbError
}

func (e *DataError) Error() string {
	return fmt.Sprintf("data error %v", e.DbError.err)
}

// CheckViolationError CheckViolationError export
type CheckViolationError struct {
	Table      string
	Constraint string
	DbError
}

func (e *CheckViolationError) Error() string {
	return fmt.Sprintf("check violation error %s.%s", e.Table, e.Constraint)
}

// No inheritance
// // ConstraintViolationError ConstraintViolationError export
// type ConstraintViolationError struct {
// 	Table      string
// 	Constraint string
// 	Schema     string
//  DbError
// }

// func (e *ConstraintViolationError) Error() string {
// 	return fmt.Sprintf("constraint violation error %s.%s", e.Table, e.Constraint)
// }

// ForeignKeyViolationError ForeignKeyViolationError export
type ForeignKeyViolationError struct {
	Table      string
	Constraint string
	Schema     string
	DbError
}

func (e *ForeignKeyViolationError) Error() string {
	return fmt.Sprintf("not null violation error %s.%s", e.Table, e.Table)
}

// NotNullViolationError NotNullViolationError export
type NotNullViolationError struct {
	Table  string
	Column string
	Schema string
	DbError
}

func (e *NotNullViolationError) Error() string {
	return fmt.Sprintf("not null violation error %s.%s", e.Table, e.Table)
}

// UniqueViolationError UniqueViolationError export
type UniqueViolationError struct {
	Table      string
	Column     string
	Constraint string
	Schema     string
	DbError
}

func (e *UniqueViolationError) Error() string {
	return fmt.Sprintf("unique violation error %s.%s", e.Table, e.Table)
}
