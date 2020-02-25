package mssql

import (
	mssqldb "github.com/denisenkom/go-mssqldb"
	"github.com/stackworx-go/dberrors"
	"github.com/stackworx-go/dberrors/internal/dialect"
	"regexp"
)

// Parse Parse export
func Parse(err error) error {
	if nativeError, ok := err.(mssqldb.Error); ok {
		if err := uniqueViolationError(nativeError); err != nil {
			return err
		}

		if err := notNullViolationError(nativeError); err != nil {
			return err
		}

		if err := foreignKeyViolationError(nativeError); err != nil {
			return err
		}

		if err := checkViolationError(nativeError); err != nil {
			return err
		}

		if isDataException(nativeError) {
			return &dberrors.DataError{
				DbError: dberrors.NewDbError(nativeError, dialect.MSSQL),
			}
		}
	}

	return nil
}

var uniqueViolationErrorUniqueIndexRe = regexp.MustCompile(`Cannot insert duplicate key row in object '(.+)\.(.+)' with unique index '(.+)'. The duplicate key value is (.+).`)
var uniqueViolationErrorUniqueConstraintRe = regexp.MustCompile(`Violation of UNIQUE KEY constraint '(.+)'. Cannot insert duplicate key in object '(.+)\.(.+)'. The duplicate key value is \((.+)\)`)

func uniqueViolationError(nativeError mssqldb.Error) error {
	// 2627 - Violation in unique constraint (although it is implemented using unique index)
	if isErrorClassAndNumber(nativeError, 14, 2627) {
		if match := uniqueViolationErrorUniqueConstraintRe.FindStringSubmatch(nativeError.Message); match != nil {
			return &dberrors.UniqueViolationError{
				Table:      match[3],
				Schema:     match[2],
				Constraint: match[1],
				DbError:    dberrors.NewDbError(nativeError, dialect.MSSQL),
			}
		}
	}

	// 2601 - Violation in unique index
	if isErrorClassAndNumber(nativeError, 14, 2601) {
		if match := uniqueViolationErrorUniqueIndexRe.FindStringSubmatch(nativeError.Message); match != nil {
			return &dberrors.UniqueViolationError{
				Table:      match[2],
				Constraint: match[3],
				Schema:     match[1],
				DbError:    dberrors.NewDbError(nativeError, dialect.MSSQL),
			}
		}
	}

	return nil
}

var notNullViolationErrorRe = regexp.MustCompile(`Cannot insert the value NULL into column '(.+)', table '(.+)\.(.+)\.(.+)'; column does not allow nulls. (?:INSERT|UPDATE) fails.`)

func notNullViolationError(nativeError mssqldb.Error) error {
	if isErrorClassAndNumber(nativeError, 16, 515) || isErrorClassAndNumber(nativeError, 16, 50000) {
		if match := notNullViolationErrorRe.FindStringSubmatch(nativeError.Message); match != nil {
			return &dberrors.NotNullViolationError{
				Table:   match[4],
				Column:  match[1],
				Schema:  match[3],
				DbError: dberrors.NewDbError(nativeError, dialect.MSSQL),
			}
		}

	}

	return nil
}

var foreignKeyViolationErrorInsertUpdateRe = regexp.MustCompile(`The (?:INSERT|UPDATE) statement conflicted with the FOREIGN KEY constraint "(.+)". The conflict occurred in database "(.+)", table "(.+)\.(.+)", column '(.+)'.`)
var foreignKeyViolationErrorDeleteRe = regexp.MustCompile(`The DELETE statement conflicted with the REFERENCE constraint "(.+)". The conflict occurred in database "(.+)", table "(.+)\.(.+)", column '(.+)'.`)

func foreignKeyViolationError(nativeError mssqldb.Error) error {
	if isErrorClassAndNumber(nativeError, 16, 547) {
		if match := foreignKeyViolationErrorInsertUpdateRe.FindStringSubmatch(nativeError.Message); match != nil {
			return &dberrors.ForeignKeyViolationError{
				Table:      match[4],
				Schema:     match[3],
				Constraint: match[1],
				DbError:    dberrors.NewDbError(nativeError, dialect.MSSQL),
			}
		}

		if match := foreignKeyViolationErrorDeleteRe.FindStringSubmatch(nativeError.Message); match != nil {
			return &dberrors.ForeignKeyViolationError{
				Table:      match[4],
				Schema:     match[3],
				Constraint: match[1],
				DbError:    dberrors.NewDbError(nativeError, dialect.MSSQL),
			}
		}
	}

	return nil
}

var checkViolationErrorRegex = regexp.MustCompile(`The (INSERT|UPDATE) statement conflicted with the CHECK constraint "(.+)". The conflict occurred in database "(.+)", table "(.+\.)?(.+)", column '(.+)'.`)

func checkViolationError(nativeError mssqldb.Error) error {
	if isErrorClassAndNumber(nativeError, 16, 547) {
		if match := checkViolationErrorRegex.FindStringSubmatch(nativeError.Message); match != nil {
			return &dberrors.CheckViolationError{
				Table:      match[5],
				Constraint: match[2],
				DbError:    dberrors.NewDbError(nativeError, dialect.MSSQL),
			}
		}
	}

	return nil
}

func isErrorClassAndNumber(nativeError mssqldb.Error, class uint8, number int32) bool {
	return nativeError.SQLErrorClass() == class && nativeError.SQLErrorNumber() == number
}

func isDataException(nativeError mssqldb.Error) bool {
	// 241 - Conversion failed when converting date and/or time from character string.
	// 242 - The conversion of a nvarchar data type to a datetime data type resulted in an out-of-range value.
	// 245 - Conversion failed when converting the nvarchar value 'lol' to data type int.
	// 8152 - String or binary data would be truncated.
	return isErrorClassAndNumber(nativeError, 16, 241) ||
		isErrorClassAndNumber(nativeError, 16, 242) ||
		isErrorClassAndNumber(nativeError, 16, 245) ||
		isErrorClassAndNumber(nativeError, 16, 8152)
}
