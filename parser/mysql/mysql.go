package mysql

import (
	"errors"
	"github.com/stackworx-go/dberrors"
	"github.com/stackworx-go/dberrors/internal/dialect"
	"regexp"

	"github.com/go-sql-driver/mysql"
)

// Parse Parse export
func Parse(err error) error {
	var nativeError *mysql.MySQLError
	if errors.As(err, &nativeError) {
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
				DbError: dberrors.NewDbError(nativeError, dialect.MYSQL),
			}
		}
	}

	return nil
}

var uniqueViolationErrorRe = regexp.MustCompile(`Duplicate entry '(.+)' for key '(.+)'`)

func uniqueViolationError(nativeError *mysql.MySQLError) error {
	// ER_DUP_ENTRY - 1062
	// ER_DUP_ENTRY - 1569
	// ER_DUP_ENTRY_WITH_KEY_NAME - 1586
	if nativeError.Number == 1062 || nativeError.Number == 1569 || nativeError.Number == 1586 {
		if match := uniqueViolationErrorRe.FindStringSubmatch(nativeError.Message); match != nil {
			return &dberrors.UniqueViolationError{
				Constraint: match[2],
				DbError:    dberrors.NewDbError(nativeError, dialect.MYSQL),
			}
		}
	}

	return nil
}

var notNullViolationErrorBadNullErrorRe = regexp.MustCompile(`Column '(.+)' cannot be null`)
var notNullViolationErrorNoDefaultForFieldRe = regexp.MustCompile(`Field '(.+)' doesn't have a default value`)

func notNullViolationError(nativeError *mysql.MySQLError) error {
	// ER_BAD_NULL_ERROR - 1048
	if nativeError.Number == 1048 {
		if match := notNullViolationErrorBadNullErrorRe.FindStringSubmatch(nativeError.Message); match != nil {
			return &dberrors.NotNullViolationError{
				Column:  match[1],
				DbError: dberrors.NewDbError(nativeError, dialect.MYSQL),
			}
		}
	}

	// ER_NO_DEFAULT_FOR_FIELD - 1364
	if nativeError.Number == 1364 {
		if match := notNullViolationErrorNoDefaultForFieldRe.FindStringSubmatch(nativeError.Message); match != nil {
			return &dberrors.NotNullViolationError{
				DbError: dberrors.NewDbError(nativeError, dialect.MYSQL),
			}
		}
	}

	return nil
}

var foreignKeyViolationErrorNoReferencedRe = regexp.MustCompile("a foreign key constraint fails \\(`(.+)`\\.`(.+)`, CONSTRAINT `(.+)` FOREIGN KEY \\(`(.+)`\\) REFERENCES `(.+)` \\(`(.+)`\\)/")
var foreignKeyViolationErrorRowIsReferencedRe = regexp.MustCompile("Cannot (?:add|delete) or update a (?:parent|child) row: a foreign key constraint fails \\(`(.+)`\\.`(.+)`, CONSTRAINT `(.+)` FOREIGN KEY \\(`(.+)`\\) REFERENCES `(.+)` \\(`(.+)`\\)")

func foreignKeyViolationError(nativeError *mysql.MySQLError) error {
	// ER_NO_REFERENCED_ROW - 1216
	// ER_ROW_IS_REFERENCED - 23000
	// For these variants, there is no table or constraint information available.
	// These seem to be mostly thrown on mysql 8 when the db user doesn't have
	// privileges to the parent table. There seems to be a bug however that
	// causes these generic errors to be thrown even in some cases where the
	// user DOES have privileges.
	if nativeError.Number == 1216 || nativeError.Number == 23000 {
		if match := foreignKeyViolationErrorNoReferencedRe.FindStringSubmatch(nativeError.Message); match != nil {
			return &dberrors.ForeignKeyViolationError{
				DbError: dberrors.NewDbError(nativeError, dialect.MYSQL),
			}
		}
	}

	// ER_NO_REFERENCED_ROW_2 - 1452
	if nativeError.Number == 1452 {
		if match := foreignKeyViolationErrorRowIsReferencedRe.FindStringSubmatch(nativeError.Message); match != nil {
			return &dberrors.ForeignKeyViolationError{
				Schema:     match[1],
				Table:      match[2],
				Constraint: match[3],
				DbError:    dberrors.NewDbError(nativeError, dialect.MYSQL),
			}
		}
	}

	// ER_ROW_IS_REFERENCED_2 - 1451
	if nativeError.Number == 1451 {
		if match := foreignKeyViolationErrorNoReferencedRe.FindStringSubmatch(nativeError.Message); match != nil {
			return &dberrors.ForeignKeyViolationError{
				//Schema: nativeError.Schema,
				//Table: nativeError.Table,
				//Constraint: nativeError.Constraint,
				DbError: dberrors.NewDbError(nativeError, dialect.MYSQL),
			}
		}
	}

	return nil
}

func checkViolationError(nativeError *mysql.MySQLError) error {
	if nativeError.Message == "23514" {
		// TODO
		return &dberrors.CheckViolationError{
			//Table:nativeError.Table,
			//Constraint: nativeError.Constraint,
			DbError: dberrors.NewDbError(nativeError, dialect.MYSQL),
		}
	}

	return nil
}

func isDataException(nativeError *mysql.MySQLError) bool {
	// ER_DATA_TOO_LONG - 1406
	// ER_TRUNCATED_WRONG_VALUE - 1292
	// ER_TRUNCATED_WRONG_VALUE_FOR_FIELD - 1366
	return nativeError.Number == 1406 || nativeError.Number == 1292 || nativeError.Number == 1366
}
