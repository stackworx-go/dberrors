package sqlite

import (
	"github.com/mattn/go-sqlite3"
	"github.com/stackworx-go/dberrors"
	"github.com/stackworx-go/dberrors/internal/dialect"
	"regexp"
)

// Parse Parse export
func Parse(err error) error {

	if nativeError, ok := err.(sqlite3.Error); ok {
		if err := constraintViolationError(nativeError); err != nil {
			return err
		}
	}

	return nil
}

func constraintViolationError(nativeError sqlite3.Error) error {
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

	return nil
}

var uniqueViolationErrorRe = regexp.MustCompile(`UNIQUE constraint failed: (.+)\.(.+)$`)

func uniqueViolationError(nativeError sqlite3.Error) error {
	if nativeError.Code == 19 && nativeError.ExtendedCode == 2067 {
		if match := uniqueViolationErrorRe.FindStringSubmatch(nativeError.Error()); match != nil {
			return &dberrors.UniqueViolationError{
				Column:  match[2],
				Table:   match[1],
				DbError: dberrors.NewDbError(nativeError, dialect.SQLITE3),
			}
		}
	}

	return nil
}

var notNullViolationErrorRe = regexp.MustCompile(`NOT NULL constraint failed: (.+)\.(.+)`)

func notNullViolationError(nativeError sqlite3.Error) error {
	if nativeError.Code == sqlite3.ErrConstraint && nativeError.ExtendedCode == 1299 {
		if match := notNullViolationErrorRe.FindStringSubmatch(nativeError.Error()); match != nil {
			return &dberrors.NotNullViolationError{
				Table:   match[1],
				Column:  match[2],
				DbError: dberrors.NewDbError(nativeError, dialect.SQLITE3),
			}
		}
	}

	return nil
}

func foreignKeyViolationError(nativeError sqlite3.Error) error {
	if nativeError.Code == sqlite3.ErrConstraint && nativeError.ExtendedCode == 787 {
		return &dberrors.ForeignKeyViolationError{
			DbError: dberrors.NewDbError(nativeError, dialect.SQLITE3),
		}
	}

	return nil
}

func checkViolationError(nativeError sqlite3.Error) error {
	if nativeError.Code == sqlite3.ErrConstraint {
		return &dberrors.CheckViolationError{
			DbError: dberrors.NewDbError(nativeError, dialect.SQLITE3),
		}
	}

	return nil
}
