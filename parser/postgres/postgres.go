package postgres

import (
	"errors"
	"github.com/lib/pq"
	"github.com/stackworx-go/dberrors"
	"github.com/stackworx-go/dberrors/internal/dialect"
	"regexp"
)

// Parse Parse export
func Parse(err error) error {
	var nativeError *pq.Error
	if errors.As(err, &nativeError) {
		if err := constraintViolationError(nativeError); err != nil {
			return err
		}

		if isDataException(nativeError) {
			return &dberrors.DataError{
				DbError: dberrors.NewDbError(nativeError, dialect.POSTGRES),
			}
		}
	}

	return nil
}

func constraintViolationError(nativeError *pq.Error) error {
	if isIntegrityConstraintViolation(nativeError) {
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
	}

	return nil
}

var uniqueViolationErrorDetailRe = regexp.MustCompile(`Key \((.+)\)=\(.+\) already exists`)
var uniqueViolationErrorConstraintRe = regexp.MustCompile(`(.+) unique`)

func uniqueViolationError(nativeError *pq.Error) error {
	if nativeError.Code == "23505" {
		if match := uniqueViolationErrorDetailRe.FindStringSubmatch(nativeError.Detail); match != nil {
			if constraintMatch := uniqueViolationErrorConstraintRe.FindStringSubmatch(nativeError.Constraint); constraintMatch != nil {
				return &dberrors.UniqueViolationError{
					Table:      nativeError.Table,
					Schema:     nativeError.Schema,
					Constraint: constraintMatch[1],
					Column:     match[1],
					DbError:    dberrors.NewDbError(nativeError, dialect.POSTGRES),
				}
			}
		}
	}

	return nil
}

func notNullViolationError(nativeError *pq.Error) error {
	if nativeError.Code == "23502" {
		return &dberrors.NotNullViolationError{
			Table:   nativeError.Table,
			Column:  nativeError.Column,
			DbError: dberrors.NewDbError(nativeError, dialect.POSTGRES),
		}
	}

	return nil
}

func foreignKeyViolationError(nativeError *pq.Error) error {
	if nativeError.Code == "23503" {
		return &dberrors.ForeignKeyViolationError{
			Schema:     nativeError.Schema,
			Table:      nativeError.Table,
			Constraint: nativeError.Constraint,
			DbError:    dberrors.NewDbError(nativeError, dialect.POSTGRES),
		}
	}

	return nil
}

func checkViolationError(nativeError *pq.Error) error {
	if nativeError.Code == "23514" {
		// TODO
		return &dberrors.CheckViolationError{
			Table:      nativeError.Table,
			Constraint: nativeError.Constraint,
			DbError:    dberrors.NewDbError(nativeError, dialect.POSTGRES),
		}
	}

	return nil
}

func isDataException(nativeError *pq.Error) bool {
	return nativeError.Code.Class() == "22"
}

func isIntegrityConstraintViolation(nativeError *pq.Error) bool {
	return nativeError.Code.Class() == "23"
}
