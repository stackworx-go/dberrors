package check_violation_error_test

import (
	"flag"
	"fmt"
	"github.com/stackworx-go/dberrors"
	"github.com/stackworx-go/dberrors/internal/dialect"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"

	_ "github.com/lib/pq"
	"github.com/stackworx-go/dberrors/internal"
)

var table = "theTable"

var ddl = internal.DDL{
	Tables: []internal.Table{{
		Name: table,
	}},
	Postgres: []string{fmt.Sprintf(`
CREATE TABLE "%s"
(
    id         serial PRIMARY KEY,
    value1     integer,
    "theValue" integer,

    CHECK (value1 < 10),
    CHECK ("theValue" < 20)
);`, table)},
	Sqlite3: []string{fmt.Sprintf(`
CREATE TABLE %s
(
    id       integer PRIMARY KEY,
    value1   integer,
    theValue integer,

    CHECK (value1 < 10),
    CHECK (theValue < 20)
);`, table)},
	// Primary key cannot be null in MSSQL
	Mssql: []string{fmt.Sprintf(`
CREATE TABLE "%[1]s"
(
    id         int,
    value1     integer,
    "theValue" integer, CONSTRAINT %[1]s_value1_check CHECK(value1 <10), CONSTRAINT %[1]s_theValue_check CHECK ("theValue" < 20)
);`, table)},
}

func TestInsert(t *testing.T) {
	testCases, cleanup := internal.BuildTestCases(t, ddl)
	defer cleanup()

	for _, tc := range testCases {
		t.Run(string(tc.Dialect), func(t *testing.T) {
			if tc.Dialect == dialect.MYSQL {
				t.Skip("Mysql does not support skip constraints")
			}

			tc.SetUp(t)
			defer tc.TearDown(t)

			var err error

			if tc.Dialect == dialect.MSSQL {
				_, err = tc.DB.Exec(fmt.Sprintf("insert into %s (%s) values (@p1)",
					internal.Quote(tc.Dialect, table), internal.Quote(tc.Dialect, "value1")), 11)
			} else {
				_, err = tc.DB.Exec(fmt.Sprintf("insert into %s (%s) values ($1)",
					internal.Quote(tc.Dialect, table), internal.Quote(tc.Dialect, "value1")), 11)
			}

			assert.Error(t, err)

			parsedErr := internal.ParseError(tc.Dialect, err)

			if tc.Dialect == dialect.POSTGRES || tc.Dialect == dialect.MSSQL {
				assert.Equal(t, &dberrors.CheckViolationError{
					Table:      table,
					Constraint: "theTable_value1_check",
					DbError:    dberrors.NewDbError(err, tc.Dialect),
				}, parsedErr)
			} else {
				assert.Equal(t, &dberrors.CheckViolationError{
					Table:      "",
					Constraint: "",
					DbError:    dberrors.NewDbError(err, tc.Dialect),
				}, parsedErr)
			}
		})
	}
}

func TestUpdate(t *testing.T) {
	testCases, cleanup := internal.BuildTestCases(t, ddl)
	defer cleanup()

	for _, tc := range testCases {
		t.Run(string(tc.Dialect), func(t *testing.T) {
			if tc.Dialect == dialect.MYSQL {
				t.Skip("Mysql does not support skip constraints")
			}

			tc.SetUp(t)
			defer tc.TearDown(t)

			var err error

			if tc.Dialect == dialect.MSSQL {
				_, err = tc.DB.Exec(fmt.Sprintf("insert into %s (%s) values (@p1)",
					internal.Quote(tc.Dialect, table), internal.Quote(tc.Dialect, "value1")), 11)
			} else {
				_, err = tc.DB.Exec(fmt.Sprintf("insert into %s (%s) values ($1)",
					internal.Quote(tc.Dialect, table), internal.Quote(tc.Dialect, "value1")), 11)
			}

			// TODO invoke update

			assert.Error(t, err)

			parsedErr := internal.ParseError(tc.Dialect, err)

			if tc.Dialect == dialect.POSTGRES || tc.Dialect == dialect.MSSQL {
				assert.Equal(t, &dberrors.CheckViolationError{
					Table:      table,
					Constraint: "theTable_value1_check",
					DbError:    dberrors.NewDbError(err, tc.Dialect),
				}, parsedErr)
			} else {
				assert.Equal(t, &dberrors.CheckViolationError{
					Table:      "",
					Constraint: "",
					DbError:    dberrors.NewDbError(err, tc.Dialect),
				}, parsedErr)
			}
		})
	}
}

func TestMain(m *testing.M) {
	_ = flag.String("mysql", "", "Run Mysql Tests")
	_ = flag.String("postgres", "", "Run Postgres Test")
	flag.Parse()
	os.Exit(m.Run())
}
