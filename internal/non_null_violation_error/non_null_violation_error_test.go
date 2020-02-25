package non_null_violation_error__test

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
create table "%s"
(
    "id"                serial primary key,
    "not_nullable"      integer      not null,
    "notNullableString" varchar(255) not null
);`, table)},
	Sqlite3: []string{fmt.Sprintf(`
create table %s
(
    id                integer      not null primary key autoincrement,
    not_nullable      integer      not null,
    notNullableString varchar(255) not null
);`, table)},
	Mssql: []string{fmt.Sprintf(`
CREATE TABLE [%s]
(
    [id]                int identity (1,1) not null primary key,
    [not_nullable]      int                not null,
    [notNullableString] nvarchar(255)      not null
);`, table)},
	Mysql: []string{fmt.Sprintf(`
create table theTable
(
    id                int unsigned not null auto_increment primary key,
    not_nullable      int          not null,
    notNullableString varchar(255) not null
);`)},
}

func TestInsert(t *testing.T) {
	testCases, cleanup := internal.BuildTestCases(t, ddl)
	defer cleanup()

	for _, tc := range testCases {
		t.Run(string(tc.Dialect), func(t *testing.T) {
			tc.SetUp(t)
			defer tc.TearDown(t)

			var err error

			if tc.Dialect == dialect.MSSQL {
				_, err = tc.DB.Exec(fmt.Sprintf("insert into %s (%s, %s) values (@p1, @p2)",
					internal.Quote(tc.Dialect, table),
					internal.Quote(tc.Dialect, "not_nullable"),
					internal.Quote(tc.Dialect, "notNullableString"),
				), nil, "foot")
			} else if tc.Dialect == dialect.MYSQL {
				_, err = tc.DB.Exec(fmt.Sprintf("insert into %s (%s, %s) values (?, ?)",
					internal.Quote(tc.Dialect, table),
					internal.Quote(tc.Dialect, "not_nullable"),
					internal.Quote(tc.Dialect, "notNullableString"),
				), nil, "foot")
			} else {
				_, err = tc.DB.Exec(fmt.Sprintf("insert into %s (%s, %s) values ($1, $2)",
					internal.Quote(tc.Dialect, table),
					internal.Quote(tc.Dialect, "not_nullable"),
					internal.Quote(tc.Dialect, "notNullableString"),
				), nil, "foot")
			}

			assert.Error(t, err)

			parsedErr := internal.ParseError(tc.Dialect, err)

			if tc.Dialect == dialect.POSTGRES {
				assert.Equal(t, &dberrors.NotNullViolationError{
					Table:   table,
					Column:  "not_nullable",
					DbError: dberrors.NewDbError(err, tc.Dialect),
				}, parsedErr)
			} else if tc.Dialect == dialect.MSSQL {
				assert.Equal(t, &dberrors.NotNullViolationError{
					Table:   table,
					Column:  "not_nullable",
					Schema:  "dbo",
					DbError: dberrors.NewDbError(err, tc.Dialect),
				}, parsedErr)
			} else if tc.Dialect == dialect.MYSQL {
				assert.Equal(t, &dberrors.NotNullViolationError{
					Table:   "",
					Column:  "not_nullable",
					Schema:  "",
					DbError: dberrors.NewDbError(err, tc.Dialect),
				}, parsedErr)
			} else {
				assert.Equal(t, &dberrors.NotNullViolationError{
					Table:   table,
					Column:  "not_nullable",
					DbError: dberrors.NewDbError(err, tc.Dialect),
				}, parsedErr)
			}
		})
	}
}
func TestInsertMissingColumn(t *testing.T) {
	t.Skip("pending")
}

func TestUpdate(t *testing.T) {
	t.Skip("pending")
}

func TestMain(m *testing.M) {
	_ = flag.String("mysql", "", "Run Mysql Tests")
	_ = flag.String("postgres", "", "Run Postgres Test")
	flag.Parse()
	os.Exit(m.Run())
}
