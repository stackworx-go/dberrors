package data_error_test

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
    "id"        serial primary key,
    "date"      date,
    "date_time" timestamptz,
    "string"    varchar(10),
    "int"       integer
);
        `, table)},
	Sqlite3: []string{fmt.Sprintf(`
create table %s
(
    id        integer not null primary key autoincrement,
    date      date,
    date_time datetime,
    string    varchar(10),
    int       integer
);`, table)},
	// Primary key cannot be null in MSSQL
	Mssql: []string{fmt.Sprintf(`
CREATE TABLE [%s]
(
    [id]
    int
    identity(1,1) not null primary key, 
	[date] date, 
	[date_time] datetime, 
	[string] nvarchar(10), 
	[int] int);
`, table)},
	Mysql: []string{fmt.Sprintf(`
create table %s
(
    id        int unsigned not null auto_increment primary key,
    date      date,
    date_time datetime,
    string    varchar(10),
    %s       int
);`, table, "`int`")},
}

func TestDateRandomText(t *testing.T) {
	testCases, cleanup := internal.BuildTestCases(t, ddl)
	defer cleanup()

	for _, tc := range testCases {
		t.Run(string(tc.Dialect), func(t *testing.T) {
			if tc.Dialect == dialect.SQLITE3 {
				// https://www.sqlite.org/faq.html (3)
				t.Skip("SQLITE does not throw data errors")
			}

			tc.SetUp(t)
			defer tc.TearDown(t)

			var err error

			if tc.Dialect == dialect.MSSQL {
				_, err = tc.DB.Exec(fmt.Sprintf("insert into %s (%s) values (@p1)",
					internal.Quote(tc.Dialect, table), internal.Quote(tc.Dialect, "date")), "lol")
			} else if tc.Dialect == dialect.MYSQL {
				_, err = tc.DB.Exec(fmt.Sprintf("insert into %s (%s) values (?)",
					internal.Quote(tc.Dialect, table), internal.Quote(tc.Dialect, "date")), "lol")
			} else {
				_, err = tc.DB.Exec(fmt.Sprintf("insert into %s (%s) values ($1)",
					internal.Quote(tc.Dialect, table), internal.Quote(tc.Dialect, "date")), "lol")
			}

			assert.Error(t, err)

			parsedErr := internal.ParseError(tc.Dialect, err)

			assert.Equal(t, &dberrors.DataError{
				DbError: dberrors.NewDbError(err, tc.Dialect),
			}, parsedErr)
		})
	}
}

func TestDateTimeRandomText(t *testing.T) {
	t.Skip("pending")
}

func TestDateTimeInvalidDate(t *testing.T) {
	t.Skip("pending")
}

func TestStringTooLong(t *testing.T) {
	t.Skip("pending")
}

func TestIntegerInvalid(t *testing.T) {
	t.Skip("pending")
}

func TestMain(m *testing.M) {
	_ = flag.String("mysql", "", "Run Mysql Tests")
	_ = flag.String("postgres", "", "Run Postgres Test")
	flag.Parse()
	os.Exit(m.Run())
}
