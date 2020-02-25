package non_null_violation_error__test

import (
	"flag"
	"fmt"
	"github.com/stackworx-go/dberrors"
	"github.com/stackworx-go/dberrors/internal/dialect"
	"github.com/stretchr/testify/assert"
	"log"
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
    "id"              serial primary key,
    "i_am_unique_col" integer,
    "uniquePart1"     varchar(255),
    "uniquePart2"     varchar(255),
    constraint "thetable_i_am_unique_col_unique unique" unique ("i_am_unique_col"),
    constraint "thetable_uniquepart1_uniquepart2_unique" unique ("uniquePart1", "uniquePart2")
);`, table)},
	Sqlite3: []string{fmt.Sprintf(`
create table %s
(
    id              integer not null primary key autoincrement,
    i_am_unique_col integer,
    uniquePart1     varchar(255),
    uniquePart2     varchar(255),
    constraint thetable_i_am_unique_col_unique unique (i_am_unique_col),
    constraint thetable_uniquepart1_uniquepart2_unique unique (uniquePart1, uniquePart2)
);`, table)},
	Mssql: []string{fmt.Sprintf(`
CREATE TABLE [%s]
(
    [id]              int identity (1,1) not null primary key,
    [i_am_unique_col] int,
    [uniquePart1]     nvarchar(255),
    [uniquePart2]     nvarchar(255),
    CONSTRAINT [thetable_i_am_unique_col_unique] UNIQUE ([i_am_unique_col]),
    
);`, table), fmt.Sprintf(`
create unique index thetable_uniquepart1_uniquepart2_unique on %s(uniquePart1, uniquePart2) where uniquePart1 is not null and uniquePart2 is not null;`, table)},
	Mysql: []string{fmt.Sprintf(`
create table theTable
(
    id              int unsigned not null auto_increment primary key,
    i_am_unique_col int,
    uniquePart1     varchar(255),
    uniquePart2     varchar(255),

    constraint thetable_i_am_unique_col_unique unique (i_am_unique_col),
    constraint thetable_uniquepart1_uniquepart2_unique unique (uniquePart1, uniquePart2)
);`)},
}

func TestInsertSingleColumn(t *testing.T) {
	testCases, cleanup := internal.BuildTestCases(t, ddl)
	defer cleanup()

	for _, tc := range testCases {
		t.Run(string(tc.Dialect), func(t *testing.T) {
			tc.SetUp(t)
			defer tc.TearDown(t)

			var err error

			if tc.Dialect == dialect.MSSQL {
				_, err = tc.DB.Exec(fmt.Sprintf("insert into %s (%s) values (@p1)",
					internal.Quote(tc.Dialect, table),
					internal.Quote(tc.Dialect, "i_am_unique_col"),
				), 1)

				if err != nil {
					log.Fatalf("failed to insert: %v", err)
				}

				_, err = tc.DB.Exec(fmt.Sprintf("insert into %s (%s) values (@p1)",
					internal.Quote(tc.Dialect, table),
					internal.Quote(tc.Dialect, "i_am_unique_col"),
				), 2)

				if err != nil {
					log.Fatalf("failed to insert: %v", err)
				}

				_, err = tc.DB.Exec(fmt.Sprintf("update %s set %[2]s = @p1 where %[2]s = @p2",
					internal.Quote(tc.Dialect, table),
					internal.Quote(tc.Dialect, "i_am_unique_col"),
				), 1, 2)
			} else if tc.Dialect == dialect.MYSQL {
				_, err = tc.DB.Exec(fmt.Sprintf("insert into %s (%s) values (?)",
					internal.Quote(tc.Dialect, table),
					internal.Quote(tc.Dialect, "i_am_unique_col"),
				), 1)

				if err != nil {
					log.Fatalf("failed to insert: %v", err)
				}

				_, err = tc.DB.Exec(fmt.Sprintf("insert into %s (%s) values (?)",
					internal.Quote(tc.Dialect, table),
					internal.Quote(tc.Dialect, "i_am_unique_col"),
				), 2)

				if err != nil {
					log.Fatalf("failed to insert: %v", err)
				}

				_, err = tc.DB.Exec(fmt.Sprintf("update %s set %[2]s = ? where %[2]s = ?",
					internal.Quote(tc.Dialect, table),
					internal.Quote(tc.Dialect, "i_am_unique_col"),
				), 1, 2)
			} else {
				_, err = tc.DB.Exec(fmt.Sprintf("insert into %s (%s) values ($1)",
					internal.Quote(tc.Dialect, table),
					internal.Quote(tc.Dialect, "i_am_unique_col"),
				), 1)

				if err != nil {
					log.Fatalf("failed to insert: %v", err)
				}

				_, err = tc.DB.Exec(fmt.Sprintf("insert into %s (%s) values ($1)",
					internal.Quote(tc.Dialect, table),
					internal.Quote(tc.Dialect, "i_am_unique_col"),
				), 2)

				if err != nil {
					log.Fatalf("failed to insert: %v", err)
				}

				_, err = tc.DB.Exec(fmt.Sprintf("update %s set %[2]s = $1 where %[2]s = $2",
					internal.Quote(tc.Dialect, table),
					internal.Quote(tc.Dialect, "i_am_unique_col"),
				), 1, 2)
			}

			assert.Error(t, err)

			parsedErr := internal.ParseError(tc.Dialect, err)

			if tc.Dialect == dialect.POSTGRES {
				assert.Equal(t, &dberrors.UniqueViolationError{
					Table:      table,
					Schema:     "public",
					Column:     "i_am_unique_col",
					Constraint: "thetable_i_am_unique_col_unique",
					DbError:    dberrors.NewDbError(err, tc.Dialect),
				}, parsedErr)
			} else if tc.Dialect == dialect.MSSQL {
				assert.Equal(t, &dberrors.UniqueViolationError{
					Table:      table,
					Constraint: "thetable_i_am_unique_col_unique",
					Schema:     "dbo",
					DbError:    dberrors.NewDbError(err, tc.Dialect),
				}, parsedErr)
			} else if tc.Dialect == dialect.MYSQL {
				assert.Equal(t, &dberrors.UniqueViolationError{
					Constraint: "thetable_i_am_unique_col_unique",
					DbError:    dberrors.NewDbError(err, tc.Dialect),
				}, parsedErr)
			} else {
				assert.Equal(t, &dberrors.UniqueViolationError{
					Column:  "i_am_unique_col",
					Table:   table,
					DbError: dberrors.NewDbError(err, tc.Dialect),
				}, parsedErr)
			}
		})
	}
}
func TestInsertMultipleColumns(t *testing.T) {
	t.Skip("pending")
}

func TestUpdateSingleColumn(t *testing.T) {
	t.Skip("pending")
}

func TestUpdateSingleColumnSubquery(t *testing.T) {
	t.Skip("pending")
}

func TestMain(m *testing.M) {
	_ = flag.String("mysql", "", "Run Mysql Tests")
	_ = flag.String("postgres", "", "Run Postgres Test")
	flag.Parse()
	os.Exit(m.Run())
}
