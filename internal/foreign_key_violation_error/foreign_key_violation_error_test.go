package foreign_key_violation_error

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

var sourceTable = "source"
var targetTable = "target"

var ddl = internal.DDL{
	Tables: []internal.Table{{
		Name:      sourceTable,
		DropOrder: 1,
	}, {
		Name:      targetTable,
		DropOrder: 0,
	}},
	Postgres: []string{fmt.Sprintf(`
create table "%s"
(
    "id"    serial primary key,
    "value" integer
);`, targetTable),
		fmt.Sprintf(`
create table "%[1]s"
(
    "id"          serial primary key,
    "foreign_key" integer,
    constraint %[1]s_foreign_key_foreign
        foreign key (foreign_key)
            REFERENCES %[1]s ("id") on delete CASCADE
);`, sourceTable)},
	Sqlite3: []string{fmt.Sprintf(`
create table %s
(
    id    integer not null primary key autoincrement,
    value integer
);`, targetTable),
		fmt.Sprintf(`
create table %[1]s
(
    id          integer not null primary key autoincrement,
    foreign_key integer,
    foreign key (foreign_key) references %[2]s (id)
);`, sourceTable, targetTable)},
	Mssql: []string{fmt.Sprintf(`
CREATE TABLE [%s]
(
    [id]    int identity(1, 1) not null primary key,
    [value] int
);`, targetTable),
		fmt.Sprintf(`
CREATE TABLE [%[1]s]
(
    [id]          int identity(1, 1) not null primary key,
    [foreign_key] int,
    CONSTRAINT [%[1]s_foreign_key_foreign] FOREIGN KEY ([foreign_key]) REFERENCES [%[2]s] ([id])
);`, sourceTable, targetTable)},
	Mysql: []string{fmt.Sprintf(`
create table %s
(
    id    int unsigned not null auto_increment primary key,
    value int
);`, targetTable),
		fmt.Sprintf(`
create table %[1]s
(
    id          int unsigned not null auto_increment primary key,
    foreign_key int unsigned,
    constraint %[1]s_foreign_key_foreign FOREIGN KEY (foreign_key)
        REFERENCES %[2]s (id)
        ON DELETE CASCADE
);`, sourceTable, targetTable)},
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
				_, err = tc.DB.Exec(fmt.Sprintf("insert into %s (%s) values (@p1)",
					internal.Quote(tc.Dialect, sourceTable), internal.Quote(tc.Dialect, "foreign_key")), 123456)
			} else if tc.Dialect == dialect.MYSQL {
				_, err = tc.DB.Exec(fmt.Sprintf("insert into %s (%s) values (?)",
					internal.Quote(tc.Dialect, sourceTable), internal.Quote(tc.Dialect, "foreign_key")), 123456)
			} else {
				_, err = tc.DB.Exec(fmt.Sprintf("insert into %s (%s) values ($1)",
					internal.Quote(tc.Dialect, sourceTable), internal.Quote(tc.Dialect, "foreign_key")), 123456)
			}

			assert.Error(t, err)

			parsedErr := internal.ParseError(tc.Dialect, err)

			if tc.Dialect == dialect.MSSQL {
				assert.Equal(t, &dberrors.ForeignKeyViolationError{
					Table:      "target",
					Constraint: "source_foreign_key_foreign",
					Schema:     "dbo",
					DbError:    dberrors.NewDbError(err, tc.Dialect),
				}, parsedErr)
			} else if tc.Dialect == dialect.POSTGRES {
				assert.Equal(t, &dberrors.ForeignKeyViolationError{
					Table:      "source",
					Schema:     "public",
					Constraint: "source_foreign_key_foreign",
					DbError:    dberrors.NewDbError(err, tc.Dialect),
				}, parsedErr)
			} else if tc.Dialect == dialect.MYSQL {
				assert.Equal(t, &dberrors.ForeignKeyViolationError{
					Table:      "source",
					Schema:     "db_errors_test",
					Constraint: "source_foreign_key_foreign",
					DbError:    dberrors.NewDbError(err, tc.Dialect),
				}, parsedErr)
			} else {
				assert.Equal(t, &dberrors.ForeignKeyViolationError{
					Table:      "",
					Schema:     "",
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
