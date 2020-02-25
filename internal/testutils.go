package internal

import (
	"database/sql"
	"fmt"
	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stackworx-go/dberrors/internal/dialect"
	"github.com/stackworx-go/dberrors/parser/mssql"
	"github.com/stackworx-go/dberrors/parser/mysql"
	"github.com/stackworx-go/dberrors/parser/postgres"
	"github.com/stackworx-go/dberrors/parser/sqlite"
	"log"
	"net/url"
	"os"
	"testing"
)

// DDL DDL export
type DDL struct {
	// TODO: delete order
	// TODO: truncate order
	Tables   []Table
	Mysql    []string
	Postgres []string
	Sqlite3  []string
	Mssql    []string
}

// TestCase TestCase export
type TestCase struct {
	DB         *sql.DB
	Dialect    dialect.Dialect
	tables     []Table
	statements []string
}

// Cleanup Cleanup export
type Cleanup func()

func getEnv(key, fallback string) string {
	if key == "" {
		return fallback
	}

	value := os.Getenv(key)
	if len(value) == 0 {
		return fallback
	}
	return value
}

func open(driverName, envVarName, url string) *sql.DB {

	db, err := sql.Open(driverName, getEnv(envVarName, url))

	if err != nil {
		log.Fatalf("Failed to open db connection to %s: %v", driverName, err)
	}

	err = db.Ping()

	if err != nil {
		log.Fatalf("Failed to ping db %s: %v", driverName, err)
	}

	return db
}

type Table struct {
	DropOrder int
	Name      string
}

// BuildTestCases BuildTestCases export
func BuildTestCases(t *testing.T, ddl DDL) ([]TestCase, Cleanup) {
	// TODO: flags
	postgresDb := open("postgres", "POSTGRES_URI", "user=db_errors dbname=db_errors_test password=db_errors_password sslmode=disable")
	sqliteDb := open("sqlite3", "", ":memory:")

	_, err := sqliteDb.Exec("PRAGMA foreign_keys=ON;")

	if err != nil {
		t.Fatal(err)
	}

	u := &url.URL{
		Scheme: "sqlserver",
		User:   url.UserPassword("sa", "eioC9vvCZzQSy4S9g37i"),
		Host:   fmt.Sprintf("%s:%d", "localhost", 1433),
	}

	mssqlDb := open("sqlserver", "MSSQL_URI", u.String())
	mysqlDb := open("mysql", "MYSQL_URI", "root:root@(localhost:3306)/db_errors_test")

	return []TestCase{{
			DB:         postgresDb,
			Dialect:    dialect.POSTGRES,
			statements: ddl.Postgres,
			tables:     ddl.Tables,
		}, {
			DB:         sqliteDb,
			Dialect:    dialect.SQLITE3,
			statements: ddl.Sqlite3,
			tables:     ddl.Tables,
		}, {
			DB:         mssqlDb,
			Dialect:    dialect.MSSQL,
			statements: ddl.Mssql,
			tables:     ddl.Tables,
		}, {
			DB:         mysqlDb,
			Dialect:    dialect.MYSQL,
			statements: ddl.Mysql,
			tables:     ddl.Tables,
		}}, func() {
			// TODO: run in parallel
			if err := postgresDb.Close(); err != nil {
				log.Fatalf("failed to close postgres: %v", err)
			}
			if err = sqliteDb.Close(); err != nil {
				log.Fatalf("failed to close sqlite: %v", err)
			}
			if err = mssqlDb.Close(); err != nil {
				log.Fatalf("failed to close mssql: %v", err)
			}
			if err = mysqlDb.Close(); err != nil {
				log.Fatalf("failed to close mysql: %v", err)
			}
		}
}

// TearDown TearDown export
func (tc TestCase) TearDown(t *testing.T) {
	fmt.Printf("TearDown %s\n", tc.Dialect)
	err := tc.dropTablesIfExists()

	if err != nil {
		t.Fatalf("TearDown Failed for dialect %v: %v", tc.Dialect, err)
	}
}

// SetUp SetUp export
func (tc TestCase) SetUp(t *testing.T) {
	fmt.Printf("SetUp %s\n", tc.Dialect)
	err := tc.dropTablesIfExists()

	if err != nil {
		t.Fatalf("SetUp Failed for dialect %v: %v", tc.Dialect, err)
	}

	for _, statement := range tc.statements {
		_, err := tc.DB.Exec(statement)

		if err != nil {
			t.Fatalf("SetUp Failed for dialect %v: %v, SQL: %s", tc.Dialect, err, statement)
		}
	}
}

type ByDropOrder []Table

func (a ByDropOrder) Len() int           { return len(a) }
func (a ByDropOrder) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByDropOrder) Less(i, j int) bool { return a[i].DropOrder > a[j].DropOrder }

func (tc TestCase) dropTablesIfExists() error {
	orderedTables := make([]Table, len(tc.tables))
	_ = copy(orderedTables, tc.tables)

	for _, table := range orderedTables {
		stmt := fmt.Sprintf("drop table if exists %s", Quote(tc.Dialect, table.Name))
		fmt.Printf("Statement: %s, Dialect: %s\n", stmt, tc.Dialect)
		_, err := tc.DB.Exec(stmt)

		if err != nil {
			return err
		}
	}

	return nil
}

// Quote Quote export
func Quote(d dialect.Dialect, identifier string) string {
	switch d {
	case dialect.POSTGRES:
		return fmt.Sprintf(`"%s"`, identifier)
	case dialect.MSSQL:
		return fmt.Sprintf("[%s]", identifier)
	case dialect.MYSQL:
		return fmt.Sprintf("`%s`", identifier)
	case dialect.SQLITE3:
		return fmt.Sprintf("`%s`", identifier)
	default:
		panic(fmt.Errorf("invalid dialect: %s", d))
	}
}

func ParseError(d dialect.Dialect, err error) error {
	switch d {
	case dialect.POSTGRES:
		return postgres.Parse(err)
	case dialect.MSSQL:
		return mssql.Parse(err)
	case dialect.MYSQL:
		return mysql.Parse(err)
	case dialect.SQLITE3:
		return sqlite.Parse(err)
	default:
		panic(fmt.Errorf("invalid dialect: %s", d))
	}
}
