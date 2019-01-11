package main

import (
	"flag"
	"log"
	"os"

	"github.com/jackc/pgx"

	"github.com/ikitiki/logical_backup/pkg/logicalrestore"
	"github.com/ikitiki/logical_backup/pkg/message"
)

var (
	pgUser, pgPass, pgHost, pgDbname              *string
	targetTable, backupDir, tableName, schemaName *string
	pgPort                                        *uint
)

func init() {
	//TODO: switch to go-flags or similar
	pgDbname = flag.String("db", "postgres", "Name of the database to connect to")
	pgUser = flag.String("user", "postgres", "Postgres user name")
	pgPass = flag.String("password", "", "Postgres password")
	pgHost = flag.String("host", "localhost", "Postgres server hostname")
	pgPort = flag.Uint("port", 5432, "Postgres server port")

	tableName = flag.String("table", "", "Source table name")
	schemaName = flag.String("schema", "public", "Schema name")
	targetTable = flag.String("target-table", "", "Target table name (optional)")
	backupDir = flag.String("backup-dir", "", "Backups dir")

	flag.Parse()

	if *tableName == "" || *schemaName == "" || *backupDir == "" {
		flag.Usage()
		os.Exit(1)
	}
}

func main() {
	if *targetTable != "" {
		log.Printf("restoring into %v", targetTable)
	}

	config := pgx.ConnConfig{
		Database: *pgDbname,
		User:     *pgUser,
		Port:     uint16(*pgPort),
		Password: *pgPass,
		Host:     *pgHost,
	}

	// honor PGHOST, PGPORT and other libpq variables when set.
	envConfig, err := pgx.ParseEnvLibpq()
	if err != nil {
		log.Fatalf("could not parse libpq environment variables: %v", err)
	}
	config = config.Merge(envConfig)

	tbl := message.NamespacedName{Namespace: *schemaName, Name: *tableName}
	r := logicalrestore.New(tbl, *backupDir, config)

	if err := r.Restore(); err != nil {
		log.Fatalf("could not restore table: %v", err)
	}
}
