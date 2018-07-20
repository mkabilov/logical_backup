package main

import (
	"flag"
	"log"
	"os"
	"strings"

	"github.com/jackc/pgx"

	"github.com/ikitiki/logical_backup/pkg/logicalrestore"
)

func main() {
	var tableName string

	pgDbname := flag.String("db", "postgres", "Name of the database to connect to")
	pgUser := flag.String("user", "postgres", "Postgres user name")
	pgPass := flag.String("password", "", "Postgres password")
	pgHost := flag.String("host", "localhost", "Postgres server hostname")
	pgPort := flag.Uint("port", 5432, "Postgres server port")
	pgTable := flag.String("table", "", "Table name")
	dir := flag.String("dir", "", "Backups dir")

	flag.Parse()

	//TODO: switch to go-flags or similar
	if *pgTable == "" || *dir == "" {
		flag.Usage()
		os.Exit(1)
	}

	schemaName := "public"
	tableParts := strings.Split(*pgTable, ".")
	switch len(tableParts) {
	case 2:
		schemaName, tableName = tableParts[0], tableParts[1]
	case 1:
		tableName = tableParts[0]
	default:
		log.Fatalf("invalid table name")
	}

	config := pgx.ConnConfig{
		Database: *pgDbname,
		User:     *pgUser,
		Port:     uint16(*pgPort),
		Password: *pgPass,
		Host:     *pgHost}
	r := logicalrestore.New(schemaName, tableName, *dir, config)

	if err := r.Restore(); err != nil {
		log.Fatalf("could not restore table: %v", err)
	}
}
