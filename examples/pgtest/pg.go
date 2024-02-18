package main

import (
	"database/sql"
	"fmt"
	"github.com/skhatri/go-connectors/lib/pg"
	"os"
)

func main() {
	certDir := os.Getenv("CERT_DIR")
	opts := make(map[string]interface{})
	opts["host"] = "postgres"
	opts["username"] = "postgres"
	opts["password"] = "password"
	opts["ssl"] = "true"
	if certDir != "" {
		opts["cafile"] = fmt.Sprintf("%s/ca.crt", certDir)
	}

	fmt.Println("test pg connection")
	session, err := pg.OpenSession(opts)
	if err != nil {
		fmt.Println("error", err)
		return
	}
	defer session.Close()
	session.Query("select schema_name from information_schema.schemata", func(rows *sql.Rows) error {
		if rows.Next() {
			var schemaName string
			rows.Scan(&schemaName)
			fmt.Println(schemaName)
		}
		return nil
	})
}
