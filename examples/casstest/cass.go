package main

import (
	"fmt"
	"github.com/gocql/gocql"
	"github.com/skhatri/go-connectors/lib/cassandra"
	"os"
)

func main() {
	opts := make(map[string]interface{})
	opts["host"] = "cassandra"
	opts["username"] = "cassandra"
	opts["password"] = "cassandra"
	opts["ssl"] = "true"
	certDir := os.Getenv("CERT_DIR")
	if certDir != "" {
		opts["cafile"] = fmt.Sprintf("%s/ca.crt", certDir)
	}
	fmt.Println("test cassandra connection")
	_, cluster := cassandra.CreateClusterConfig(opts)
	err := cassandra.Query(cluster, "select keyspace_name from system_schema.keyspaces", func(scn gocql.Scanner) error {
		for scn.Next() {
			var keyspace string
			scn.Scan(&keyspace)
			fmt.Println(keyspace)
		}
		return nil
	})
	if err != nil {
		fmt.Println("error in cassandra test", err)
		return
	}
}
