package pg

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"github.com/skhatri/go-connectors/lib/conn"
	"github.com/skhatri/go-logger/logging"
)

var log = logging.NewLogger("pg-operations")

func parseConnectionOptions(opts map[string]interface{}) (*conn.ConnectionParameters, string) {
	if _, ok := opts["database"]; !ok {
		opts["database"] = "postgres"
	}
	connParams := conn.ParseParameters(opts, 5432)
	certs := ""
	sslmode := "disable"
	if connParams.Ssl {
		sslmode = "verify-full"
		certs = " "
		if keyfile, ok := opts["keyfile"]; ok {
			certs = fmt.Sprintf("%s sslkey=%v", certs, keyfile)
		}
		if certfile, ok := opts["certfile"]; ok {
			certs = fmt.Sprintf("%s sslcert=%v", certs, certfile)
		}
		if cafile, ok := opts["cafile"]; ok {
			certs = fmt.Sprintf("%s sslrootcert=%v", certs, cafile)
		}
	}
	connStr := fmt.Sprintf("user=%s password=%s host=%s port=%d sslmode=%s dbname=%s%s",
		connParams.Username, connParams.Password, connParams.Host, connParams.Port, sslmode, connParams.Database, certs)
	return connParams, connStr
}

func OpenSession(opts map[string]interface{}) (*DbSession, error) {
	_, connStr := parseConnectionOptions(opts)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.WithTask("open-db-connection").WithMessage("could not open db connection").WithError(err)
		return nil, err
	}
	return &DbSession{
		db: db,
	}, nil
}

type DbSession struct {
	db *sql.DB
}

func (ds *DbSession) Execute(command string, args ...any) (int64, error) {
	var result sql.Result
	var err error
	if len(args) > 0 {
		result, err = ds.db.Exec(command, args...)
	} else {
		result, err = ds.db.Exec(command)
	}
	if err != nil {
		log.WithTask("execute-db-command").WithMessage("could not execute command").WithError(err)
		return 0, err
	}
	return result.RowsAffected()
}

func (ds *DbSession) Query(query string, rowMapper func(*sql.Rows) error, args ...any) error {
	var rows *sql.Rows
	var rerr error
	if len(args) > 0 {
		rows, rerr = ds.db.Query(query, args...)
	} else {
		rows, rerr = ds.db.Query(query)
	}
	if rerr != nil {
		log.WithTask("query-schema").WithMessage("could not query for schema data").WithError(rerr)
		return rerr
	}
	for rows.Next() {
		rerr := rowMapper(rows)
		if rerr != nil {
			return rerr
		}
	}
	return nil
}

func (ds *DbSession) Close() {
	err := ds.db.Close()
	if err != nil {
		return
	}
}
