package cassandra

import (
	"github.com/gocql/gocql"
	"github.com/skhatri/go-connectors/lib/conn"
	"github.com/skhatri/go-logger/logging"

	"time"
)

var logger = logging.NewLogger("cass-operations")

func CreateClusterConfig(opts map[string]interface{}) (*conn.ConnectionParameters, *gocql.ClusterConfig) {

	connParams := conn.ParseParameters(opts, 9042)

	cluster := gocql.NewCluster(connParams.Host)
	cluster.Port = connParams.Port
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: connParams.Username,
		Password: connParams.Password,
	}
	cluster.ConnectTimeout = 10 * time.Second
	if tm, ok := opts["timeout"]; ok {
		timeout, parseErr := time.ParseDuration(tm.(string))
		if parseErr == nil {
			cluster.ConnectTimeout = timeout
		}
	}
	if connParams.Ssl {
		sslOptions := &gocql.SslOptions{}
		if keyFile, ok := opts["keyfile"]; ok {
			sslOptions.KeyPath = keyFile.(string)
		}
		if certFile, ok := opts["certfile"]; ok {
			sslOptions.CertPath = certFile.(string)
		}
		if caFile, ok := opts["cafile"]; ok {
			sslOptions.CaPath = caFile.(string)
		}
		cluster.SslOpts = sslOptions
	}
	return connParams, cluster
}

func Query(cluster *gocql.ClusterConfig, queryStr string, rowMapper func(scanner gocql.Scanner) error, values ...any) error {
	session, err := cluster.CreateSession()
	if err != nil {
		logger.WithTask("create-cassandra-session").WithError(err)
		return err
	}
	defer session.Close()
	query := session.Query(queryStr).Bind(values...)
	scanner := query.Iter().Scanner()
	for scanner.Next() {
		rowErr := rowMapper(scanner)
		if rowErr != nil {
			return rowErr
		}
	}
	return nil
}

func Execute(cluster *gocql.ClusterConfig, command string, values ...any) error {
	session, err := cluster.CreateSession()
	if err != nil {
		logger.WithTask("create-cassandra-session").WithError(err)
		return err
	}
	defer session.Close()
	query := session.Query(command).Bind(values...)
	return query.Exec()
}


