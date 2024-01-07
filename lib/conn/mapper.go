package conn

import (
	"fmt"
	"github.com/skhatri/go-fns/lib/fs"
	"github.com/skhatri/go-logger/logging"
	"strconv"
)

var LOG = logging.NewLogger("conn-parse")

type ConnectionParameters struct {
	Username string
	Password string
	Host     string
	Port     int
	Ssl      bool
	Database string
}

func ParseParameters(opts map[string]interface{}, defaultPort int) *ConnectionParameters {
	dbname := ""
	if dname, ok := opts["database"]; ok {
		dbname = dname.(string)
	}
	ssl := false
	if sslFlag, ok := opts["ssl"]; ok && fmt.Sprintf("%v", sslFlag) == "true" {
		ssl = true
	}
	username := readMaybeFromFile(opts, "username")
	secret := readMaybeFromFile(opts, "password")

	host := fmt.Sprintf("%v", opts["host"])

	port := defaultPort
	if pValue, ok := opts["port"]; ok {
		pt, portErr := strconv.Atoi(fmt.Sprintf("%v", pValue))
		if portErr == nil {
			if pt > 0 && pt <= 65535 {
				port = pt
			}
		}
	}

	return &ConnectionParameters{
		Username: username,
		Password: secret,
		Host:     host,
		Port:     port,
		Ssl:      ssl,
		Database: dbname,
	}
}

func readMaybeFromFile(opts map[string]interface{}, key string) string {
	result := ""
	if scr, ok := opts[key]; ok {
		value, err := fs.ParsePasswordEntry(scr.(string))
		if err != nil {
			LOG.WithTask("read-file-data").Fatalf("%s", "could not read value from file")
		}
		result = value
	}
	return result
}
