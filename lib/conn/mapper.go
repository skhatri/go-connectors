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
	username := ""
	secret := ""
	if usr, ok := opts["username"]; ok {
		username = fmt.Sprintf("%v", usr)
	}
	if scr, ok := opts["password"]; ok {
		secretEntry, secretErr := fs.ParsePasswordEntry(scr.(string))
		if secretErr != nil {
			LOG.WithTask("read-file-data").Fatalf("%s", "could not read password file")
		}
		secret = secretEntry
	}

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
