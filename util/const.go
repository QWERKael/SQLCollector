package util

import (
	"flag"
)

var (
	LogLevel         string
	LogPath          string
	ServerConfigPath string
	DBConfigPath     string
	ServerConfig     map[string]map[string]string
	DBConfig         map[string]map[string]string
	WithSource       bool
)

func init() {
	flag.StringVar(&LogLevel, "lvl", "info", "log level: debug, info, warn, error, dpanic, panic, fatal")
	flag.StringVar(&LogPath, "log", "sqlcollector.log", "the path of log")
	flag.StringVar(&ServerConfigPath, "conf", "server.conf", "server configuration")
	flag.StringVar(&DBConfigPath, "dbconf", "db.conf", "databases configuration")
	flag.BoolVar(&WithSource, "with-source", true, "show source in result set")
	flag.Parse()
}
