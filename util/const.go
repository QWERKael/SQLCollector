package util

import (
	"flag"
)

var (
	LogLevel   string
	LogPath    string
	ConfigPath string
	WithSource bool
	Config     *Conf
)

func init() {
	flag.StringVar(&LogLevel, "lvl", "info", "log level: debug, info, warn, error, dpanic, panic, fatal")
	flag.StringVar(&LogPath, "log", "sqlcollector.log", "the path of log")
	flag.StringVar(&ConfigPath, "conf", "db.toml", "server configuration")
	flag.BoolVar(&WithSource, "with-source", true, "show source in result set")
	flag.Parse()
}
