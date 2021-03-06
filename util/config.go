package util

import (
	"github.com/BurntSushi/toml"
	"github.com/QWERKael/utility-go/io"
)

type Conf struct {
	Server    ServerConf      `toml:"server"`
	Source    []SourceConf    `toml:"source"`
	Group     []GroupConf     `toml:"group"`
	Decorator []DecoratorConf `toml:"decorator"`
}

type ServerConf struct {
	Addr           string   `toml:"addr"`
	User           string   `toml:"user"`
	Password       string   `toml:"password"`
	WhiteList      []string `toml:"whitelist"`
	Includes       []string `toml:"includes"`
	LuaPackagePath string   `toml:"lua_package_path"`
}

type SourceConf struct {
	Name     string     `toml:"name"`
	Type     string     `toml:"type"`
	Host     string     `toml:"host"`
	Port     int        `toml:"port"`
	User     string     `toml:"user"`
	Password string     `toml:"password"`
	Database string     `toml:"database"`
	View     []ViewConf `toml:"view"`
}

type ViewConf struct {
	Name string `toml:"name"`
	SQL  string `toml:"sql"`
}

type GroupConf struct {
	Name       string   `toml:"name"`
	SourceList []string `toml:"sourcelist"`
}

type DecoratorConf struct {
	Name string            `toml:"name"`
	Path string            `toml:"path"`
	Func string            `toml:"func"`
	NRet int               `toml:"NRet"`
	Args map[string]string `toml:"args"`
}

func ParseConfigFromToml(path string) (*Conf, error) {
	b, err := io.ReadFile(path)
	if err != nil {
		return nil, err
	}
	cfg := &Conf{}
	err = toml.Unmarshal(b, cfg)
	if err != nil {
		return nil, err
	}

	if len(cfg.Server.Includes) > 0 {
		for _, include := range cfg.Server.Includes {
			ib, err := io.ReadFile(include)
			if err != nil {
				return nil, err
			}
			b = append(b, []byte("\n")...)
			b = append(b, ib...)
		}
		cfg = &Conf{}
		err = toml.Unmarshal(b, cfg)
		if err != nil {
			return nil, err
		}
	}
	return cfg, nil
}
