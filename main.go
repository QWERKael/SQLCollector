package main

import (
	"SQLCollector/handler"
	"SQLCollector/util"
	"fmt"
	"github.com/QWERKael/utility-go/codec"
	"github.com/go-mysql-org/go-mysql/server"
	"net"
	"time"
)

type RemoteThrottleProvider struct {
	*server.InMemoryProvider
	delay int // in milliseconds
}

func (m *RemoteThrottleProvider) GetCredential(username string) (password string, found bool, err error) {
	time.Sleep(time.Millisecond * time.Duration(m.delay))
	return m.InMemoryProvider.GetCredential(username)
}

func main() {
	util.ServerConfig, _ = codec.DecodeIniAllSection(util.ServerConfigPath)
	util.DBConfig, _ = codec.DecodeIniAllSection(util.DBConfigPath)
	l, _ := net.Listen("tcp", util.ServerConfig["server"]["addr"])
	util.SugarLogger.Infof("listening for %s", util.ServerConfig["server"]["addr"])
	// user either the in-memory credential provider or the remote credential provider (you can implement your own)
	//inMemProvider := server.NewInMemoryProvider()
	//inMemProvider.AddUser("root", "123")

	remoteProvider := &RemoteThrottleProvider{server.NewInMemoryProvider(), 10 + 50}
	remoteProvider.AddUser(util.ServerConfig["server"]["user"], util.ServerConfig["server"]["password"])
	//var tlsConf = server.NewServerTLSConfig(test_keys.CaPem, test_keys.CertPem, test_keys.KeyPem, tls.VerifyClientCertIfGiven)
	for {
		c, _ := l.Accept()
		go func() {
			// Create a connection with user root and an empty password.
			// You can use your own handler to handle command here.
			svr := server.NewDefaultServer()
			h := handler.NewHandler()
			for region, section := range util.DBConfig {
				if region == "DEFAULT" {
					continue
				}
				err := h.AddConnect(region, fmt.Sprintf("%s:%s", section["host"], section["port"]), section["user"], section["password"], section["database"])
				if err != nil {
					util.SugarLogger.Errorf("添加[%s]连接失败：%s", region, err.Error())
				}
			}
			conn, err := server.NewCustomizedConn(c, svr, remoteProvider, &h)

			if err != nil {
				util.SugarLogger.Errorf("Connection error: %v", err)
				return
			}

			for {
				err = conn.HandleCommand()
				if err != nil {
					util.SugarLogger.Errorf(`Could not handle command: %v`, err)
					return
				}
			}
		}()
	}
}
