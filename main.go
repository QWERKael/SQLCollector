package main

import (
	"SQLCollector/handler"
	"SQLCollector/structs"
	"SQLCollector/util"
	"github.com/go-mysql-org/go-mysql/server"
	"net"
	"strings"
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
	//加载配置文件
	var err error
	util.SugarLogger.Debugf("加载配置文件：%s", util.ConfigPath)
	util.Config, err = util.ParseConfigFromToml(util.ConfigPath)
	if err != nil {
		util.SugarLogger.Errorf("加载配置文件错误：%s", err.Error())
	}
	//开始监听
	l, _ := net.Listen("tcp", util.Config.Server.Addr)
	util.SugarLogger.Infof("listening for %s", util.Config.Server.Addr)
	whiteSet := structs.NewSet(util.Config.Server.WhiteList)
	util.SugarLogger.Debugf("白名单列表：%s", whiteSet.ToStringList())
	// user either the in-memory credential provider or the remote credential provider (you can implement your own)
	//inMemProvider := server.NewInMemoryProvider()
	//inMemProvider.AddUser("root", "123")

	remoteProvider := &RemoteThrottleProvider{server.NewInMemoryProvider(), 10 + 50}
	remoteProvider.AddUser(util.Config.Server.User, util.Config.Server.Password)
	//var tlsConf = server.NewServerTLSConfig(test_keys.CaPem, test_keys.CertPem, test_keys.KeyPem, tls.VerifyClientCertIfGiven)
	for {
		c, _ := l.Accept()
		remoteAddr := c.RemoteAddr().String()
		util.SugarLogger.Infof("接受到【%s】的连接", remoteAddr)
		remoteIp := strings.Split(remoteAddr, ":")[0]
		if !whiteSet.Exists(remoteIp) {
			c.Close()
			continue
		}
		go func() {
			// Create a connection with user root and an empty password.
			// You can use your own handler to handle command here.
			svr := server.NewDefaultServer()
			h := handler.NewHandler()
			for _, source := range util.Config.Source {
				err := h.AddConnect(source)
				if err != nil {
					util.SugarLogger.Errorf("添加[%s]连接失败：%s", source.Name, err.Error())
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
