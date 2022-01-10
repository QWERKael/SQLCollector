package handler

import (
	"SQLCollector/util"
	"fmt"
	qsql "github.com/QWERKael/utility-go/database/mysql"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/siddontang/go-log/log"
	"strings"
)

func NewHandler() Handler {
	h := Handler{
		Connecting:   make([]string, 0),
		ConnectNames: make([]string, 0),
		ConnectPool:  make(map[string]*qsql.Connector, 0),
	}
	return h
}

type Handler struct {
	Connecting   []string
	ConnectNames []string
	ConnectPool  map[string]*qsql.Connector
}

func (h *Handler) AddConnect(sourceName, host string, port int, user, password, dbName string) error {
	//connect, err := client.Connect(addr, user, password, dbName)
	connect := &qsql.Connector{}
	err := connect.Connect(user, password, "tcp", host, port, dbName)
	if err != nil {
		return err
	}
	h.ConnectNames = append(h.ConnectNames, sourceName)
	h.ConnectPool[sourceName] = connect
	return nil
}

func (h *Handler) UseDB(dbName string) error {
	for _, group := range util.Config.Group {
		if group.Name == dbName {
			dbName = strings.Join(group.SourceList, ",")
		}
	}
	util.SugarLogger.Debugf("use dbName: %s", dbName)
	var dbList []string
	for _, db := range strings.Split(dbName, ",") {
		if _, ok := h.ConnectPool[db]; !ok {
			log.Errorf("未知的数据库：%s", db)
			return nil
		}
		dbList = append(dbList, db)
	}
	h.Connecting = dbList
	util.SugarLogger.Debugf("connecting db: %s", strings.Join(dbList, ", "))
	return nil
}
func (h Handler) HandleQuery(query string) (*mysql.Result, error) {
	util.SugarLogger.Debugf("查询命令: %s\n", query)
	switch query {
	case "show databases":
		fallthrough
	case "show dbs":
		values := make([][]interface{}, 0)
		for _, connectName := range h.ConnectNames {
			values = append(values, []interface{}{connectName})
		}
		rs, err := mysql.BuildSimpleTextResultset([]string{"connects"}, values)
		if err != nil {
			return nil, err
		}
		return &mysql.Result{
			Status:       34,
			Warnings:     0,
			InsertId:     0,
			AffectedRows: 0,
			Resultset:    rs,
		}, nil
	case "show groups":
		fallthrough
	case "show gps":
		values := make([][]interface{}, 0)
		for _, group := range util.Config.Group {
			values = append(values, []interface{}{group.Name, strings.Join(group.SourceList, ", ")})
		}
		rs, err := mysql.BuildSimpleTextResultset([]string{"group name", "source list"}, values)
		if err != nil {
			return nil, err
		}
		return &mysql.Result{
			Status:       34,
			Warnings:     0,
			InsertId:     0,
			AffectedRows: 0,
			Resultset:    rs,
		}, nil
	case "show using":
		util.SugarLogger.Debugf("查看正在使用的数据源")
		values := make([][]interface{}, 0)
		for _, connectName := range h.Connecting {
			values = append(values, []interface{}{connectName})
		}
		rs, err := mysql.BuildSimpleTextResultset([]string{"connecting"}, values)
		if err != nil {
			util.SugarLogger.Errorf("查看正在使用的数据源出现错误：%s", err.Error())
			return nil, err
		}
		util.SugarLogger.Debugf("正在使用的数据源有：%s", strings.Join(h.Connecting, ", "))
		util.SugarLogger.Debugf("%#v", rs)
		return &mysql.Result{
			Status:       34,
			Warnings:     0,
			InsertId:     0,
			AffectedRows: 0,
			Resultset:    rs,
		}, nil
	}
	return h.Query(query, util.WithSource)
}

func (h Handler) HandleFieldList(table string, fieldWildcard string) ([]*mysql.Field, error) {
	return nil, fmt.Errorf("not supported now")
}
func (h Handler) HandleStmtPrepare(query string) (int, int, interface{}, error) {
	return 0, 0, nil, fmt.Errorf("not supported now")
}
func (h Handler) HandleStmtExecute(context interface{}, query string, args []interface{}) (*mysql.Result, error) {
	return nil, fmt.Errorf("not supported now")
}

func (h Handler) HandleStmtClose(context interface{}) error {
	return nil
}

func (h Handler) HandleOtherCommand(cmd byte, data []byte) error {
	return mysql.NewError(
		mysql.ER_UNKNOWN_ERROR,
		fmt.Sprintf("command %d is not supported now", cmd),
	)
}
