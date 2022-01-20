package handler

import (
	"SQLCollector/structs"
	"SQLCollector/util"
	"SQLCollector/view"
	"fmt"
	qsql "github.com/QWERKael/utility-go/database/mysql"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/siddontang/go-log/log"
	"strings"
)

type DataSource struct {
	Name  string
	Conn  *qsql.Connector
	Views *view.Views
}

func NewHandler() Handler {
	h := Handler{
		Connecting:   make([]string, 0),
		ConnectNames: make([]string, 0),
		ConnectPool:  make(map[string]DataSource, 0),
	}
	return h
}

type Handler struct {
	Connecting   []string
	ConnectNames []string
	ConnectPool  map[string]DataSource
}

func (h *Handler) AddConnect(source util.SourceConf) error {
	//connect, err := client.Connect(addr, user, password, dbName)
	connect := &qsql.Connector{}
	err := connect.Connect(source.User, source.Password, "tcp", source.Host, source.Port, source.Database)
	if err != nil {
		return err
	}
	views := view.NewViews()
	for _, viewConf := range source.View {
		err := views.Add(viewConf.Name, viewConf.SQL, nil)
		if nil != err {
			return err
		}
	}
	h.ConnectNames = append(h.ConnectNames, source.Name)
	h.ConnectPool[source.Name] = DataSource{
		Name:  source.Name,
		Conn:  connect,
		Views: views,
	}
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
	util.SugarLogger.Debugf("接受到的查询命令: %s\n", query)
	var rs *mysql.Resultset = nil
	var err error
	names := make([]string, 0)
	values := make([][]interface{}, 0)
	switch query {
	case "show databases":
		fallthrough
	case "show dbs":
		for _, connectName := range h.ConnectNames {
			values = append(values, []interface{}{connectName})
		}
		names = []string{"connects"}
		break
	case "show groups":
		fallthrough
	case "show gps":
		for _, group := range util.Config.Group {
			values = append(values, []interface{}{group.Name, strings.Join(group.SourceList, ", ")})
		}
		names = []string{"group name", "source list"}
		break
	case "show views":
		using := structs.NewSet(h.Connecting)
		for _, sourceConf := range util.Config.Source {
			if using.Exists(sourceConf.Name) {
				for _, viewConf := range sourceConf.View {
					values = append(values, []interface{}{sourceConf.Name, viewConf.Name, viewConf.SQL})
				}
			}
		}
		names = []string{"source", "name", "description"}
		break
	case "show using":
		for _, connectName := range h.Connecting {
			values = append(values, []interface{}{connectName})
		}
		names = []string{"connecting"}
		break
	}

	if len(names) > 0 && len(values) > 0 {
		rs, err = mysql.BuildSimpleTextResultset(names, values)
	}
	if err != nil {
		return nil, err
	}
	if rs != nil {
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
