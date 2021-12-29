package handler

import (
	"SQLCollector/util"
	"errors"
	"fmt"
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/siddontang/go-log/log"
	"strings"
)

func NewHandler() Handler {
	h := Handler{
		Connecting:   make([]string, 0),
		ConnectNames: []string{"all"},
		ConnectPool:  make(map[string]*client.Conn, 0),
	}
	return h
}

type Handler struct {
	Connecting   []string
	ConnectNames []string
	ConnectPool  map[string]*client.Conn
}

func (h *Handler) AddConnect(region, addr, user, password, dbName string) error {
	if region == "all" {
		h.ConnectNames = append(h.ConnectNames, region)
		return nil
	}
	connect, err := client.Connect(addr, user, password, dbName)
	if err != nil {
		return err
	}
	h.ConnectNames = append(h.ConnectNames, region)
	h.ConnectPool[region] = connect
	return nil
}

func (h *Handler) UseDB(dbName string) error {
	if dbName == "all" {
		dbName = strings.Join(h.ConnectNames, ",")
	}
	util.SugarLogger.Debugf("use dbName: %s", dbName)
	var dbList []string
	for _, db := range strings.Split(dbName, ",") {
		if db == "all" {
			continue
		}
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

func row2interfaces(fvs []mysql.FieldValue, source *string) []interface{} {
	rowInterface := make([]interface{}, 0)
	if source != nil {
		rowInterface = append(rowInterface, *source)
	}
	for _, fv := range fvs {
		switch fv.Type {
		case mysql.FieldValueTypeUnsigned:
			rowInterface = append(rowInterface, fmt.Sprintf("%d", fv.AsUint64()))
		case mysql.FieldValueTypeSigned:
			rowInterface = append(rowInterface, fmt.Sprintf("%d", fv.AsInt64()))
		case mysql.FieldValueTypeFloat:
			rowInterface = append(rowInterface, fmt.Sprintf("%f", fv.AsFloat64()))
		case mysql.FieldValueTypeString:
			rowInterface = append(rowInterface, fmt.Sprintf("%s", fv.AsString()))
		default: // FieldValueTypeNull
			rowInterface = append(rowInterface, nil)
		}
	}
	return rowInterface
}

func (h *Handler) Query(query string, withSource bool) (*mysql.Result, error) {
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
	case "show using":
		values := make([][]interface{}, 0)
		for _, connectName := range h.Connecting {
			values = append(values, []interface{}{connectName})
		}
		rs, err := mysql.BuildSimpleTextResultset([]string{"connecting"}, values)
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
	}
	//进行聚合查询
	collectNames := make([]string, 0)
	collectValues := make([][]interface{}, 0)
	for _, db := range h.Connecting {
		source := db
		util.SugarLogger.Debugf("查询数据库：%s", db)
		r, err := h.ConnectPool[db].Execute(query)
		if err != nil {
			return nil, err
		}

		//如果当前连接查询出来的结果是空集，则跳过
		if len(r.Resultset.Values) == 0 {
			util.SugarLogger.Debugf("当前连接[%s]查询出来的结果是空集，跳过", source)
			continue
		}

		colCount := len(r.Resultset.Values[0])
		names := make([]string, colCount)

		//处理表头
		//如果表头是空的，就初始化表头；否则，校验表头
		for name, i := range r.Resultset.FieldNames {
			util.SugarLogger.Debugf("name: %s\n", name)
			names[i] = name
		}
		if len(collectNames) == 0 {
			collectNames = names
		} else if len(collectNames) != len(names) {
			return nil, errors.New("可能存在异构表结构")
		} else {
			for i, name := range names {
				if collectNames[i] != name {
					return nil, errors.New("可能存在异构表结构")
				}
			}
		}

		//处理数据行
		for _, row := range r.Resultset.Values {
			if withSource {
				collectValues = append(collectValues, row2interfaces(row, &source))
			} else {
				collectValues = append(collectValues, row2interfaces(row, nil))
			}
		}
	}

	if withSource {
		collectNames = append([]string{"source"}, collectNames...)
	}

	//将结果组装成指定格式
	util.SugarLogger.Debugf("开始组装collectValues:\n%#v\n%#v\n", collectNames, collectValues)
	rs, err := mysql.BuildSimpleTextResultset(collectNames, collectValues)
	if err != nil {
		return nil, err
	}
	util.SugarLogger.Debugf("组装完成collectValues\n")
	return &mysql.Result{
		Status:       34,
		Warnings:     0,
		InsertId:     0,
		AffectedRows: 0,
		Resultset:    rs,
	}, nil
}
