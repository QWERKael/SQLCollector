package handler

import (
	"SQLCollector/util"
	"errors"
	"fmt"
	qsql "github.com/QWERKael/utility-go/database/mysql"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/siddontang/go-log/log"
	"strconv"
	"strings"
	"sync"
)

func NewHandler() Handler {
	h := Handler{
		Connecting:   make([]string, 0),
		ConnectNames: []string{"all"},
		ConnectPool:  make(map[string]*qsql.Connector, 0),
	}
	return h
}

type Handler struct {
	Connecting   []string
	ConnectNames []string
	ConnectPool  map[string]*qsql.Connector
}

func (h *Handler) AddConnect(region, host, port, user, password, dbName string) error {
	if region == "all" {
		h.ConnectNames = append(h.ConnectNames, region)
		return nil
	}
	//connect, err := client.Connect(addr, user, password, dbName)
	connect := &qsql.Connector{}
	portInt, err := strconv.Atoi(port)
	if err != nil {
		util.SugarLogger.Errorf("端口错误: %s\n", err.Error())
		return err
	}
	err = connect.Connect(user, password, "tcp", host, portInt, dbName)
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

type queryResult struct {
	db     string
	names  []string
	values []map[string]string
	err    error
}

func (h *Handler) Query(query string, withSource bool) (*mysql.Result, error) {
	//进行聚合查询
	util.SugarLogger.Debugf("开始进行聚合查询：%s", query)
	collectNames := make([]string, 0)
	collectValues := make([][]interface{}, 0)
	queryResultChannel := make(chan *queryResult, len(h.Connecting))

	var wg sync.WaitGroup
	for _, db := range h.Connecting {
		util.SugarLogger.Debugf("发布查询任务到：%s", db)
		wg.Add(1)
		go func(db string) {
			util.SugarLogger.Debugf("查询数据库：%s", db)
			r, names, err := h.ConnectPool[db].QueryAsMapStringListWithColNames(query)
			if err != nil {
				util.SugarLogger.Errorf("当前连接[%s]查询出来的结果报错：%s", db, err.Error())
				queryResultChannel <- &queryResult{
					db:     db,
					names:  nil,
					values: nil,
					err:    err,
				}
				return
			}
			//如果当前连接查询出来的结果是空集，则跳过
			if len(r) == 0 {
				util.SugarLogger.Debugf("当前连接[%s]查询出来的结果是空集，跳过", db)
				wg.Done()
				return
			}
			queryResultChannel <- &queryResult{
				db:     db,
				names:  names,
				values: r,
				err:    nil,
			}
			return
		}(db)
	}
	util.SugarLogger.Debugf("查询任务已发布")

	var namesErr error
	namesErr = nil
	errStr := ""
	catchErr := false
	go func() {
		util.SugarLogger.Debugf("开始组装任务")
		length := 0
		var collectNamesSet *util.Set
		for qr := range queryResultChannel {
			util.SugarLogger.Debugf("开始组装[%s]数据", qr.db)
			//校验是否有报错
			if qr.err != nil {
				util.SugarLogger.Errorf("当前连接[%s]查询出来的结果报错：%s", qr.db, qr.err.Error())
				errStr += fmt.Sprintf("[%s] get query error: %s\n", qr.db, qr.err.Error())
				catchErr = true
			}
			//只要一组数据中有一个报错，后续数据就不再组装，只尽可能多的收集错误信息
			if catchErr {
				util.SugarLogger.Debugf("本次查询出现错误，跳过数据组装")
				wg.Done()
				continue
			}
			//初始化表头，或者校验表头
			namesSet := util.NewSet(qr.names)
			if namesSet == nil {
				util.SugarLogger.Debugf("[%s]表头为空", qr.db)
				wg.Done()
				continue
			}
			if collectNamesSet == nil {
				collectNames = qr.names
				collectNamesSet = namesSet
				length = len(collectNames)
			}
			if length != len(qr.names) || !collectNamesSet.Equal(namesSet) {
				util.SugarLogger.Errorf("可能存在异构表结构: \n%d != %d\n或者\n%#v\n%#v", length, len(qr.names), collectNamesSet, namesSet)
				namesErr = errors.New(fmt.Sprintf("可能存在异构表结构: \n%d != %d\n或者\n%#v\n%#v", length, len(qr.names), collectNamesSet, namesSet))
				wg.Done()
				continue
			}
			//拼接数据
			for _, row := range qr.values {
				collectRow := make([]interface{}, 0)
				if withSource {
					collectRow = []interface{}{qr.db}
				}
				for _, name := range collectNames {
					cell := row[name]
					if cell == "" {
						cell = " "
					}
					collectRow = append(collectRow, cell)
					util.SugarLogger.Debugf("组装collectRow:%s:%s", name, cell)
				}
				collectValues = append(collectValues, collectRow)
			}
			util.SugarLogger.Debugf("数据源[%s]的数据已组装完成", qr.db)
			wg.Done()
		}
	}()

	wg.Wait()
	if catchErr {
		return nil, errors.New(errStr)
	}

	if namesErr != nil {
		util.SugarLogger.Debugf("组装数据报错: %s", namesErr.Error())
		return nil, namesErr
	}
	util.SugarLogger.Debugf("所有数据源的数据都已组装完成")
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
