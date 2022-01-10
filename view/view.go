package view

import (
	"bytes"
	"fmt"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/format"
	_ "github.com/pingcap/parser/test_driver"
)

func parse(sql string) (*ast.StmtNode, error) {
	p := parser.New()

	stmtNodes, _, err := p.Parse(sql, "", "")
	if err != nil {
		return nil, err
	}

	return &stmtNodes[0], nil
}

type Views struct {
	ViewMap map[string]*View
}

func NewViews() *Views {
	return &Views{ViewMap: make(map[string]*View)}
}

func (vs *Views) Add(name, sql string, source *ast.TableSource) error {
	if source == nil {
		tempSQL := fmt.Sprintf("select 1 from (%s) as %s", sql, name)
		node, err := parse(tempSQL)
		if err != nil {
			return err
		}
		ss, ok := (*node).(*ast.SelectStmt)
		if !ok {
			return fmt.Errorf("添加的SQL不是SELECT语句")
		}
		source = ss.From.TableRefs.Left.(*ast.TableSource)
	}
	vs.ViewMap[name] = &View{
		Name:        name,
		SQL:         sql,
		TableSource: source,
	}
	return nil
}

func (vs *Views) Enter(in ast.Node) (ast.Node, bool) {
	if tableSource, ok := in.(*ast.TableSource); ok {
		if tableName, ok := tableSource.Source.(*ast.TableName); ok {
			if view, ok := vs.ViewMap[tableName.Name.O]; ok {
				return view.TableSource, false
			}
		}
	}
	return in, false
}

func (vs *Views) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}

type View struct {
	Name        string
	SQL         string
	TableSource *ast.TableSource
}

func ReplaceViews(originSQL string, views *Views) (string, error) {

	astNode, err := parse(originSQL)

	(*astNode).Accept(views)
	fmt.Println("解析结束")

	buf := new(bytes.Buffer)
	restoreCtx := format.NewRestoreCtx(format.RestoreKeyWordUppercase|format.RestoreNameBackQuotes, buf)
	err = (*astNode).Restore(restoreCtx)
	if nil != err {
		return "", err
	}
	return buf.String(), nil
}
