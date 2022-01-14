package structs

import (
	"github.com/QWERKael/utility-go/log"
	lua "github.com/yuin/gopher-lua"
)

type CollectTable struct {
	names []string
	data  []map[string]string
}

func NewCollectTable() *CollectTable {
	names := make([]string, 0)
	data := make([]map[string]string, 0)
	return &CollectTable{names: names, data: data}
}

func (ct *CollectTable) Set(names []string, mapList []map[string]string) {
	ct.names = names
	ct.data = mapList
}

func (ct *CollectTable) GetNames() []string {
	return ct.names
}

func (ct *CollectTable) GetData() []map[string]string {
	return ct.data
}

func (ct *CollectTable) ConvertToLuaTable() *lua.LTable {
	names := ct.GetNames()
	data := ct.GetData()

	lTable := &lua.LTable{}
	lNames := &lua.LTable{}
	lData := &lua.LTable{}
	for i, name := range names {
		lNames.Insert(i, lua.LString(name))
	}
	for i, m := range data {
		subTable := &lua.LTable{}
		for k, v := range m {
			subTable.RawSetString(k, lua.LString(v))
		}
		lData.Insert(i, subTable)
	}
	lTable.RawSetString("names", lNames)
	lTable.RawSetString("data", lData)
	return lTable
}

func (ct *CollectTable) ConvertFromLuaTable(rst *lua.LTable) {
	lNames := rst.RawGet(lua.LString("names")).(*lua.LTable)
	names := make([]string, lNames.Len()+1)
	lNames.ForEach(func(idx, tab lua.LValue) {
		i, _ := idx.(lua.LNumber)
		name, _ := tab.(lua.LString)
		names[int(i)] = string(name)
	})

	lData := rst.RawGet(lua.LString("data")).(*lua.LTable)
	log.SugarLogger.Debugf("Len = %d", lData.Len())
	data := make([]map[string]string, lData.Len()+1)
	lData.ForEach(func(idx, tab lua.LValue) {
		i, _ := idx.(lua.LNumber)
		t, _ := tab.(*lua.LTable)
		log.SugarLogger.Debugf("i = %d", int(i))
		data[int(i)] = make(map[string]string, 0)
		t.ForEach(func(key, value lua.LValue) {
			k := key.String()
			v := value.String()
			log.SugarLogger.Debugf("k = %s", string(k))
			log.SugarLogger.Debugf("v = %s", string(v))
			data[int(i)][string(k)] = string(v)
			log.SugarLogger.Debugf("i = %d 插入完成", int(i))
		})
	})
	ct.Set(names, data)
}

func ConvertStringMapToLuaTable(m map[string]string) *lua.LTable {
	lTable := &lua.LTable{}
	for k, v := range m {
		lTable.RawSetString(k, lua.LString(v))
	}
	return lTable
}
