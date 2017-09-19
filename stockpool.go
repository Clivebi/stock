package stock

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
)

type StockBaseData struct {
	symbol string
	name   string
	mktcap float64
	nmc    float64
}

func (o StockBaseData) String() string {
	text := o.symbol
	text += ","
	text += o.name
	text += ","
	text += strconv.FormatFloat(o.mktcap, 'f', 4, 64)
	text += ","
	text += strconv.FormatFloat(o.nmc, 'f', 4, 64)
	return text
}

func (o StockBaseData) StringSql() string {
	text := FormatString(o.symbol)
	text += ","
	text += FormatString(o.name)
	text += ","
	text += strconv.FormatFloat(o.mktcap, 'f', 4, 64)
	text += ","
	text += strconv.FormatFloat(o.nmc, 'f', 4, 64)
	return text
}

type StockBasePool struct {
	conf *Config
}

func (o *StockBasePool) EnumStockFromNetwork(Page, Size int) (*[]StockBaseData, error) {
	url := "http://money.finance.sina.com.cn/d/api/openapi_proxy.php/?__s=[[\"hq\",\"hs_a\",\"\",0,"
	url += strconv.Itoa(Page)
	url += ","
	url += strconv.Itoa(Size)
	url += "]]"
	buf, err := SendHttpRequest(url, "")
	if err != nil {
		return nil, err
	}
	var root []interface{}
	if err := json.Unmarshal(buf, &root); err != nil {
		return nil, err
	}
	if len(root) != 1 {
		return nil, errors.New("invalid json packets1")
	}
	item, b := root[0].(map[string]interface{})
	if !b {
		return nil, errors.New("invalid json packets2")
	}
	items, b := item["items"].([]interface{})
	if !b {
		return nil, errors.New("invalid json packets5")
	}
	if len(items) == 0 {
		return nil, errors.New("invalid json packets6")
	}
	Ret := make([]StockBaseData, 0, Size)
	for _, v := range items {
		if r, b := v.([]interface{}); b {
			if len(r) < 21 {
				continue
			}
			var data StockBaseData
			if r2, b2 := r[0].(string); b2 {
				data.symbol = r2
			}
			if r2, b2 := r[2].(string); b2 {
				data.name = r2
			}
			if r2, b2 := r[19].(float64); b2 {
				data.mktcap = (r2 / 10000)
			}
			if r2, b2 := r[20].(float64); b2 {
				data.nmc = (r2 / 10000)
			}
			Ret = append(Ret, data)
		}
	}
	return &Ret, nil
}

func (o *StockBasePool) UpdateStockToDataBase(values []StockBaseData) {
	db, err := NewDataBase(o.conf.DB_user, o.conf.DB_pass, o.conf.DB_addr, o.conf.DB_name)
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, v := range values {
		sql := "replace into `stockbase`(symbol,name,mktcap,nmc) VALUES("
		sql += v.StringSql()
		sql += ")"
		//
		db.ExeSql(sql)
	}
}

func (o *StockBasePool) UpdateThread(PageStart, PageEnd, Size int) {
	for i := PageStart; i < PageEnd; i++ {
		ob, err := o.EnumStockFromNetwork(i, Size)
		if err != nil {
			fmt.Println(err)
		} else {
			o.UpdateStockToDataBase(*ob)
			if len(*ob) < Size {
				break
			}
		}
	}
}

type Config struct {
	DB_addr string `json:"db_addr"`
	DB_user string `json:"db_user"`
	DB_pass string `json:"db_pass"`
	DB_name string `json:"db_name"`
}

func NewBaseTask(conf *Config) *StockBasePool {
	ret := &StockBasePool{
		conf: conf,
	}
	return ret
}
