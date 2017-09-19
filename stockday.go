package stock

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
)

const (
	INVALID_INDEX int = 1000000
)

type StockDayData struct {
	symbol      string
	pct         float64
	agvprice    float64
	trade       float64
	changeratio float64
	turnover    float64
	netamount   float64
	ratioamount float64
	r0_net      float64
	r0_ratio    float64
	r0x_ratio   float64
}

func (o StockDayData) String() string {
	text := o.symbol
	text += ","
	text += strconv.FormatFloat(o.pct, 'f', 4, 64)
	text += ","
	text += strconv.FormatFloat(o.agvprice, 'f', 4, 64)
	text += ","
	text += strconv.FormatFloat(o.trade, 'f', 4, 64)
	text += ","
	text += strconv.FormatFloat(o.changeratio, 'f', 4, 64)
	text += ","
	text += strconv.FormatFloat(o.turnover, 'f', 4, 64)
	text += ","
	text += strconv.FormatFloat(o.netamount, 'f', 4, 64)
	text += ","
	text += strconv.FormatFloat(o.ratioamount, 'f', 4, 64)
	text += ","
	text += strconv.FormatFloat(o.r0_net, 'f', 4, 64)
	text += ","
	text += strconv.FormatFloat(o.r0_ratio, 'f', 4, 64)
	text += ","
	text += strconv.FormatFloat(o.r0x_ratio, 'f', 4, 64)
	return text
}

func (o StockDayData) StringSql() string {
	text := FormatString(o.symbol)
	text += ","
	text += strconv.FormatFloat(o.pct, 'f', 4, 64)
	text += ","
	text += strconv.FormatFloat(o.agvprice, 'f', 4, 64)
	text += ","
	text += strconv.FormatFloat(o.trade, 'f', 4, 64)
	text += ","
	text += strconv.FormatFloat(o.changeratio, 'f', 4, 64)
	text += ","
	text += strconv.FormatFloat(o.turnover, 'f', 4, 64)
	text += ","
	text += strconv.FormatFloat(o.netamount, 'f', 4, 64)
	text += ","
	text += strconv.FormatFloat(o.ratioamount, 'f', 4, 64)
	text += ","
	text += strconv.FormatFloat(o.r0_net, 'f', 4, 64)
	text += ","
	text += strconv.FormatFloat(o.r0_ratio, 'f', 4, 64)
	text += ","
	text += strconv.FormatFloat(o.r0x_ratio, 'f', 4, 64)
	return text
}

type StockDayPool struct {
	conf *Config
}

func (o *StockDayPool) ParseJsObject(js string) []map[string]string {
	t := js[1 : len(js)-1]

	arrays := strings.Split(t, "},{")
	Ret := make([]map[string]string, 0, len(arrays))

	for _, v := range arrays {
		v = strings.Replace(v, "\"", "", -1)
		v = strings.Replace(v, "}", "", -1)
		v = strings.Replace(v, "{", "", -1)
		kv := make(map[string]string)
		values := strings.Split(v, ",")
		for _, v2 := range values {
			x := strings.Split(v2, ":")
			if len(x) >= 2 {
				kv[x[0]] = x[1]
			}
		}
		if len(kv) > 0 {
			Ret = append(Ret, kv)
		}
	}
	return Ret
}

func (o *StockDayPool) GetStockFromNetwork(symbol, date string) (*StockDayData, error) {
	data := &StockDayData{}
	if err := o.GetStockFromNetwork1(symbol, date, data); err != nil {
		return nil, err
	}
	if err := o.GetStockFromNetwork2(symbol, data); err != nil {
		return nil, err
	}
	if data.pct == 0 || data.trade == 0 {
		data = &StockDayData{}
		data.symbol = symbol
	}
	return data, nil
}

func (o *StockDayPool) GetStockFromNetwork1(symbol, date string, data *StockDayData) error {
	url := "http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_Bill.GetBillSum?symbol="
	url += symbol
	url += "&num=60&sort=ticktime&asc=0&volume=40000&amount=0&type=0&day="
	url += date
	buf, err := SendHttpRequest(url, "")
	if err != nil {
		return err
	}
	items := o.ParseJsObject(string(buf))
	if len(items) == 0 {
		return errors.New("invalid text : " + string(buf))
	}
	item := items[0]
	data.pct, _ = strconv.ParseFloat(item["totalvolpct"], 4)
	data.agvprice, _ = strconv.ParseFloat(item["avgprice"], 4)
	data.symbol = symbol
	return nil
}

func (o *StockDayPool) GetStockFromNetwork2(symbol, date string, data *StockDayData) error {
	url := "http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/MoneyFlow.ssl_qsfx_zjlrqs?page=4&num=2&sort=opendate&asc=0&daima="
	url += symbol
	buf, err := SendHttpRequest(url, "")
	if err != nil {
		return err
	}
	items := o.ParseJsObject(string(buf))
	if len(items) == 0 {
		return errors.New("invalid text : " + string(buf))
	}
	index := INVALID_INDEX
	for i, v := range items {
		if v["opendate"] == date {
			index = i
			break
		}
	}
	if index == INVALID_INDEX {
		data.trade = 0
		return nil
	}
	item := items[index]

	data.trade, _ = strconv.ParseFloat(item["trade"], 4)
	data.changeratio, _ = strconv.ParseFloat(item["changeratio"], 4)
	data.turnover, _ = strconv.ParseFloat(item["turnover"], 4)
	data.netamount, _ = strconv.ParseFloat(item["netamount"], 4)
	data.ratioamount, _ = strconv.ParseFloat(item["ratioamount"], 4)
	data.r0_net, _ = strconv.ParseFloat(item["r0_net"], 4)
	data.r0_ratio, _ = strconv.ParseFloat(item["r0_ratio"], 4)
	data.r0x_ratio, _ = strconv.ParseFloat(item["r0x_ratio"], 4)
	return nil
}

func (o *StockDayPool) UpdateStockToDataBase(values []StockDayData, date string) {
	db, err := NewDataBase(o.conf.DB_user, o.conf.DB_pass, o.conf.DB_addr, o.conf.DB_name)
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, v := range values {
		sql := "DELETE FROM `stockday` WHERE date=" + FormatString(date) + " AND symbol=" + FormatString(v.symbol)
		db.ExeSql(sql)
		sql = "INSERT INTO `stockday`(date,symbol,totalvolpct,avgprice,trade,changeratio,turnover,netamount,ratioamount,r0_net,r0_ratio,r0x_ratio) VALUES("
		sql += FormatString(date)
		sql += ","
		sql += v.StringSql()
		sql += ")"
		db.ExeSql(sql)
	}
}

func (o *StockDayPool) updateThread(wait *sync.WaitGroup, symbols []string, date string) {
	List := make([]StockDayData, 0, len(symbols))
	for _, v := range symbols {
		if r, err := o.GetStockFromNetwork(v, date); err == nil {
			List = append(List, *r)
		} else {
			fmt.Println(err)
		}
	}
	o.UpdateStockToDataBase(List, date)
	if wait != nil {
		wait.Done()
	}
}

func (o *StockDayPool) UpdateWithMuiltThread(symbols []string, date string, threadcount int) {
	wait := &sync.WaitGroup{}
	xArray := make([][]string, 0, threadcount)
	for i := 0; i < threadcount; i++ {
		xArray = append(xArray, make([]string, 0, 4000/threadcount))
	}
	for i, v := range symbols {
		xArray[i%threadcount] = append(xArray[i%threadcount], v)
	}
	for i := 0; i < threadcount; i++ {
		go o.updateThread(wait, xArray[i], date)
		wait.Add(1)
	}
	wait.Wait()
}

func NewDayTask(conf *Config) *StockDayPool {
	ret := &StockDayPool{
		conf: conf,
	}
	return ret
}
