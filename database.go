package stock

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
)

func FormatString(t string) string {
	r := "'"
	r += t
	r += "'"
	return r
}

type DataBase struct {
	db *sql.DB
}

func NewDataBase(user, pass, addr, dbname string) (*DataBase, error) {
	cstr := user + ":" + pass + "@tcp(" + addr + ")/" + dbname + "?charset=utf8"
	db, err := sql.Open("mysql", cstr)
	if err != nil {
		return nil, err
	}
	r := &DataBase{
		db: db,
	}
	return r, nil
}

func (s *DataBase) Close() {
	s.db.Close()
}

//检查sql是否具有返回值
func (s *DataBase) IsSqlHaveResult(sql string) (bool, error) {
	fmt.Println(sql)
	hr, err := s.db.Query(sql)
	if err != nil {
		return false, err
	}
	return hr.Next(), nil
}

//执行sql语句,返回受影响的行数
func (s *DataBase) ExeSql(sql string) (int64, error) {
	hr, err := s.db.Exec(sql)
	if err != nil {
		return 0, err
	}
	return hr.RowsAffected()
}

//执行查询语句，返回执行结果
func (s *DataBase) Query(cmd string) ([]*map[string]string, error) {
	fmt.Println(cmd)
	rows, err := s.db.Query(cmd)
	if err != nil {
		return nil, err
	}
	columns, _ := rows.Columns()
	values := make([]sql.RawBytes, len(columns))
	scanargs := make([]interface{}, len(values))
	for i := range values {
		scanargs[i] = &values[i]
	}

	n := 0
	result := make(map[int]*map[string]string)
	for rows.Next() {
		m := make(map[string]string)

		err := rows.Scan(scanargs...)

		if err != nil {
			return nil, err
		}
		for i, v := range values {
			m[columns[i]] = string(v)
		}
		result[n] = &m
		n++
	}

	ret := make([]*map[string]string, len(result), len(result))
	for i, v := range result {
		ret[i] = v
	}
	return ret, nil
}
