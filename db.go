package oram

import (
	"bytes"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unsafe"
)

type SqlExecutor interface {
	Model(class interface{}) *ConDB
	Table(name string) *ConDB
	Where(query string, values ...interface{}) *ConDB
	Maps(maps map[string]interface{}) *ConDB
	Or(query string, values ...interface{}) *ConDB
	IN(key string, value string) *ConDB
	GroupBy(value string) *ConDB
	Count(agrs ...interface{}) int64
	PageSize(size int32) int32
	Find(out interface{}) *ConDB

	Select(args string) *ConDB
	Sort(key, sort string) *ConDB
	Page(cur, count int32) *ConDB

	Flush(c interface{}) error
	Update(field string, values ...interface{}) error
	Delete(i ...interface{}) error
	Insert(i interface{}) error
	SelectInt(field string) int64
	SelectStr(field string) string
	QueryField(field string, out interface{}) error
	Field(field string) *ConDB

	Get(out interface{}) error
	FindById(out, id interface{}) error
	IsExit() (bool, error)

	TxBegin() *ConDB
	Tx(tx *sql.Tx) *ConDB
	Commit() error
	Rollback() error
	GetForUpdate(out interface{}) error

	Exec(sql string, args ...interface{}) (sql.Result, error)
	QueryRow(query string, args ...interface{}) *sql.Row
	QueryRows(query string, args ...interface{}) (*sql.Rows, error)

	QueryMap(query string, args ...interface{}) (map[string]string, error)
	QueryMaps(query string, args ...interface{}) ([]map[string]string, error)

	List() ([]map[string]string, error)
	Query() (map[string]string, error)
}

var _ SqlExecutor = &ConDB{}

var prefix string = "tb_"

type ConDB struct {
	Db           *sql.DB
	parent       *ConDB
	tx           *sql.Tx
	query        string
	inCondition  string
	params       []interface{}
	Condition    []map[string]interface{}
	OrCondition  []map[string]interface{}
	table        string
	field        string
	Offset       int32
	Limit        int32
	sort         string
	group        string
	Err          error
	Result       sql.Result
	LastInsertId int64
	Idx          int
}

var logger SqlLogger
var logPrefix string

type SqlLogger interface {
	Printf(format string, v ...interface{})
}

func (m *ConDB) TraceOn(prefix string, log SqlLogger) {
	logger = log
	if prefix == "" {
		logPrefix = prefix
	} else {
		logPrefix = fmt.Sprintf("%s ", prefix)
	}
}

// TraceOff turns off tracing. It is idempotent.
func (m *ConDB) TraceOff() {
	logger = nil
	logPrefix = ""
}

func (m *ConDB) trace(query string, args ...interface{}) {
	if logger != nil {
		var margs = argsToStr(args...)
		logger.Printf("%s%s [%s]", logPrefix, query, margs)
	}
}

func (m *ConDB) clone() *ConDB {

	db := &ConDB{Db: m.Db, parent: m, tx: nil, inCondition: "", query: "", table: "", Condition: nil, field: "*", Offset: 0, Limit: 0, sort: "", group: "", Idx: 0}
	return db
}

//ad dbMap new month
func (m *ConDB) Model(class interface{}) *ConDB {

	if m.parent == nil {

		db := m.clone()
		db.table = getTable(class)
		return db
	} else {

		if m.table == "" {
			m.table = getTable(class)
		}
		return m
	}

}
func (m *ConDB) Table(name string) *ConDB {

	if m.parent == nil {
		db := m.clone()
		db.table = name
		return db
	} else {

		m.table = name
		return m
	}
}

func (m *ConDB) Where(query string, values ...interface{}) *ConDB {

	if m.parent == nil {
		db := m.clone()
		db.Condition = append(db.Condition, map[string]interface{}{"query": query, "args": values})
		return db
	} else {

		m.Condition = append(m.Condition, map[string]interface{}{"query": query, "args": values})
		return m
	}
}
func (m *ConDB) Field(field string) *ConDB {

	m.field = field
	return m
}

func (m *ConDB) TxBegin() *ConDB {

	tx, _ := m.Db.Begin()

	if m.parent == nil {
		db := m.clone()
		db.tx = tx
		return db
	} else {

		m.tx = tx
		return m
	}
}
func (m *ConDB) Tx(tx *sql.Tx) *ConDB {

	if m.parent == nil {
		db := m.clone()
		db.tx = tx
		return db
	} else {

		m.tx = tx
		return m
	}
}

func (m *ConDB) Commit() error {

	return m.tx.Commit()
}

func (m *ConDB) Rollback() error {

	return m.tx.Rollback()
}

func (m *ConDB) Maps(maps map[string]interface{}) *ConDB {

	i := 1
	if m.parent == nil {
		db := m.clone()
		if maps != nil && len(maps) > 0 {

			for k, v := range maps {
				if m_type(v) == "string" && v == "" { //忽略空
					continue
				}
				query := fmt.Sprintf("%s=:%d", k, i)
				db.Where(query, v)
				i++
				db.Idx++
			}

		}
		return db
	} else {

		if maps != nil && len(maps) > 0 {

			for k, v := range maps {
				if m_type(v) == "string" && v == "" { //忽略空
					continue
				}
				query := fmt.Sprintf("%s=:%d", k, i)
				m.Where(query, v)
				i++
				m.Idx++
			}
		}
		return m
	}

}

func (db *ConDB) maps(maps map[string]interface{}) *ConDB {

	if db.parent == nil {
		return nil
	}
	i := 0
	s := bytes.Buffer{}
	if maps != nil && len(maps) > 0 {

		for k, v := range maps {

			if m_type(v) == "string" && v == "" { //忽略空

				continue
			}
			if i > 0 {

				s.WriteString(" AND ")
			}
			if m_type(v) == "string" {

				s.WriteString(fmt.Sprintf(" %s='%s' ", k, v))
			} else {

				s.WriteString(fmt.Sprintf(" %s=%v ", k, v))
			}
			i++
		}
	}

	db.query = s.String()

	return db
}

func (db *ConDB) Select(args string) *ConDB {

	if db.parent == nil {
		return nil
	}
	db.field = args
	return db
}

func getName(class interface{}) string {

	str := reflect.TypeOf(class).String()
	//str := fmt.Sprintf("%v", t)

	buff := bytes.NewBuffer([]byte{})

	for pos, char := range str {
		if str[pos] != '*' && str[pos] != '[' && str[pos] != ']' {

			buff.WriteRune(char)
		}
	}

	return buff.String()
}

func m_type(i interface{}) string {
	switch i.(type) {
	case string:
		return "string"
	case int:
		return "number"
	case int32:
		return "number"
	case int64:
		return "number"
	case float64:
		return "number"
	case []string:
		return "strings"
	default:
		return ""
	}

}
func getTable(class interface{}) string {

	var table string
	se := reflect.TypeOf(class).String()
	//se := fmt.Sprintf("%v", ts)

	idx := strings.LastIndex(se, ".")
	if idx > 0 {

		idx++
		ss := string([]rune(se)[idx:len(se)])
		table = strings.ToLower(ss)
	} else {
		table = se
	}

	return prefix + table
}

func (m *ConDB) Flush(c interface{}) error {

	var db *ConDB
	if m.parent == nil {

		db = m.clone()
	} else {
		db = m
	}

	s := bytes.Buffer{}

	s.WriteString("UPDATE ")

	if db.table == "" {

		s.WriteString(getTable(c))
	} else {

		s.WriteString(db.table)
	}

	s.WriteString(" set ")

	val := reflect.ValueOf(c)
	typ := reflect.TypeOf(c)

	_, ins := typ.MethodByName("PreUpdate")
	if ins {
		mv := val.MethodByName("PreUpdate")
		mv.Call(nil)
	}

	data := toMap(val, typ)
	buff := bytes.NewBuffer([]byte{})

	var id interface{}
	idx := 1
	for k, v := range data {

		if k == "id" {
			id = v
			continue
		}
		buff.WriteString(",")
		buff.WriteString(k)
		buff.WriteString(fmt.Sprintf("=:%d ", idx))

		//buff.WriteString(parseString(v))
		db.params = append(db.params, v)
		idx++
	}

	sql := strings.TrimLeft(buff.String(), `,`)

	s.WriteString(sql)
	s.WriteString(" where id =:")
	s.WriteString(fmt.Sprintf("%d ", idx))

	//p := getKey(c, "Id")
	db.params = append(db.params, id)

	db.trace(s.String(), db.params...)

	if db.tx == nil {
		db.Result, db.Err = db.Db.Exec(s.String(), db.params...)

	} else {
		db.Result, db.Err = db.tx.Exec(s.String(), db.params...)

	}

	if db.Err != nil {

		return db.Err
	}

	aff_nums, err := db.Result.RowsAffected()
	if err == nil {
		db.trace("RowsAffected num:", aff_nums)
	} else {
		db.trace("RowsAffected error:%v", err)
	}

	return err

}

func (db *ConDB) Update(field string, values ...interface{}) error {

	if db.parent == nil {

		db.trace("doesn't init ConDB")
		return errors.New("doesn't init ConDB")
	}
	s := bytes.Buffer{}

	s.WriteString("UPDATE ")
	s.WriteString(db.table)
	s.WriteString(" set ")

	s.WriteString(db.Conver(field))

	s.WriteString(db.buildSql())

	params := append(values, db.params...)

	db.trace(s.String(), db.params...)

	if db.tx == nil {
		db.Result, db.Err = db.Db.Exec(s.String(), params...)

	} else {
		db.Result, db.Err = db.tx.Exec(s.String(), params...)

	}

	if db.Err != nil {

		return db.Err
	}

	aff_nums, err := db.Result.RowsAffected()
	if err == nil {
		db.trace("RowsAffected num:", aff_nums)
		if aff_nums == 0 {

			return errors.New("RowsAffected rows is 0")
		}
	} else {
		db.trace("RowsAffected error:%v", err)
	}

	return err

}

func (m *ConDB) Exec(sql string, params ...interface{}) (sql.Result, error) {

	var db *ConDB
	if m.parent == nil {

		db = m.clone()
	} else {
		db = m
	}

	db.trace(sql, params...)

	if db.tx == nil {
		db.Result, db.Err = db.Db.Exec(sql, params...)

	} else {
		db.Result, db.Err = db.tx.Exec(sql, params...)

	}

	return db.Result, db.Err

}

func getType(b interface{}) reflect.Type {

	cType := reflect.TypeOf(b)

	if cType.Kind() == reflect.Slice || cType.Kind() == reflect.Ptr {
		cType = cType.Elem()
	}

	if cType.Kind() == reflect.Slice || cType.Kind() == reflect.Ptr {

		cType = cType.Elem()

	}

	return cType

}
func (db *ConDB) Delete(i ...interface{}) error {

	if len(i) > 0 {

		c := i[0]
		key := reflect.ValueOf(c).Elem().FieldByName("Id")

		if !key.IsValid() {

			db.trace("doesn't found key")
			return errors.New("doesn't found key")
		}
		//db1 := db.clone()
		id := fmt.Sprintf("%d", key)
		return db.Model(c).Where("id=:1", id).delete()

	} else {

		return db.delete()
	}

}

func (db *ConDB) delete() error {

	if db.parent == nil {
		db.trace("doesn't init ConDB,need first get new ConDB")
		return errors.New("doesn't init ConDB,need first get new ConDB")
	}
	if db.table == "" {

		db.trace("no defined table name ")
		return errors.New("not defined table name")
	}
	s := bytes.Buffer{}

	s.WriteString("DELETE  FROM ")
	s.WriteString(db.table)

	s.WriteString(db.buildSql())

	db.trace(s.String(), db.params...)

	if db.tx == nil {
		db.Result, db.Err = db.Db.Exec(s.String(), db.params...)

	} else {
		db.Result, db.Err = db.tx.Exec(s.String(), db.params...)

	}

	if db.Err != nil {

		db.trace("error:%v", db.Err)
		return db.Err
	}

	aff_nums, err := db.Result.RowsAffected()
	if err == nil {
		db.trace("RowsAffected num:", aff_nums)
	} else {
		db.trace("RowsAffected error:%v", err)
	}

	return err
}

func into(field string) string {

	//arry := strings.Split(field, ",")

	//pty := strings.Repeat("?,", len(arry))
	str := ""
	sum := strings.Count(field, "?")
	for i := 1; i <= sum; i++ {

		str = fmt.Sprintf("%s:%d,", str, i)

	}

	vas := strings.TrimRight(str, `,`)
	return vas
}

func sets(field string) (int, string) {
	str := strings.Replace(field, ",", "=?,", -1)

	sum := strings.Count(str, "?")
	for i := 1; i <= sum; i++ {

		str = strings.Replace(str, "?", fmt.Sprintf(":%d", i), 1)

	}
	return sum, str
}

func (m *ConDB) Save(table, field string, key interface{}, args []interface{}) error {

	var db *ConDB
	if m.parent == nil {

		db = m.clone()
	} else {
		db = m
	}
	idx, ss := sets(field)

	sql := fmt.Sprintf(`update %s set %s where id = :`, table, ss, idx)

	args = append(args, key)
	db.trace(sql, args...)
	if db.tx == nil {

		db.Result, db.Err = db.Db.Exec(sql, args...)
	} else {
		db.Result, db.Err = db.tx.Exec(sql, args...)
	}

	if db.Err != nil {

		return db.Err
	}

	aff_nums, err := db.Result.RowsAffected()
	if err == nil {
		db.trace("RowsAffected num:", aff_nums)
	} else {
		db.trace("RowsAffected error:%v", err)
	}

	return err

}

func (m *ConDB) Add(table, field string, args []interface{}) error {

	var db *ConDB
	if m.parent == nil {

		db = m.clone()
	} else {
		db = m
	}

	sql := `insert into ` + table + ` (` + field + `) values (` + into(field) + `)`

	db.trace(sql, args...)
	if db.tx == nil {

		db.Result, db.Err = db.Db.Exec(sql, args...)
	} else {
		db.Result, db.Err = db.tx.Exec(sql, args...)
	}

	return db.Err

}

func (m *ConDB) Insert(i interface{}) error {

	var db *ConDB
	if m.parent == nil {

		db = m.clone()
	} else {
		db = m
	}
	s := bytes.Buffer{}

	s.WriteString("INSERT INTO  ")

	if db.table == "" {
		db.table = getTable(i)

	}

	s.WriteString(db.table)

	s.WriteString(insertSql(db, i))

	db.trace(s.String())

	//var ret sql.Result
	//var err error
	if db.tx == nil {

		db.Result, db.Err = db.Db.Exec(s.String())
	} else {
		db.Result, db.Err = db.tx.Exec(s.String())
	}

	if db.Err != nil {

		return db.Err
	}

	insID := db.LastInsertId

	db.trace("RowsAffected num:", insID)

	key := reflect.ValueOf(i).Elem().FieldByName("Id")
	typ, _ := reflect.TypeOf(i).Elem().FieldByName("Id")

	tp := typ.Type.Name()

	if tp == "int" {

		newValue := reflect.ValueOf(int(insID))
		key.Set(newValue)
	} else if tp == "int32" {

		newValue := reflect.ValueOf(int32(insID))
		key.Set(newValue)
	} else if tp == "int64" {

		newValue := reflect.ValueOf(insID)
		key.Set(newValue)
	}

	return nil
}

func (db *ConDB) InsertId() int64 {

	return db.LastInsertId
}

func (db *ConDB) Sort(key, sort string) *ConDB {
	if db.parent == nil {
		return nil
	}
	db.sort = fmt.Sprintf(" ORDER BY %s %s ", key, sort)
	return db
}
func (db *ConDB) Page(cur, count int32) *ConDB {
	if db.parent == nil {
		return nil
	}
	start := (cur - 1) * count
	if start < 0 {
		start = 0
	}
	db.Offset = start
	db.Limit = start + count
	return db
}

func (db *ConDB) Or(query string, values ...interface{}) *ConDB {
	if db.parent == nil {
		return nil
	}
	db.OrCondition = append(db.OrCondition, map[string]interface{}{"query": query, "args": values})
	return db
}

func (db *ConDB) IN(key string, value string) *ConDB {

	if db.parent == nil {
		return nil
	}
	db.inCondition = key + " IN (" + value + ") "

	return db
}

func (db *ConDB) GroupBy(value string) *ConDB {
	if db.parent == nil {
		return nil
	}
	db.group = " group by " + value
	return db
}
func (db *ConDB) PageSize(size int32) int32 {

	total := int32(db.Count())

	if total <= 0 {

		return 0
	}
	var page int32 = 0
	if total%size == 0 {
		page = total / size
	} else {
		page = (total + size) / size
	}

	return page

}
func (db *ConDB) Count(agrs ...interface{}) int64 {

	if db.parent == nil {
		return 0
	}
	if db.table == "" {
		if len(agrs) == 0 {
			return 0
		}
		db.table = getTable(agrs[0])
	}

	db_sql := bytes.Buffer{}
	db_sql.WriteString("SELECT count(")
	db_sql.WriteString(db.field)
	db_sql.WriteString(") FROM ")
	db_sql.WriteString(db.table)

	db_sql.WriteString(db.buildSql())

	if db.group != "" {

		db_sql.WriteString(db.group)
	}

	db.trace(db_sql.String(), db.params...)

	var count int64 = 0

	err := db.Db.QueryRow(db_sql.String(), db.params...).Scan(&count)
	if err != nil {

		if err == sql.ErrNoRows {
			// there were no rows, but otherwise no error occurred
			//no found record
		}

		db.Err = err

		return 0
	}
	//count, db.Err = db.dbmap.SelectInt(sql.String())
	return count

}
func (db *ConDB) Find(out interface{}) *ConDB {

	if db.parent == nil {
		return nil
	}
	if db.table == "" {

		db.table = getTable(out)
	}

	sqlStr := bytes.Buffer{}
	sqlStr.WriteString("SELECT ")
	sqlStr.WriteString(db.field)
	sqlStr.WriteString(" FROM ")
	sqlStr.WriteString(db.table)

	sqlStr.WriteString(db.buildSql())

	if db.group != "" {

		sqlStr.WriteString(db.group)
	}
	if db.sort != "" {

		sqlStr.WriteString(db.sort)
	}
	var sql string
	if db.Limit > 0 {

		//ls := fmt.Sprintf(" limit %d,%d", db.Offset, db.Limit)
		//sqlStr.WriteString(ls)
		sql = fmt.Sprintf("SELECT * FROM (SELECT TT.*, ROWNUM AS ROWNO FROM (%s) TT  WHERE ROWNUM <= %d) TABLE_ALIAS WHERE TABLE_ALIAS.ROWNO >= %d", sqlStr.String(), db.Limit, db.Offset)
	} else {
		sql = sqlStr.String()
	}

	db.trace(sql, db.params...)

	rows, err := db.Db.Query(sql, db.params...)
	if err != nil {

		db.Err = err
		return db
	}
	defer rows.Close()

	db.Err = rowsToList(rows, out)
	//_, db.Err = db.dbmap.Select(out, sql.String())
	return db
}
func (db *ConDB) FindAll(out interface{}) *ConDB {

	if db.parent == nil {
		return nil
	}
	if db.table == "" {

		db.table = getTable(out)
	}

	sqlStr := bytes.Buffer{}
	sqlStr.WriteString("SELECT ")
	sqlStr.WriteString(db.field)
	sqlStr.WriteString(" FROM ")
	sqlStr.WriteString(db.table)

	db.trace(sqlStr.String(), nil)

	rows, err := db.Db.Query(sqlStr.String())
	if err != nil {

		db.Err = err
		return db
	}
	defer rows.Close()

	db.Err = rowsToList(rows, out)

	//_, db.Err = db.dbmap.Select(out, sql.String())
	return db
}
func (db *ConDB) Query() (map[string]string, error) {

	if db.parent == nil {
		return nil, errors.New("not found ConDB")
	}
	if db.table == "" {

		return nil, errors.New("not found table")
	}

	sqlStr := bytes.Buffer{}
	sqlStr.WriteString("SELECT ")
	sqlStr.WriteString(db.field)
	sqlStr.WriteString(" FROM ")
	sqlStr.WriteString(db.table)

	sql := db.buildSql()
	sqlStr.WriteString(sql)

	if db.group != "" {

		sqlStr.WriteString(db.group)
	}
	if sql != "" {
		sqlStr.WriteString(" and rownum <=1")
	} else {
		sqlStr.WriteString("  rownum <=1")
	}

	db.trace(sqlStr.String(), db.params...)

	rows, err := db.Db.Query(sqlStr.String(), db.params...)
	if err != nil {

		return nil, err
	}
	defer rows.Close()

	return rowsToMap(rows)

}
func (db *ConDB) List() ([]map[string]string, error) {

	if db.parent == nil {
		return nil, errors.New("not found ConDB")
	}
	if db.table == "" {

		return nil, errors.New("not found table")
	}

	sqlStr := bytes.Buffer{}
	sqlStr.WriteString("SELECT ")
	sqlStr.WriteString(db.field)
	sqlStr.WriteString(" FROM ")
	sqlStr.WriteString(db.table)

	sqlStr.WriteString(db.buildSql())

	if db.group != "" {

		sqlStr.WriteString(db.group)
	}
	if db.sort != "" {

		sqlStr.WriteString(db.sort)
	}

	var sql string
	if db.Limit > 0 {

		//ls := fmt.Sprintf(" limit %d,%d", db.Offset, db.Limit)
		//sqlStr.WriteString(ls)
		sql = fmt.Sprintf("SELECT * FROM (SELECT TT.*, ROWNUM AS ROWNO FROM (%s) TT  WHERE ROWNUM <= %d) TABLE_ALIAS WHERE TABLE_ALIAS.ROWNO >= %d", sqlStr.String(), db.Limit, db.Offset)
	} else {
		sql = sqlStr.String()
	}

	db.trace(sqlStr.String(), db.params...)

	rows, err := db.Db.Query(sql, db.params...)
	if err != nil {

		return nil, err
	}
	defer rows.Close()

	return rowsToMaps(rows)
}

func (db *ConDB) SelectInt(field string) int64 {

	var out int64
	db_sql := bytes.Buffer{}
	db_sql.WriteString("SELECT ")
	db_sql.WriteString(field)
	db_sql.WriteString(" FROM ")
	db_sql.WriteString(db.table)

	sql := db.buildSql()
	db_sql.WriteString(sql)

	if sql != "" {
		db_sql.WriteString(" and rownum <=1")
	} else {
		db_sql.WriteString("  rownum <=1")
	}

	db.trace(db_sql.String(), db.params...)

	db.Err = db.Db.QueryRow(db_sql.String(), db.params...).Scan(&out)
	return out
}
func (db *ConDB) SelectStr(field string) string {

	var out string
	db_sql := bytes.Buffer{}
	db_sql.WriteString("SELECT ")
	db_sql.WriteString(field)
	db_sql.WriteString(" FROM ")
	db_sql.WriteString(db.table)

	sql := db.buildSql()
	db_sql.WriteString(sql)

	if sql != "" {
		db_sql.WriteString(" and rownum <=1")
	} else {
		db_sql.WriteString("  rownum <=1")
	}

	db.trace(db_sql.String(), db.params...)

	db.Err = db.Db.QueryRow(db_sql.String(), db.params...).Scan(&out)
	return out
}
func (db *ConDB) QueryField(field string, out interface{}) error {

	db_sql := bytes.Buffer{}
	db_sql.WriteString("SELECT ")
	db_sql.WriteString(field)
	db_sql.WriteString(" FROM ")
	db_sql.WriteString(db.table)

	db_sql.WriteString(db.buildSql())

	db.trace(db_sql.String(), db.params...)

	rows, err := db.Db.Query(db_sql.String(), db.params...)
	if err != nil {

		return err
	}
	defer rows.Close()

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return err
		}
		return sql.ErrNoRows
	}

	defer rows.Close()

	return rows.Scan(out)
}

func (db *ConDB) IsExit() (bool, error) {

	if db.parent == nil {
		return false, errors.New("no found model")
	}

	var out int64

	db_sql := bytes.Buffer{}
	db_sql.WriteString("SELECT 1  FROM ")
	db_sql.WriteString(db.table)

	sql := db.buildSql()
	db_sql.WriteString(sql)

	if sql != "" {
		db_sql.WriteString(" and rownum =1")
	} else {
		db_sql.WriteString("  rownum =1")
	}

	db.trace(db_sql.String(), db.params...)

	db.Err = db.Db.QueryRow(db_sql.String(), db.params...).Scan(&out)

	if db.Err != nil && db.Err.Error() == "sql: no rows in result set" {

		return false, nil
	}
	return out > 0, db.Err

}
func (db *ConDB) FindById(out, id interface{}) error {

	DB := db
	if db.parent == nil {
		DB = db.clone()
	}
	if DB.table == "" {

		DB.table = getTable(out)
	}

	sqlStr := bytes.Buffer{}
	sqlStr.WriteString("SELECT ")
	sqlStr.WriteString(DB.field)
	sqlStr.WriteString(" FROM ")
	sqlStr.WriteString(DB.table)
	sqlStr.WriteString(" WHERE id=:1")

	DB.trace(sqlStr.String(), id)
	rows, err := DB.Db.Query(sqlStr.String(), id)
	if err != nil {

		return err
	}
	defer rows.Close()

	//return rowsToStruct(rows, out)
	mp, err := rowsToMap(rows)
	StructOfMap(out, mp)
	return err

}
func (db *ConDB) Get(out interface{}) error {

	if db.parent == nil {
		return nil
	}
	if db.table == "" {

		db.table = getTable(out)
	}

	sqlStr := bytes.Buffer{}
	sqlStr.WriteString("SELECT ")
	sqlStr.WriteString(db.field)
	sqlStr.WriteString(" FROM ")
	sqlStr.WriteString(db.table)

	sql := db.buildSql()
	sqlStr.WriteString(sql)

	if db.group != "" {

		sqlStr.WriteString(db.group)
	}

	if sql != "" {
		sqlStr.WriteString(" and rownum =1")
	} else {
		sqlStr.WriteString("  rownum =1")
	}

	db.trace(sqlStr.String(), db.params...)

	t := reflect.TypeOf(out)
	kind := t.Elem().Kind()

	if reflect.Struct == kind {

		rows, err := db.Db.Query(sqlStr.String(), db.params...)
		if err != nil {

			return err
		}
		defer rows.Close()

		//return rowsToStruct(rows, out)
		mp, err := rowsToMap(rows)
		StructOfMap(out, mp)
		return err
	}

	db.Err = db.Db.QueryRow(sqlStr.String(), db.params...).Scan(out)
	return db.Err

}

func (db *ConDB) GetForUpdate(out interface{}) error {

	if db.parent == nil {
		return nil
	}
	if db.table == "" {

		db.table = getTable(out)
	}

	sqlStr := bytes.Buffer{}
	sqlStr.WriteString("SELECT ")
	sqlStr.WriteString(db.field)
	sqlStr.WriteString(" FROM ")
	sqlStr.WriteString(db.table)

	sqlStr.WriteString(db.buildSql())

	sqlStr.WriteString(" for update")

	db.trace(sqlStr.String())

	rows, err := db.tx.Query(sqlStr.String(), db.params...)

	if err != nil {

		return err
	}
	defer rows.Close()

	//return rowsToStruct(rows, out)
	mp, err := rowsToMap(rows)
	StructOfMap(out, mp)
	return err

}

func (m *ConDB) QueryRow(query string, args ...interface{}) *sql.Row {
	m.trace(query, args...)
	return m.Db.QueryRow(query, args...)
}

func (m *ConDB) QueryRows(query string, args ...interface{}) (*sql.Rows, error) {
	m.trace(query, args...)
	return m.Db.Query(query, args...)
}

func (m *ConDB) QueryMap(query string, args ...interface{}) (map[string]string, error) {

	sqlstr := conver(0, query)
	m.trace(sqlstr, args...)
	rows, err := m.Db.Query(sqlstr, args...)
	if err != nil {

		return nil, err
	}
	defer rows.Close()
	return rowsToMap(rows)
}

func (m *ConDB) QueryMaps(query string, args ...interface{}) ([]map[string]string, error) {

	sqlstr := conver(0, query)
	m.trace(sqlstr, args...)
	rows, err := m.Db.Query(sqlstr, args...)
	if err != nil {

		return nil, err
	}
	defer rows.Close()
	return rowsToMaps(rows)
}

type emptyInterface struct {
	typ  *struct{}
	word unsafe.Pointer
}

func StructOfMap(struct_ interface{}, data map[string]string) {

	structInter := (interface{})(struct_)

	t := reflect.TypeOf(structInter).Elem()

	structPtr := (*emptyInterface)(unsafe.Pointer(&structInter)).word

	for i, m := 0, t.NumField(); i < m; i++ {

		obj := t.Field(i)

		col := obj.Tag.Get("db")
		val := data[col]
		key := obj.Name

		if obj.Anonymous { // 输出匿名字段结构

			for x, mm := 0, obj.Type.NumField(); x < mm; x++ {

				fld := obj.Type.Field(x)
				tag := fld.Tag.Get("db")
				kind := fld.Type.Name()

				val = data[tag]

				field, _ := t.FieldByName(fld.Name)
				fieldPtr := uintptr(structPtr) + field.Offset

				if kind == "string" {
					*((*string)(unsafe.Pointer(fieldPtr))) = val
				} else if kind == "int32" {
					*((*int32)(unsafe.Pointer(fieldPtr))) = Int32(val)
				} else if kind == "int64" {
					*((*int64)(unsafe.Pointer(fieldPtr))) = Int64(val)
				}
			}
		} else {

			field, _ := t.FieldByName(key)
			fieldPtr := uintptr(structPtr) + field.Offset

			kind := obj.Type.Name()

			if kind == "string" {
				*((*string)(unsafe.Pointer(fieldPtr))) = val
			} else if kind == "int32" {
				*((*int32)(unsafe.Pointer(fieldPtr))) = Int32(val)
			} else if kind == "int64" {
				*((*int64)(unsafe.Pointer(fieldPtr))) = Int64(val)
			} else if kind == "float64" {
				*((*float64)(unsafe.Pointer(fieldPtr))) = Float64(val)
			} else {
				*((*string)(unsafe.Pointer(fieldPtr))) = val
			}
		}
	}

}

func argsToStr(args ...interface{}) string {
	var margs string
	for i, a := range args {
		var v interface{} = a
		if x, ok := v.(driver.Valuer); ok {
			y, err := x.Value()
			if err == nil {
				v = y
			}
		}
		switch v.(type) {
		case string:
			v = fmt.Sprintf("%q", v)
		default:
			v = fmt.Sprintf("%v", v)
		}
		margs += fmt.Sprintf("%d:%s", i+1, v)
		if i+1 < len(args) {
			margs += " "
		}
	}
	return margs
}
func SliceClear(s *[]interface{}) {
	*s = (*s)[0:0]
}

func (db *ConDB) buildSql() string {

	sql := bytes.Buffer{}
	SliceClear(&db.params)
	if len(db.Condition) > 0 {

		sql.WriteString(" WHERE ")

		i := 0
		for _, clause := range db.Condition {

			query := clause["query"].(string)
			values := clause["args"].([]interface{})
			if i > 0 {
				sql.WriteString(" AND ")
			}

			query = db.Conver(query)
			sql.WriteString(query)

			for _, vv := range values {

				db.params = append(db.params, vv)
			}
			i++
		}

	}
	if len(db.OrCondition) > 0 {

		sql.WriteString(" OR ")

		i := 0

		for _, clause := range db.OrCondition {

			query := clause["query"].(string)
			values := clause["args"].([]interface{})
			if i > 0 {
				sql.WriteString(" OR ")
			}
			query = db.Conver(query)
			sql.WriteString(query)

			for _, vv := range values {

				db.params = append(db.params, vv)
			}
			i++
		}

	}
	if db.inCondition != "" {

		if len(db.Condition) > 0 {

			sql.WriteString(" AND ")
		} else {
			sql.WriteString("  ")
		}
		sql.WriteString(db.inCondition)

	}
	return sql.String()
}

func (db *ConDB) createSql() string {

	sql := bytes.Buffer{}
	if db.query != "" {

		sql.WriteString(" WHERE ")
		sql.WriteString(db.query)

	}

	if len(db.Condition) > 0 {

		if db.query != "" {

			sql.WriteString(" AND ")
		} else {

			sql.WriteString(" WHERE ")
		}

		sql.WriteString(buildCondition(db.Condition))

	}
	if len(db.OrCondition) > 0 {

		sql.WriteString(" OR ")

		sql.WriteString(buildOrCondition(db.OrCondition))

	}
	return sql.String()
}
func (db *ConDB) Conver(str string) string {

	sum := strings.Count(str, "?")
	for i := 1; i <= sum; i++ {

		str = strings.Replace(str, "?", fmt.Sprintf(":%d", i+db.Idx), 1)

	}
	db.Idx += sum
	return str
}
func buildCondition(w []map[string]interface{}) string {

	buff := bytes.NewBuffer([]byte{})
	i := 0

	for _, clause := range w {
		if sql := buildSelectQuery(clause); sql != "" {

			if i > 0 {
				buff.WriteString(" AND ")
			}
			buff.WriteString(sql)
			i++
		}

	}
	return buff.String()
}

func buildOrCondition(w []map[string]interface{}) string {

	buff := bytes.NewBuffer([]byte{})
	i := 0

	for _, clause := range w {
		if sql := buildSelectQuery(clause); sql != "" {

			if i > 0 {
				buff.WriteString(" Or ")
			}
			buff.WriteString(sql)
			i++
		}

	}
	return buff.String()
}

func buildSelectQuery(clause map[string]interface{}) (str string) {
	switch value := clause["query"].(type) {
	case string:
		str = value
	case []string:
		str = strings.Join(value, ", ")
	}

	args := clause["args"].([]interface{})

	buff := bytes.NewBuffer([]byte{})
	i := 0
	for pos, char := range str {
		if str[pos] == ':' {

			if m_type(args[i]) == "string" {
				buff.WriteString("'")
				buff.WriteString(args[i].(string))
				buff.WriteString("'")
			} else {
				buff.WriteString(fmt.Sprintf("%v", args[i]))
			}
			i++
		} else {
			buff.WriteRune(char)
		}
	}

	str = buff.String()

	return
}

func insertSql(db *ConDB, i interface{}) string {

	val := reflect.ValueOf(i)
	getType := reflect.TypeOf(i)
	/*
		for i := 0; i < getType.NumMethod(); i++ {

			m := getType.Method(i)
			if m.Name == "PreInsert" {

				mv := val.MethodByName("PreInsert")
				mv.Call(nil)
			}
		}*/
	_, ins := getType.MethodByName("PreInsert")
	if ins {
		mv := val.MethodByName("PreInsert")
		mv.Call(nil)
	}
	data := toMap(val, getType)
	buff := bytes.NewBuffer([]byte{})
	value := bytes.NewBuffer([]byte{})

	autoSeq := true
	for k, v := range data {

		if k == "ID" {

			id := parseString(v)
			if id == "" || id == "0" {
				continue
			} else {
				autoSeq = false
			}
		}
		buff.WriteString(",")
		buff.WriteString(k)

		value.WriteString(",'")
		value.WriteString(parseString(v))
		value.WriteString("'")

	}

	key := strings.TrimLeft(buff.String(), `,`)
	vas := strings.TrimLeft(value.String(), `,`)

	var sql string
	if autoSeq {

		var seq int64

		err := db.Db.QueryRow("select seq_" + db.table + ".nextval from dual").Scan(&seq)
		if err != nil {

			db.trace("seq error:", err)
			return ""
		}
		db.LastInsertId = seq

		sql = fmt.Sprintf(` (ID,%s ) values (%d,%s)`, key, seq, vas)
	} else {
		sql = ` (` + key + `) values (` + vas + `)`
	}

	return sql
}

func parseString(value interface{}, args ...int) (s string) {
	switch v := value.(type) {
	case bool:
		s = strconv.FormatBool(v)
	case float32:
		s = strconv.FormatFloat(float64(v), 'f', -1, 32)
	case float64:
		s = strconv.FormatFloat(v, 'f', -1, 64)
	case int:
		s = strconv.FormatInt(int64(v), 10)
	case int8:
		s = strconv.FormatInt(int64(v), 10)
	case int16:
		s = strconv.FormatInt(int64(v), 10)
	case int32:
		s = strconv.FormatInt(int64(v), 10)
	case int64:
		s = strconv.FormatInt(v, 10)
	case uint:
		s = strconv.FormatUint(uint64(v), 10)
	case uint8:
		s = strconv.FormatUint(uint64(v), 10)
	case uint16:
		s = strconv.FormatUint(uint64(v), 10)
	case uint32:
		s = strconv.FormatUint(uint64(v), 10)
	case uint64:
		s = strconv.FormatUint(v, 10)
	case string:
		s = v
	case []byte:
		s = string(v)
	default:
		s = fmt.Sprintf("%v", v)
	}
	return s
}

func toMap(v reflect.Value, t reflect.Type) map[string]interface{} {

	m := make(map[string]interface{})
	vt := t.Elem()
	vv := v.Elem()

	for i := 0; i < vt.NumField(); i++ {

		key := vt.Field(i)
		tag := key.Tag.Get("db")

		obj := vt.Field(i)
		value := vv.Field(i).Interface()

		if obj.Anonymous { // 输出匿名字段结构

			for x := 0; x < obj.Type.NumField(); x++ {

				af := obj.Type.Field(x)
				tag := af.Tag.Get("db")
				vv := reflect.ValueOf(value)
				vl := vv.Field(x).Interface()
				m[tag] = vl
			}
		} else {
			m[tag] = value
		}
	}
	return m
}

func structToMap(i interface{}) map[string]interface{} {

	m := make(map[string]interface{})
	vt := reflect.TypeOf(i).Elem()
	vv := reflect.ValueOf(i).Elem()

	for i, mn := 0, vt.NumField(); i < mn; i++ {

		key := vt.Field(i)
		tag := key.Tag.Get("db")
		mk := key.Tag.Get("key")

		value := vv.Field(i).Interface()

		if key.Anonymous { // 输出匿名字段结构

			for x, mm := 0, key.Type.NumField(); x < mm; x++ {

				field := key.Type.Field(x)

				tag := field.Tag.Get("db")

				vv := reflect.ValueOf(value)

				vl := vv.Field(x).Interface()

				m[tag] = vl
			}
		} else if mk == "auto" {
			continue
		} else {

			//fmt.Printf("%q => %q, ", chKey, vv.FieldByName(key.Name).String())
			//fmt.Printf("第%d个字段是：%s:%v = %v \n", i+1, key.Name, key.Type, value)
			m[tag] = value
		}
	}
	return m
}

func mapToStruct(data map[string]string, c interface{}) {

	pv := reflect.ValueOf(c).Elem()
	pt := reflect.TypeOf(c).Elem()

	for i, mn := 0, pt.NumField(); i < mn; i++ {

		obj := pt.Field(i)

		key := pt.Field(i).Name
		ktype := pt.Field(i).Type
		col := pt.Field(i).Tag.Get("db")
		value := data[col]

		val := reflect.ValueOf(value)
		vtype := reflect.TypeOf(value)

		if ktype != vtype {

			val, _ = conversionType(value, ktype.Name())
		}

		if obj.Anonymous { // 输出匿名字段结构

			for x, max := 0, obj.Type.NumField(); x < max; x++ {

				af := obj.Type.Field(x)

				k := af.Name
				t := af.Type
				d := af.Tag.Get("db")

				vl := data[d]

				av := reflect.ValueOf(vl)
				at := reflect.TypeOf(vl)

				if t != at {
					av, _ = conversionType(vl, t.Name())
				}
				pv.FieldByName(k).Set(av)

			}
		} else {

			pv.FieldByName(key).Set(val)
		}
	}
}
func conversionType(value string, ktype string) (reflect.Value, error) {

	if ktype == "string" {

		return reflect.ValueOf(ktype), nil
	} else if ktype == "int64" {

		buf, err := strconv.ParseInt(value, 10, 64)
		return reflect.ValueOf(buf), err
	} else if ktype == "int32" {

		buf, err := strconv.ParseInt(value, 10, 64)
		return reflect.ValueOf(int32(buf)), err
	} else if ktype == "int8" {

		buf, err := strconv.ParseInt(value, 10, 64)
		return reflect.ValueOf(int8(buf)), err
	} else if ktype == "int" {

		buf, err := strconv.Atoi(value)
		return reflect.ValueOf(buf), err
	} else if ktype == "float32" {

		buf, err := strconv.ParseFloat(value, 64)
		return reflect.ValueOf(float32(buf)), err
	} else if ktype == "float64" {

		buf, err := strconv.ParseFloat(value, 64)
		return reflect.ValueOf(buf), err
	} else if ktype == "time.Time" {

		buf, err := time.ParseInLocation("2006-01-02 15:04:05", value, time.Local)
		return reflect.ValueOf(buf), err
	} else if ktype == "Time" {

		buf, err := time.ParseInLocation("2006-01-02 15:04:05", value, time.Local)
		return reflect.ValueOf(buf), err
	} else {
		return reflect.ValueOf(ktype), nil
	}
}

func mapReflect(m map[string]string, v reflect.Value) error {

	t := v.Type()
	val := v.Elem()
	typ := t.Elem()

	if !val.IsValid() {
		return errors.New("数据类型不正确")
	}
	kind := typ.Kind()
	//fmt.Println("type:", kind)
	if reflect.Struct == kind {
		for i := 0; i < val.NumField(); i++ {

			obj := typ.Field(i)

			if obj.Anonymous { // 输出匿名字段结构

				value := val.Field(i)
				for x := 0; x < obj.Type.NumField(); x++ {

					af := obj.Type.Field(x)

					key := af.Name
					ktype := af.Type
					tag := af.Tag.Get("db")

					meta := m[tag]

					vl := reflect.ValueOf(meta)
					vt := reflect.TypeOf(meta)

					if ktype != vt {
						vl, _ = conversionType(meta, ktype.Name())
					}
					value.FieldByName(key).Set(vl)

				}
			}

			key := obj.Name
			ktype := obj.Type

			col := obj.Tag.Get("db")
			if len(col) == 0 {
				continue
			}
			meta, ok := m[col]
			if !ok {
				continue
			}

			vl := reflect.ValueOf(meta)
			vt := reflect.TypeOf(meta)

			if ktype != vt {
				vl, _ = conversionType(meta, ktype.Name())
			}

			val.FieldByName(key).Set(vl)

		}
	} else if kind == reflect.Int64 || kind == reflect.Int32 || kind == reflect.Int {

		for _, value := range m {

			integer64, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return err
			}
			val.SetInt(integer64)
		}

	} else if kind == reflect.Float64 {

		for _, value := range m {

			integer64, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return err
			}
			val.SetInt(integer64)
		}

	} else if kind == reflect.Bool {

		for _, value := range m {

			b, err := strconv.ParseBool(value)
			if err != nil {
				return err
			}
			val.SetBool(b)
		}

	} else { //reflect.Int32, reflect.Int64，reflect.Int, reflect.String

		for _, value := range m {
			//fmt.Println(key, "======:", value)
			newValue := reflect.ValueOf(value)
			val.Set(newValue)
		}
	}
	return nil
}

/*

	value := val.Field(i)
	kind := value.Kind()
	tag := typ.Field(i).Tag.Get("db")
	if !value.CanSet() {
		return errors.New("结构体字段没有读写权限")
	}

	if len(meta) == 0 {
		continue
	}
	meta, ok := m[tag]
	if !ok {
		continue
	}

	if kind == reflect.Struct {

		fmt.Println(kind)

	} else if kind == reflect.String {
		value.SetString(meta)
	} else if kind == reflect.Float32 {
		f, err := strconv.ParseFloat(meta, 32)
		if err != nil {
			return err
		}
		value.SetFloat(f)
	} else if kind == reflect.Float64 {
		f, err := strconv.ParseFloat(meta, 64)
		if err != nil {
			return err
		}
		value.SetFloat(f)
	} else if kind == reflect.Int64 {
		integer64, err := strconv.ParseInt(meta, 10, 64)
		if err != nil {
			return err
		}
		value.SetInt(integer64)
	} else if kind == reflect.Int {
		integer, err := strconv.Atoi(meta)
		if err != nil {
			return err
		}
		value.SetInt(int64(integer))
	} else if kind == reflect.Int32 {
		integer, err := strconv.ParseInt(meta, 10, 64)
		if err != nil {
			return err
		}
		value.SetInt(integer)
	} else if kind == reflect.Bool {
		b, err := strconv.ParseBool(meta)
		if err != nil {
			return err
		}
		value.SetBool(b)
	} else if kind == reflect.Int8 {
		integer, err := strconv.ParseInt(meta, 10, 64)
		if err != nil {
			return err
		}
		value.SetInt(integer)
	} else {
		fmt.Println(kind)
		return errors.New("数据库映射存在不识别的数据类型")
	}
*/

func rowsToList(rows *sql.Rows, in interface{}) error {

	d, err := rowsToMaps(rows)
	if err != nil {
		return err
	}

	length := len(d)

	if length > 0 {
		v := reflect.ValueOf(in).Elem()

		newv := reflect.MakeSlice(v.Type(), 0, length)
		v.Set(newv)
		v.SetLen(length)

		index := 0
		for i := 0; i < length; i++ {

			k := v.Type().Elem()

			newObj := reflect.New(k)
			err := mapReflect(d[i], newObj)
			if err != nil {
				return err
			}

			v.Index(index).Set(newObj.Elem())
			index++
		}
		v.SetLen(index)
	}
	return nil
}
func rowsToStruct(rows *sql.Rows, out interface{}) error {

	column, err := rows.Columns() //读出查询出的列字段名
	if err != nil {
		//logger.Error(err)
		return err
	}

	values := make([][]byte, len(column))     //values是每个列的值，这里获取到byte里
	scans := make([]interface{}, len(column)) //因为每次查询出来的列是不定长的，用len(column)定住当次查询的长度

	for i := range values {

		scans[i] = &values[i]
	}

	for rows.Next() {

		if err := rows.Scan(scans...); err != nil {
			//query.Scan查询出来的不定长值放到scans[i] = &values[i],也就是每行都放在values里
			return err
		}

		row := make(map[string]string) //每行数据
		for k, v := range values {
			//每行数据是放在values里面，现在把它挪到row里
			key := column[k]
			row[key] = string(v)
		}

		mapToStruct(row, out)
		return nil
	}

	return errors.New("not found rows")
}

func rowsToMap(rows *sql.Rows) (map[string]string, error) {

	column, err := rows.Columns() //读出查询出的列字段名
	if err != nil {
		//logger.Error(err)
		return nil, err
	}

	values := make([][]byte, len(column))     //values是每个列的值，这里获取到byte里
	scans := make([]interface{}, len(column)) //因为每次查询出来的列是不定长的，用len(column)定住当次查询的长度

	for i := range values {

		scans[i] = &values[i]
	}

	for rows.Next() {

		if err := rows.Scan(scans...); err != nil {
			//query.Scan查询出来的不定长值放到scans[i] = &values[i],也就是每行都放在values里
			//logger.Error(err)
			return nil, err
		}

		row := make(map[string]string) //每行数据
		for k, v := range values {
			//每行数据是放在values里面，现在把它挪到row里
			key := column[k]
			row[key] = string(v)
		}
		return row, nil
	}

	return nil, errors.New("not found rows")
}

func rowsToMaps(rows *sql.Rows) ([]map[string]string, error) {

	column, err := rows.Columns() //读出查询出的列字段名
	if err != nil {
		//logger.Error(err)
		return nil, err
	}

	values := make([][]byte, len(column))     //values是每个列的值，这里获取到byte里
	scans := make([]interface{}, len(column)) //因为每次查询出来的列是不定长的，用len(column)定住当次查询的长度

	for i := range values {

		scans[i] = &values[i]
	}

	results := make([]map[string]string, 0) //最后得到的map
	for rows.Next() {

		if err := rows.Scan(scans...); err != nil {
			//query.Scan查询出来的不定长值放到scans[i] = &values[i],也就是每行都放在values里
			//logger.Error(err)
			return nil, err
		}

		row := make(map[string]string) //每行数据
		for k, v := range values {
			//每行数据是放在values里面，现在把它挪到row里
			key := column[k]
			row[key] = string(v)
		}
		results = append(results, row)
	}

	return results, nil
}

func conver(idx int, str string) string {

	sum := strings.Count(str, "?")
	for i := 1; i <= sum; i++ {

		str = strings.Replace(str, "?", fmt.Sprintf(":%d", i+idx), 1)

	}
	return str
}

func Int(f string) int {
	v, _ := strconv.ParseInt(f, 10, 0)
	return int(v)
}
func Int32(f string) int32 {
	v, _ := strconv.ParseInt(f, 10, 64)
	return int32(v)
}

func Int64(f string) int64 {
	v, _ := strconv.ParseInt(f, 10, 64)
	return int64(v)
}

func Float64(f string) float64 {
	v, _ := strconv.ParseFloat(f, 64)
	return float64(v)
}
