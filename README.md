# gomDB

golang ORM库，用于oracle版本

###使用

```go
import (  
	"database/sql"   
	"fmt"  
	_ "github.com/godror/godror"  
  "github.com/gkyh/racm"  
)  

  db, _ := sql.Open("godror", `user="testq" password="123456" connectString="192.169.123.72:1521/orcl"`)  
  db.SetMaxOpenConns(200)  
  db.SetMaxIdleConns(10)  
  db.Ping()  
  
  //init 
  mdb := &MDB{Db: db}  
  
  type Person struct { 
	  Id       int32  `db:"id" key:"auto"`  
	  Userid   int32  `db:"userid"`  
	  Phone    string `db:"phone"`  
	  Status   int32  `db:"status"`  
	  Accname  string `db:"acc_name"`  
	  Accno    string `db:"acc_no"`  

}  
```
###查询

```go
  var a Person  
  var arr []Person  

根据主键查询  
  db.FindById(&a, 241)  
  //select * from tb_person where id = 241
Where  
  db.Where("userid=? and phone=?", 10001, "13345678900").Get(&a)   
  //select * from  tb_person where userid=10001 and phone="13345678900" 
Map  
 m:= map[string]interface{}{"userid":10001,"phone": "13345678900"}
 db.Map(m).Get(&a)  
 //select * from  tb_person where userid=10001 and phone="13345678900"
 
 Find  
   db.Where("status=?", 1).Find(&arr)   
  //select * from  tb_person where statuse= 1
  
  db.Where("status=?", 1).Where("accno <> ?","122222").Find(&arr)  
  
Page  
   db.Where("status=?", 1).Page(1,20).Sort("id","desc").Find(&arr)    
   
   //select * from  tb_person where statuse= 1 order by id desc limit 0,20
   //Page(2,20) => limit 20,20
   
Count  
    db.Where("status=?", 1).Count(&arr)  
    db.Model(a).Where("status=?", 1).Count()  
    //select count(*) from  tb_person where statuse= 1  
```

插入，删除、更新  
```go
    p:= Person{Userid:111,Phone:"133103904940",Status:1,Accno:"3039383884444",Accname:"aaa"}  
    db.Insert(a)  
    //insert into tb_person (userid,phone,status,accno,accname) values (111,"133103904940",1,"3039383884444","aaa")  
    
    db.Model(Person{}).Where("id=?",12).Update("status=?",2)  
    
    //update tb_person  set status = 2 where id = 12
    
    db.Model(a).where("id=?",12).Delete()
    
    //delete from tb_person  where id = 12  
 ```
 设置表名  
 ```go
    db.SetPrefix("t_") // 设置表名前缀 默认tb_， 全局生效初始化设置一次
    
    db.Model(Person{}) //设置表名为 t_person
    
    db.Table("person") //设置表名为 person
    
    db.Table("person").Where("phone=?","3039383884444").Get(&a)
    //select * from person where phone='3039383884444' limit 1
```

定义查询字段
```go

   var userid int32
   db.Model(a).Select("userid).Where("phone=?","3039383884444").QueryField(&userid)
   
   //select userid from tb_person where phone = '3039383884444'
   
```
