package main

import (
    "fmt"
    "log"
    "net/http"
    "os"
    "os/signal"
    "syscall"
    "time"
    "bytes"
    "database/sql"
     _ "github.com/go-sql-driver/mysql"
    "strings"
    "strconv"
    "runtime"
)

var (
    abort bool
)


type Server struct { }
const POOL_SIZE_q2  = 4000
const POOL_SIZE_q3  = 0

//for q2
var db_conn_pool_1 []*sql.DB
var stmtOut_pool_1 []*sql.Stmt
var index_pool_1 []int
var db_conn_pool_2 []*sql.DB
var stmtOut_pool_2 []*sql.Stmt
var index_pool_2 []int
var db_conn_pool_3 []*sql.DB
var stmtOut_pool_3 []*sql.Stmt
var index_pool_3 []int
var db_conn_pool_4 []*sql.DB
var stmtOut_pool_4 []*sql.Stmt
var index_pool_4 []int
var db_conn_pool_5 []*sql.DB
var stmtOut_pool_5 []*sql.Stmt
var index_pool_5 []int
//for q3
var db_conn_pool []*sql.DB
var stmtOut_pool []*sql.Stmt
var index_pool []int

//q2 db servers
const db_server_1 = "ec2-54-86-13-44.compute-1.amazonaws.com"
const db_server_2 = "ec2-54-86-30-1.compute-1.amazonaws.com"
const db_server_3 = "ec2-54-86-21-73.compute-1.amazonaws.com"
const db_server_4 = "ec2-54-86-11-160.compute-1.amazonaws.com"
const db_server_5 = "ec2-54-85-190-60.compute-1.amazonaws.com"
//q3 loadbalancer
const db_server = "warmup-328885318.us-east-1.elb.amazonaws.com"

func (s Server) q1(resp http.ResponseWriter, req *http.Request) {
  var buffer bytes.Buffer
  var t = time.Now()
  buffer.WriteString("GiraffeLovers,3823-5293-0215\n")
  buffer.WriteString(fmt.Sprintf("%04d-%02d-%02d+%02d:%02d:%02d\n",t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second()))
  resp.Write([]byte(buffer.String()))
}


func (s Server) q2(resp http.ResponseWriter, req *http.Request) {
  var err error
  var tweet_id string
  var buffer bytes.Buffer
  var stmtOut *sql.Stmt
  buffer.WriteString("GiraffeLovers,3823-5293-0215\n")
  user_id_string := req.FormValue("userid")
  tweet_time := req.FormValue("tweet_time")
  tweet_time = strings.Replace( tweet_time, " ", "+",1)
  user_id_int64, err := strconv.ParseInt(user_id_string, 0, 64)
  if err != nil {
    fmt.Println(err)
      return
  }

  //decide whinc db server is used according to the user_id range   
  if 0 < user_id_int64 && user_id_int64 <= 197834718 {
     var index int;
     user_id := strconv.FormatInt(user_id_int64, 10)
     for len(index_pool_1) == 0 {
	fmt.Println("hit")
     }

     index_pool_1, index = index_pool_1[:len(index_pool_1)-1], index_pool_1[len(index_pool_1)-1]
     stmtOut = stmtOut_pool_1[index]

     rows, err := stmtOut.Query(user_id, tweet_time)
     if err != nil {
       db_conn_pool_1[index], err = sql.Open("mysql", "giraffe:giraffe@tcp("+db_server_1+":3306)/cloud")
       if err != nil {
         panic(err.Error())  // Just for example purpose. You should use proper error handling instead of panic
         return;
       }
       stmtOut_pool_1[index], err = db_conn_pool_1[index].Prepare("SELECT tweet_id FROM q2_1 WHERE user_id = ? and tweet_time = ?")
       if err != nil {
         panic(err.Error()) // proper error handling instead of panic in your app
         return;
       }
       rows, err = stmtOut_pool_1[index].Query(user_id, tweet_time)
       if err != nil{
         panic(err.Error()) // proper error handling instead of panic in your app
         return;
       }
     }

     for rows.Next(){
       err = rows.Scan(&tweet_id)
       if err != nil{
         panic(err.Error())
         return
       }
       buffer.WriteString(tweet_id)
       buffer.WriteString("\n")
     }

     index_pool_1 = append(index_pool_1, index)

     resp.Write([]byte(buffer.String()))

  }else if 197834718 < user_id_int64 && user_id_int64 <= 396767602 {
     var index int;
     user_id := strconv.FormatInt(user_id_int64, 10)
     for len(index_pool_2) == 0 {
	fmt.Println("hit")
     }

     index_pool_2, index = index_pool_2[:len(index_pool_2)-1], index_pool_2[len(index_pool_2)-1]
     stmtOut = stmtOut_pool_2[index]

     rows, err := stmtOut.Query(user_id, tweet_time)
     if err != nil {
       db_conn_pool_2[index], err = sql.Open("mysql", "giraffe:giraffe@tcp("+db_server_2+":3306)/cloud")
       if err != nil {
         panic(err.Error())  // Just for example purpose. You should use proper error handling instead of panic
         return;
       }
       stmtOut_pool_2[index], err = db_conn_pool_2[index].Prepare("SELECT tweet_id FROM q2_2 WHERE user_id = ? and tweet_time = ?")
       if err != nil {
         panic(err.Error()) // proper error handling instead of panic in your app
         return;
       }
       rows, err = stmtOut_pool_2[index].Query(user_id, tweet_time)
       if err != nil{
         panic(err.Error()) // proper error handling instead of panic in your app
         return;
       }
     }

     for rows.Next(){
       err = rows.Scan(&tweet_id)
       if err != nil{
         panic(err.Error())
         return
       }
       buffer.WriteString(tweet_id)
       buffer.WriteString("\n")
     }

     index_pool_2 = append(index_pool_2, index)

     resp.Write([]byte(buffer.String())) 

  }else if 396767602 < user_id_int64 && user_id_int64 <= 742870590 {
     var index int;
     user_id := strconv.FormatInt(user_id_int64, 10)

     for len(index_pool_3) == 0 {
	fmt.Println("hit")
     }

     index_pool_3, index = index_pool_3[:len(index_pool_3)-1], index_pool_3[len(index_pool_3)-1]
     stmtOut = stmtOut_pool_3[index]

     rows, err := stmtOut.Query(user_id, tweet_time)
     if err != nil {
       db_conn_pool_3[index], err = sql.Open("mysql", "giraffe:giraffe@tcp("+db_server_3+":3306)/cloud")
       if err != nil {
         panic(err.Error())  // Just for example purpose. You should use proper error handling instead of panic
         return;
       }
       stmtOut_pool_3[index], err = db_conn_pool_3[index].Prepare("SELECT tweet_id FROM q2_3 WHERE user_id = ? and tweet_time = ?")
       if err != nil {
         panic(err.Error()) // proper error handling instead of panic in your app
         return;
       }
       rows, err = stmtOut_pool_3[index].Query(user_id, tweet_time)
       if err != nil{
         panic(err.Error()) // proper error handling instead of panic in your app
         return;
       }
     }

     for rows.Next(){
       err = rows.Scan(&tweet_id)
       if err != nil{
         panic(err.Error())
         return
       }
       buffer.WriteString(tweet_id)
       buffer.WriteString("\n")
     }

     index_pool_3 = append(index_pool_3, index)

     resp.Write([]byte(buffer.String()))

  }else if 742870590 < user_id_int64 && user_id_int64 <= 1584744955 {
     var index int;
     user_id := strconv.FormatInt(user_id_int64, 10)

     for len(index_pool_4) == 0 {
	fmt.Println("hit")
     }

     index_pool_4, index = index_pool_4[:len(index_pool_4)-1], index_pool_4[len(index_pool_4)-1]
     stmtOut = stmtOut_pool_4[index]

     rows, err := stmtOut.Query(user_id, tweet_time)
     if err != nil {
       db_conn_pool_4[index], err = sql.Open("mysql", "giraffe:giraffe@tcp("+db_server_4+":3306)/cloud")
       if err != nil {
         panic(err.Error())  // Just for example purpose. You should use proper error handling instead of panic
         return;
       }
       stmtOut_pool_4[index], err = db_conn_pool_4[index].Prepare("SELECT tweet_id FROM q2_4 WHERE user_id = ? and tweet_time = ?")
       if err != nil {
         panic(err.Error()) // proper error handling instead of panic in your app
         return;
       }
       rows, err = stmtOut_pool_4[index].Query(user_id, tweet_time)
       if err != nil{
         panic(err.Error()) // proper error handling instead of panic in your app
         return;
       }
     }

     for rows.Next(){
       err = rows.Scan(&tweet_id)
       if err != nil{
         panic(err.Error())
         return
       }
       buffer.WriteString(tweet_id)
       buffer.WriteString("\n")
     }

     index_pool_4 = append(index_pool_4, index)

     resp.Write([]byte(buffer.String()))

  }else if 1584744955 < user_id_int64 {
     var index int;
     user_id := strconv.FormatInt(user_id_int64, 10)

     for len(index_pool_5) == 0 {
	fmt.Println("hit")
     }

     index_pool_5, index = index_pool_5[:len(index_pool_5)-1], index_pool_5[len(index_pool_5)-1]
     stmtOut = stmtOut_pool_5[index]

     rows, err := stmtOut.Query(user_id, tweet_time)
     if err != nil {
       db_conn_pool_5[index], err = sql.Open("mysql", "giraffe:giraffe@tcp("+db_server_5+":3306)/cloud")
       if err != nil {
         panic(err.Error())  // Just for example purpose. You should use proper error handling instead of panic
         return;
       }
       stmtOut_pool_5[index], err = db_conn_pool_5[index].Prepare("SELECT tweet_id FROM q2_5 WHERE user_id = ? and tweet_time = ?")
       if err != nil {
         panic(err.Error()) // proper error handling instead of panic in your app
         return;
       }
       rows, err = stmtOut_pool_5[index].Query(user_id, tweet_time)
       if err != nil{
         panic(err.Error()) // proper error handling instead of panic in your app
         return;
       }
     }

     for rows.Next(){
       err = rows.Scan(&tweet_id)
       if err != nil{
         panic(err.Error())
         return
       }
       buffer.WriteString(tweet_id)
       buffer.WriteString("\n")
     }

     index_pool_5 = append(index_pool_5, index)

     resp.Write([]byte(buffer.String()))
  }
}

func (s Server) q3(resp http.ResponseWriter, req *http.Request) {
  var err error
  var retweet_users string
  var buffer bytes.Buffer
  var stmtOut *sql.Stmt
  buffer.WriteString("GiraffeLovers,3823-5293-0215\n")
  user_id := req.FormValue("userid")

   var index int;

  for len(index_pool) == 0 {
	fmt.Println("hit")
  }

     index,index_pool = index_pool[0], index_pool[1:len(index_pool)]
     stmtOut = stmtOut_pool[index]

   rows, err := stmtOut.Query(user_id)
   if err != nil {
    db_conn_pool[index], err = sql.Open("mysql", "giraffe:giraffe@tcp("+db_server+":3306)/cloud")
    if err != nil {
      panic(err.Error())  // Just for example purpose. You should use proper error handling instead of panic
      return;
    }
    stmtOut_pool[index], err = db_conn_pool[index].Prepare("SELECT retweet_users FROM q3 WHERE user_id = ?")
    if err != nil {
     panic(err.Error()) // proper error handling instead of panic in your app
     return;
    }
    rows, err = stmtOut_pool[index].Query(user_id)
    if err != nil{
     panic(err.Error()) // proper error handling instead of panic in your app
     return;
    }
   }

  for rows.Next(){
    err = rows.Scan(&retweet_users)
    if err != nil{
      panic(err.Error())
      return
    }
    user_id_set := strings.Split(retweet_users, ";")
    for i := 0; i < len(user_id_set)-1; i++ {
      buffer.WriteString(user_id_set[i])
      buffer.WriteString("\n")    
    }
  }
   index_pool = append(index_pool, index)

   resp.Write([]byte(buffer.String()))
}

func (s Server) ServeHTTP(resp http.ResponseWriter, req *http.Request) {
  switch(req.URL.Path){
    case "/q1":
      s.q1(resp, req)
    case "/q2":
      s.q2(resp, req)
    case "/q3":
      s.q3(resp, req)
  }
}

func main() {
    runtime.GOMAXPROCS(2) 
    sigchan := make(chan os.Signal, 1)
    signal.Notify(sigchan, os.Interrupt)
    signal.Notify(sigchan, syscall.SIGTERM)

  //q2 db server1 
  for i := 0; i < POOL_SIZE_q2; i++ {
    index_pool_1 = append(index_pool_1, i)
    db_conn, err := sql.Open("mysql", "giraffe:giraffe@tcp("+db_server_1+":3306)/cloud")
    if err != nil {
      panic(err.Error())  // Just for example purpose. You should use proper error handling instead of panic
    }
    db_conn_pool_1 = append(db_conn_pool_1, db_conn)
    defer db_conn_pool_1[i].Close()
  }

  for i := 0; i < POOL_SIZE_q2; i++ {
    stmtOut, err := db_conn_pool_1[i].Prepare("SELECT tweet_id FROM q2_1 WHERE user_id = ? and tweet_time = ?")
    if err != nil {
     panic(err.Error()) // proper error handling instead of panic in your app
    }
    stmtOut_pool_1 = append(stmtOut_pool_1, stmtOut)
    defer stmtOut_pool_1[i].Close()
  }

  //q2 db server2
  for i := 0; i < POOL_SIZE_q2; i++ {
    index_pool_2 = append(index_pool_2, i)
    db_conn, err := sql.Open("mysql", "giraffe:giraffe@tcp("+db_server_2+":3306)/cloud")
    if err != nil {
      panic(err.Error())  // Just for example purpose. You should use proper error handling instead of panic
    }
    db_conn_pool_2 = append(db_conn_pool_2, db_conn)
    defer db_conn_pool_2[i].Close()
  }

  for i := 0; i < POOL_SIZE_q2; i++ {
    stmtOut, err := db_conn_pool_2[i].Prepare("SELECT tweet_id FROM q2_2 WHERE user_id = ? and tweet_time = ?")
    if err != nil {
     panic(err.Error()) // proper error handling instead of panic in your app
    }
    stmtOut_pool_2 = append(stmtOut_pool_2, stmtOut)
    defer stmtOut_pool_2[i].Close()
  }

  //q2 db server 3
  for i := 0; i < POOL_SIZE_q2; i++ {
    index_pool_3 = append(index_pool_3, i)
    db_conn, err := sql.Open("mysql", "giraffe:giraffe@tcp("+db_server_3+":3306)/cloud")
    if err != nil {
      panic(err.Error())  // Just for example purpose. You should use proper error handling instead of panic
    }
    db_conn_pool_3 = append(db_conn_pool_3, db_conn)
    defer db_conn_pool_3[i].Close()
  }

  for i := 0; i < POOL_SIZE_q2; i++ {
    stmtOut, err := db_conn_pool_3[i].Prepare("SELECT tweet_id FROM q2_3 WHERE user_id = ? and tweet_time = ?")
    if err != nil {
     panic(err.Error()) // proper error handling instead of panic in your app
    }
    stmtOut_pool_3 = append(stmtOut_pool_3, stmtOut)
    defer stmtOut_pool_3[i].Close()
  }

  //q2 db server4
  for i := 0; i < POOL_SIZE_q2; i++ {
    index_pool_4 = append(index_pool_4, i)
    db_conn, err := sql.Open("mysql", "giraffe:giraffe@tcp("+db_server_4+":3306)/cloud")
    if err != nil {
      panic(err.Error())  // Just for example purpose. You should use proper error handling instead of panic
    }
    db_conn_pool_4 = append(db_conn_pool_4, db_conn)
    defer db_conn_pool_4[i].Close()
  }

  for i := 0; i < POOL_SIZE_q2; i++ {
    stmtOut, err := db_conn_pool_4[i].Prepare("SELECT tweet_id FROM q2_4 WHERE user_id = ? and tweet_time = ?")
    if err != nil {
     panic(err.Error()) // proper error handling instead of panic in your app
    }
    stmtOut_pool_4 = append(stmtOut_pool_4, stmtOut)
    defer stmtOut_pool_4[i].Close()
  }

  //q2 db server5
  for i := 0; i < POOL_SIZE_q2; i++ {
    index_pool_5 = append(index_pool_5, i)
    db_conn, err := sql.Open("mysql", "giraffe:giraffe@tcp("+db_server_5+":3306)/cloud")
    if err != nil {
      panic(err.Error())  // Just for example purpose. You should use proper error handling instead of panic
    }
    db_conn_pool_5 = append(db_conn_pool_5, db_conn)
    defer db_conn_pool_5[i].Close()
  }

  for i := 0; i < POOL_SIZE_q2; i++ {
    stmtOut, err := db_conn_pool_5[i].Prepare("SELECT tweet_id FROM q2_5 WHERE user_id = ? and tweet_time = ?")
    if err != nil {
     panic(err.Error()) // proper error handling instead of panic in your app
    }
    stmtOut_pool_5 = append(stmtOut_pool_5, stmtOut)
    defer stmtOut_pool_5[i].Close()
  }

/*
  //q3 db server 
  for i := 0; i < POOL_SIZE_q3; i++ {
    index_pool = append(index_pool, i)
    db_conn, err := sql.Open("mysql", "giraffe:giraffe@tcp("+db_server+":3306)/cloud")
    if err != nil {
      panic(err.Error())  // Just for example purpose. You should use proper error handling instead of panic
    }
    db_conn_pool = append(db_conn_pool, db_conn)
    defer db_conn_pool[i].Close()
  }

  for i := 0; i < POOL_SIZE_q3; i++ {
    stmtOut, err := db_conn_pool[i].Prepare("SELECT retweet_users FROM q3 WHERE user_id = ?")
    if err != nil {
     panic(err.Error()) // proper error handling instead of panic in your app
    }
    stmtOut_pool = append(stmtOut_pool, stmtOut)
    defer stmtOut_pool[i].Close()
  }
*/

  fmt.Print("Done\n");

  server := Server{}

  go func() {
    http.Handle("/", server)
    if err := http.ListenAndServe(":80", nil); err != nil {
      log.Fatal(err)
    }
  }()

  <-sigchan
}

