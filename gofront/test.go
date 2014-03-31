package main

import (
  "net"
  "net/http"
  "net/http/fcgi"
  "bytes"
  "fmt"
  "database/sql"
  _ "github.com/go-sql-driver/mysql"
  "strings"
  "time"
  "sync"
)


type FastCGIServer struct{}

const CACHE_SIZE  = 100000
const POOL_SIZE  = 900
var mutex = &sync.Mutex{}
var db *sql.DB
var stmtOut *sql.Stmt
var tweet_id string
var err error
var cache map[string] string
var cache_keys []string
var delete_cache_key string
//var hbase_conn_pool [POOL_SIZE]*goh.HClient
var db_conn_pool [POOL_SIZE]*sql.DB
var stmtOut_pool [POOL_SIZE]*sql.Stmt
var index_pool []int

func (s FastCGIServer) q1(resp http.ResponseWriter, req *http.Request) {
  var buffer bytes.Buffer
  var t = time.Now()
  buffer.WriteString("GiraffeLovers,3823-5293-0215\n")
  buffer.WriteString(fmt.Sprintf("%04d-%02d-%02d+%02d:%02d:%02d\n",t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second()))
  resp.Write([]byte(buffer.String()))
}


func (s FastCGIServer) q2(resp http.ResponseWriter, req *http.Request) {
  var buffer bytes.Buffer
  buffer.WriteString("GiraffeLovers,3823-5293-0215\n")
   user_id := req.FormValue("userid")
   tweet_time := req.FormValue("tweet_time")
   tweet_time = strings.Replace( tweet_time, " ", "+",1)
   cache_key := user_id + "_" + tweet_time
   result, ok := cache[cache_key]
   if ok {
     //fmt.Printf("OK:%d\n", len(cache))
    resp.Write([]byte(result))
   } else{
   if err != nil {
     panic(err.Error()) // proper error handling instead of panic in your app
   }
  

   mutex.Lock()
   for len(index_pool) == 0 {}
   index := index_pool[0]
   index_pool = index_pool[1:len(index_pool)]
   mutex.Unlock()

   rows, err := stmtOut_pool[index].Query(user_id, tweet_time)
   if err != nil {
     panic(err.Error()) // proper error handling instead of panic in your app
     return
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

   index_pool = append(index_pool, index)

   if( len(cache) >= CACHE_SIZE ){
    delete(cache, cache_keys[0])
    cache_keys = cache_keys[1:len(cache_keys)]
   }
   cache_keys = append(cache_keys, cache_key) 
   cache[cache_key] = buffer.String()
   //fmt.Print(cache_keys[0])
   resp.Write([]byte(buffer.String()))
  } 
}

func (s FastCGIServer) ServeHTTP(resp http.ResponseWriter, req *http.Request) {
//func q2(resp http.ResponseWriter, req *http.Request) {
  switch(req.URL.Path){
    case "/q1":
      s.q1(resp, req)
    case "/q2":
      s.q2(resp, req)
  }
}

func main(){
  for i := 0; i < POOL_SIZE; i++ {
    index_pool = append(index_pool, i)
    db_conn_pool[i], err = sql.Open("mysql", "giraffe:giraffe@tcp(ec2-54-85-197-85.compute-1.amazonaws.com:3306)/cloud")
    if err != nil {
      panic(err.Error())  // Just for example purpose. You should use proper error handling instead of panic
    }
    defer db_conn_pool[i].Close()
  }

  for i := 0; i < POOL_SIZE; i++ {
    stmtOut_pool[i], err = db_conn_pool[i].Prepare("SELECT tweet_id FROM plan1 WHERE user_id = ? and tweet_time = ?")
    if err != nil {
     panic(err.Error()) // proper error handling instead of panic in your app
    }
    defer stmtOut_pool[i].Close()
  }

  fmt.Print("Done")
  listener,_:= net.Listen("tcp",":9000")
  cache = make(map[string]string)
  srv := new(FastCGIServer)
  fcgi.Serve(listener, srv)
}

