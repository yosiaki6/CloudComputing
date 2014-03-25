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
)


type FastCGIServer struct{}

var db *sql.DB
var stmtOut *sql.Stmt
var tweet_id string
var err error
var cache map[string] string
var cache_keys []string
var max_cache_size int
var delete_cache_key string

func (s FastCGIServer) q1(resp http.ResponseWriter, req *http.Request) {
  var buffer bytes.Buffer
  var t = time.Now()
  buffer.WriteString("GiraffeLovers,3823-5293-0215\n")
  buffer.WriteString(fmt.Sprintf("%04d-%02d-%02d+%02d:%02d:%02d\n",t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second()))
  resp.Write([]byte(buffer.String()))
}


func (s FastCGIServer) q2(resp http.ResponseWriter, req *http.Request) {
  fmt.Printf("%s\n\n",req.URL.Path)
  var buffer bytes.Buffer
  buffer.WriteString("GiraffeLovers,3823-5293-0215\n")
   user_id := req.FormValue("user_id")
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
  

   rows, err := stmtOut.Query(user_id, tweet_time)
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

   if( len(cache) >= max_cache_size ){
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
  /*
  db, err = sql.Open("mysql", "root:tobymlab@tcp/cloud")
  if err != nil {
    panic(err.Error())  // Just for example purpose. You should use proper error handling instead of panic
  }
  defer db.Close()

  stmtOut, err = db.Prepare("SELECT tweet_id FROM plan1 WHERE user_id = ? and tweet_time = ?")
  if err != nil {
    panic(err.Error()) // proper error handling instead of panic in your app
  }
  defer stmtOut.Close()
  */

  listener,_:= net.Listen("tcp",":9000")
  cache = make(map[string]string)
  max_cache_size = 10000
  srv := new(FastCGIServer)
  fcgi.Serve(listener, srv)
}
