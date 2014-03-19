package main

import (
  "net"
  "net/http"
  "net/http/fcgi"
  "bytes"
  "fmt"
  "database/sql"
  //"github.com/go-sql-driver/mysql"
  "strings"
  "time"
  "github.com/sdming/goh"
)

type FastCGIServer struct{}
type ApiHandler struct{}

var db *sql.DB
var stmtOut *sql.Stmt
var tweet_id string
var err error
var cache map[string] string
var cache_keys []string
var max_cache_size int
var delete_cache_key string

//var hbclient *goh.HClient

func query_mysql(resp http.ResponseWriter, req *http.Request) {
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
  } else {
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
      if err != nil {
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

func query_hbase(resp http.ResponseWriter, req *http.Request) {
  var buffer bytes.Buffer
  buffer.WriteString("GiraffeLovers,3823-5293-0215\n")

  // Connect to HBase
  address := "ec2-54-85-145-245.compute-1.amazonaws.com:9090"
  hbclient, err := goh.NewTcpClient(address, goh.TBinaryProtocol, false)
  if err != nil {
    fmt.Print("NewTcpClient error :: ")
    fmt.Println(err)
    return
  }
  if err = hbclient.Open(); err != nil {
    fmt.Print("Open() error :: ")
    fmt.Println(err)
    return
  }

  // Prepare input
  table := "tweets"
  user_id := req.FormValue("userid")
  raw_tweet_time := req.FormValue("tweet_time")
  tokens := strings.Split(raw_tweet_time, " ")
  tweet_time := strings.Join(tokens, "+")
  row_key := user_id + "|" + tweet_time
  fmt.Println("Query ", row_key)

  // Query
  if data, err := hbclient.Get(table, []byte(row_key), "tweet_id", nil); err != nil {
    fmt.Print("Error in query_hbase :: ")
    fmt.Println(err)
  } else {
    if data != nil && len(data) == 1 {
      str := string(data[0].Value)
      arr := strings.Split(str, ";")
      arr = arr[0:len(arr)-1]
      out := strings.Join(arr, "\n") + "\n"
      buffer.WriteString(out)
    }
  }

  hbclient.Close()
  resp.Write([]byte(buffer.String()))
}


func q1(resp http.ResponseWriter, req *http.Request) {
  var buffer bytes.Buffer
  var t = time.Now()
  buffer.WriteString("GiraffeLovers,3823-5293-0215\n")
  buffer.WriteString(fmt.Sprintf("%04d-%02d-%02d+%02d:%02d:%02d\n",t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second()))
  resp.Write([]byte(buffer.String()))
}

func q2(resp http.ResponseWriter, req *http.Request) {
  // query_mysql(resp, req)
  query_hbase(resp, req)
}

func connect_hbase() {
  // Connect to HBase
  address := "ec2-54-85-145-245.compute-1.amazonaws.com:9090"

  hbclient, err := goh.NewTcpClient(address, goh.TBinaryProtocol, false)
  if err != nil {
    fmt.Println(err)
    return
  }
  if err = hbclient.Open(); err != nil {
    fmt.Println(err)
    return
  }
}

func (s FastCGIServer) ServeHTTP(resp http.ResponseWriter, req *http.Request) {
  switch(req.URL.Path){
  case "/q1":
    q1(resp, req)
  case "/q2":
    q2(resp, req)
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

  listener,err:= net.Listen("tcp","127.0.0.1:9001")
  if err != nil {
    fmt.Println(err)
    return
  }
  cache = make(map[string]string)
  max_cache_size = 10000
  srv := new(FastCGIServer)

  //connect_hbase()
  //defer hbclient.Close()

  fcgi.Serve(listener, srv)

}
