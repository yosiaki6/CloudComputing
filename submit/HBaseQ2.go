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
  "github.com/sdming/goh"
  "os"
  "sync"
)
var mutex = &sync.Mutex{}

type FastCGIServer struct{}
type ApiHandler struct{}

// MySQL
var mysql_conn *sql.DB
var statement *sql.Stmt
var cache map[string] string
var cache_keys []string
var max_cache_size int
var delete_cache_key string
var cache_hit_count = 0
var cache_miss_count = 0

// HBase
const POOL_SIZE = 10
var hbase_conn_pool [POOL_SIZE]*goh.HClient
var is_conn_avail [POOL_SIZE]bool

var db_type string
var db_address string
var default_header = "GiraffeLovers,5148-7320-2582\n"
var query_count = 0

func query_mysql(resp http.ResponseWriter, req *http.Request) {
  var buffer bytes.Buffer
  var tweet_id string

  buffer.WriteString(default_header)
  user_id := req.FormValue("userid")
  tweet_time := req.FormValue("tweet_time")
  tweet_time = strings.Replace(tweet_time, " ", "+", 1)

  // Check for cached result
  cache_key := user_id + "_" + tweet_time
  result, _ := cache[cache_key]
  if false {
    // Cache hit. Reply result from the cache
    cache_hit_count++
    //fmt.Printf("OK:%d\n", len(cache))
    fmt.Fprintf(resp, "%s", result)

  } else {
    // Cache miss. Query result from the database
    cache_miss_count++

    mutex.Lock()
    rows, err := statement.Query(user_id, tweet_time)
    mutex.Unlock()
    if err != nil {
      fmt.Println("statement.Query :: " + err.Error())
      return
    }

    for rows.Next(){
      err = rows.Scan(&tweet_id)
      if err != nil {
        fmt.Println("rows.Scan error :: " + err.Error())
      }
      buffer.WriteString(tweet_id + "\n")
    }

    mutex.Lock()
    if(len(cache) >= max_cache_size){
      delete(cache, cache_keys[0])
      cache_keys = cache_keys[1:len(cache_keys)]
    }
    cache_keys = append(cache_keys, cache_key) 
    cache[cache_key] = buffer.String()
    mutex.Unlock()
     //fmt.Print(cache_keys[0])

    fmt.Fprintf(resp, "%s", buffer.String())

  }
}

func query_hbase(resp http.ResponseWriter, req *http.Request) {
  var buffer bytes.Buffer
  buffer.WriteString(default_header)

  // Prepare input
  table := "tweets"
  user_id := req.FormValue("userid")
  tweet_time := req.FormValue("tweet_time")
  tweet_time = strings.Replace(tweet_time, " ", "+", 1)
  row_key := user_id + "|" + tweet_time
  query_count++
  //fmt.Printf("%d Query %s\n", query_count, row_key)

  // Find available connection
  var conn_index = 0
  var result = false
  for conn_index, result = range is_conn_avail {
    if result == true {
      break
    }
  }

  // Query
  mutex.Lock()
  is_conn_avail[conn_index] = false
  data, err := hbase_conn_pool[conn_index].Get(table, []byte(row_key), "tweet_id", nil)
  is_conn_avail[conn_index] = true
  mutex.Unlock()

  if err != nil {
    fmt.Printf("(%d) hbase_conn.Get :: %s\n", query_count, err.Error())
    return //os.Exit(3)
  }
  if data != nil && len(data) == 1 {
    buffer.WriteString(string(data[0].Value))
  }

  fmt.Fprintf(resp, "%s", buffer.String())
}


func q1(resp http.ResponseWriter, req *http.Request) {
  var buffer bytes.Buffer
  var t = time.Now()
  buffer.WriteString(default_header)
  buffer.WriteString(fmt.Sprintf("%04d-%02d-%02d+%02d:%02d:%02d\n",t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second()))
  resp.Write([]byte(buffer.String()))
}

func q2(resp http.ResponseWriter, req *http.Request) {
  if db_type == "mysql" {
    query_mysql(resp, req)
  } else {
    query_hbase(resp, req)
  }
}

func connect_mysql() {
  var err error

  mysql_conn, err = sql.Open("mysql", fmt.Sprintf("giraffe:giraffe@tcp(%s:3306)/cloud", db_address))
  //defer mysql_conn.Close()
  if err != nil {
    panic("sql.Open :: "+err.Error())  // Just for example purpose. You should use proper error handling instead of panic
  }

  statement, err = mysql_conn.Prepare("SELECT tweet_id FROM plan1 WHERE user_id = ? and tweet_time = ?")
  if err != nil {
    panic("mysql_conn.Prepare :: "+err.Error())
  }
}

func connect_hbase() (conn *goh.HClient, err error) {
  address := fmt.Sprintf("%s:9090", db_address)
  if conn, err = goh.NewTcpClient(address, goh.TBinaryProtocol, false); err != nil {
    fmt.Println("NewTcpClient :: " + err.Error())
    return nil, err //os.Exit(3)
  }
  if err = conn.Open(); err != nil {
    fmt.Println("Open :: " + err.Error())
    return nil, err //os.Exit(3)
  }
  return conn, err
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
  if len(os.Args) < 3 || (os.Args[1] != "mysql" && os.Args[1] != "hbase") {
    fmt.Println("PROGRAM <mysql or hbase> <database address>")
    return
  }
  db_type = os.Args[1]
  db_address = os.Args[2]
  fmt.Printf("%s %s\n", db_type, db_address)

  if db_type == "mysql" {
    connect_mysql()
    defer mysql_conn.Close()
    fmt.Println("MySQL server connected!")
  } else {
    for i := 0; i < POOL_SIZE; i++ {
      hbase_conn_pool[i], _ = connect_hbase()
      if hbase_conn_pool[i] != nil {
        is_conn_avail[i] = true
        fmt.Println("HBase server connected! (", i, ")")
      } else {
        fmt.Println("Could not connect to HBase server. (", i, ")")
      }
    }
  }
  listener, err := net.Listen("tcp",":9001")
  if err != nil {
    fmt.Println("Listen 127.0.0.1:9001 :: " + err.Error())
    os.Exit(3)
  }
  cache = make(map[string]string)
  max_cache_size = 10000
  srv := new(FastCGIServer)

  fcgi.Serve(listener, srv)

}
