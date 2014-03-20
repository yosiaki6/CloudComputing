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
)

type FastCGIServer struct{}
type ApiHandler struct{}

// MySQL
var mysql_db *sql.DB
var statement *sql.Stmt
var tweet_id string
var err error
var cache map[string] string
var cache_keys []string
var max_cache_size int
var delete_cache_key string

// HBase
var hbase_client *goh.HClient

var db_type string
var db_address string
var default_header = "GiraffeLovers,5148-7320-2582\n"

func query_mysql(resp http.ResponseWriter, req *http.Request) {
  var buffer bytes.Buffer
  buffer.WriteString(default_header)
  user_id := req.FormValue("userid")
  tweet_time := req.FormValue("tweet_time")
  tweet_time = strings.Replace(tweet_time, " ", "+", 1)

  // Check for cached result
  cache_key := user_id + "_" + tweet_time
  result, ok := cache[cache_key]
  if ok {
    // Cache hit. Reply result from the cache
    //fmt.Printf("OK:%d\n", len(cache))
    resp.Write([]byte(result))
  } else {
    // Cache miss. Query result from the database
    if err != nil {
      fmt.Println("Cache error :: " + err.Error())
      return
    }
    rows, err := statement.Query(user_id, tweet_time)
    if err != nil {
      fmt.Println("Prepared statement error :: " + err.Error())
      return
    }

    for rows.Next(){
      err = rows.Scan(&tweet_id)
      if err != nil {
        panic(err.Error())
      }
      buffer.WriteString(tweet_id + "\n")
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
  buffer.WriteString(default_header)

  // Connect to HBase
  if hbase_client == nil {
    address := fmt.Sprintf("%s:9090", db_address)
    hbase_client, err := goh.NewTcpClient(address, goh.TBinaryProtocol, false)
    if err != nil {
      fmt.Print("NewTcpClient error :: ")
      fmt.Println(err)
      return
    }
    if err = hbase_client.Open(); err != nil {
      fmt.Print("Open() error :: ")
      fmt.Println(err)
      return
    }
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
  if data, err := hbase_client.Get(table, []byte(row_key), "tweet_id", nil); err != nil {
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

  hbase_client.Close()
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
  if db_type == "mysql" {
    query_mysql(resp, req)
  } else {
    query_hbase(resp, req)
  }
}

func connect_mysql() {
  mysql_db, err = sql.Open("mysql", fmt.Sprintf("giraffe:giraffe@tcp(%s:3306)/cloud", db_address))
  if err != nil {
    panic("sql.Open :: "+err.Error())  // Just for example purpose. You should use proper error handling instead of panic
  }
  //defer mysql_db.Close()

  statement, err = mysql_db.Prepare("SELECT tweet_id FROM plan1 WHERE user_id = ? and tweet_time = ?")
  if err != nil {
    panic("mysql_db.Prepare :: "+err.Error()) // proper error handling instead of panic in your app
  }
  //defer statement.Close()
}

func connect_hbase() {
  // Connect to HBase
  address := fmt.Sprintf("%s:9090", db_address)

  var err error
  hbase_client, err = goh.NewTcpClient(address, goh.TBinaryProtocol, false)
  if err != nil {
    fmt.Println(err)
    return
  }
  if err = hbase_client.Open(); err != nil {
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
 
  if len(os.Args) < 3 && (os.Args[1] != "mysql" || os.Args[1] != "hbase") {
    fmt.Println("PROGRAM <mysql or hbase> <database address>")
    return
  }
  db_type = os.Args[1]
  db_address = os.Args[2]
  fmt.Printf("Connecting to %s database at %s...\n", db_type, db_address)

  if db_type == "mysql" {
    connect_mysql()
  } else {
    connect_hbase()
  }
  fmt.Println("Database connected!")

  listener,err:= net.Listen("tcp","127.0.0.1:9001")
  if err != nil {
    fmt.Println(err)
    return
  }
  cache = make(map[string]string)
  max_cache_size = 10000
  srv := new(FastCGIServer)

  fcgi.Serve(listener, srv)

}
