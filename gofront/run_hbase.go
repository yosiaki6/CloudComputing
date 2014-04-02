package main

import (
  "net"
  "net/http"
  "net/http/fcgi"
  "bytes"
  "fmt"
  "strings"
  "time"
  "github.com/sdming/goh"
  "os"
  "sync"
)
var mutex = &sync.Mutex{}

type FastCGIServer struct{}
type ApiHandler struct{}

// HBase
const POOL_SIZE = 112
var hbase_conn_pool [POOL_SIZE]*goh.HClient
var avail_conn_queue []*goh.HClient

var db_address string
var default_header = "GiraffeLovers,5148-7320-2582\n"
var query_count = 0

func q1(resp http.ResponseWriter, req *http.Request) {
  var buffer bytes.Buffer
  var t = time.Now()
  buffer.WriteString(default_header)
  buffer.WriteString(fmt.Sprintf("%04d-%02d-%02d+%02d:%02d:%02d\n",t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second()))
  resp.Write([]byte(buffer.String()))
}

func q2(resp http.ResponseWriter, req *http.Request) {
  var buffer bytes.Buffer
  buffer.WriteString(default_header)
  //fmt.Printf("%d Query %s\n", query_count, row_key)

  // Prepare
  table := "tweets"
  user_id := req.FormValue("userid")
  tweet_time := req.FormValue("tweet_time")
  tweet_time = strings.Replace(tweet_time, " ", "+", 1)
  row_key := user_id + "|" + tweet_time
  query_count++

  // Query
  conn := get_connection()
  data, err := conn.Get(table, []byte(row_key), "tweet_id", nil)
  return_connection(conn)

  // Handle error
  if err != nil {
    fmt.Printf("(%d) hbase_conn.Get :: %s\n", query_count, err.Error())
    return
  }

  // Print the result
  if data != nil && len(data) == 1 {
    buffer.WriteString(string(data[0].Value))
  }
  fmt.Fprintf(resp, "%s", buffer.String())
}

func q3(resp http.ResponseWriter, req *http.Request) {
  var buffer bytes.Buffer
  buffer.WriteString(default_header)
  //fmt.Printf("%d Query %s\n", query_count, row_key)

  // Prepare
  table := "q3phase2"
  user_id := req.FormValue("userid")
  query_count++

  // Query
  conn := get_connection()
  data, err := conn.Get(table, []byte(user_id), "retweeter_id", nil)
  return_connection(conn)

  // Handle error
  if err != nil {
    fmt.Printf("(%d) hbase_conn.Get :: %s\n", query_count, err.Error())
    return
  }

  // Print the result
  if data != nil && len(data) == 1 {
    buffer.WriteString(string(data[0].Value))
  }
  fmt.Fprintf(resp, "%s", buffer.String())
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

func get_connection() *goh.HClient {
  mutex.Lock()
  for len(avail_conn_queue) == 0 {
    // Wait until there's an available connection
    //fmt.Println("Connection full. Wait..")
    time.Sleep(1)
  }
  // Dequeue
  conn := avail_conn_queue[0]
  avail_conn_queue = avail_conn_queue[1:]
  //fmt.Println("Got. ", len(avail_conn_queue), " conns left")
  mutex.Unlock()
  return conn
}

func return_connection(conn *goh.HClient) {
  // Enqueue
  avail_conn_queue = append(avail_conn_queue, conn)
  //fmt.Println("Returned. ", len(avail_conn_queue), " conns available")
}

func (s FastCGIServer) ServeHTTP(resp http.ResponseWriter, req *http.Request) {
  switch (req.URL.Path) {
  case "/q1":
    q1(resp, req)
  case "/q2":
    q2(resp, req)
  case "/q3":
    q3(resp, req)
  }
}

func main() {
  if len(os.Args) < 2 {
    fmt.Println("PROGRAM <hbase-address>")
    return
  }
  db_address = os.Args[1]
  fmt.Printf("HBase address: %s\n", db_address)

  for i := 0; i < POOL_SIZE; i++ {
    hbase_conn_pool[i], _ = connect_hbase()
    if hbase_conn_pool[i] != nil {
      avail_conn_queue = append(avail_conn_queue, hbase_conn_pool[i])
      fmt.Println("HBase server connected! (", i, ")")
    } else {
      fmt.Println("Could not connect to HBase server. (", i, ")")
    }
  }

  listener, err := net.Listen("tcp",":9001")
  if err != nil {
    fmt.Println("Listen 127.0.0.1:9001 :: " + err.Error())
    os.Exit(3)
  }
  srv := new(FastCGIServer)
  fcgi.Serve(listener, srv)

}
