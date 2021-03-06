package main

import (
  "net/http"
  "bytes"
  "fmt"
  "strings"
  "time"
  "github.com/sdming/goh"
  "os"
  "os/signal"
  "sync"
  "syscall"
  "log"
  "strconv"
)
var get_conn_mutex = &sync.Mutex{}
var return_conn_mutex = &sync.Mutex{}
var mutex = &sync.Mutex{}

type Server struct {}

var listen_port = "80"
var pool_size = 1000
const MAX_POOL_SIZE = 25000
const QUEUE_WAIT_TIME= 1000
const Q2_TABLE = "q2phase2"
const Q3_TABLE = "q3phase2"
var db_address = "localhost" /*** Put HBase address here! ***/
const RESP_FIRST_LINE = "GiraffeLovers,5148-7320-2582\n"

var hbase_conn_pool [MAX_POOL_SIZE]*goh.HClient
var avail_conn_queue []*goh.HClient
var is_avail [MAX_POOL_SIZE]bool
var query_count = 0
var next_queue = 0

func q1(req *http.Request) (string, error) {
  var buffer bytes.Buffer
  buffer.WriteString(RESP_FIRST_LINE)
  t := time.Now()
  buffer.WriteString(fmt.Sprintf("%04d-%02d-%02d+%02d:%02d:%02d\n",t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second()))

  return buffer.String(), nil
}

func q2(req *http.Request) (string, error) {
  var buffer bytes.Buffer
  buffer.WriteString(RESP_FIRST_LINE)
  //fmt.Printf("%d Query %s\n", query_count, row_key)

  // Prepare
  table := "q2phase2"
  user_id := req.FormValue("userid")
  tweet_time := req.FormValue("tweet_time")
  tweet_time = strings.Replace(tweet_time, " ", "+", 1)
  row_key := tweet_time + "|" + user_id
  query_count++

  // Query
  var conn *goh.HClient
  var conn_index int
  for conn == nil {
    x := next_queue
    for i := 0; i < pool_size; i++ {
      x = (next_queue + i) % pool_size
      if is_avail[x] == true {
        conn = hbase_conn_pool[x]
        conn_index = x
        is_avail[x] = false
        next_queue = (next_queue + 1) % pool_size
        break
      }
    }
    if conn != nil {
      break
    }
    time.Sleep(QUEUE_WAIT_TIME)
  }
  data, err := conn.Get(table, []byte(row_key), "tweet_id", nil)
  is_avail[conn_index] = true

  // Handle error
  if err != nil {
    fmt.Printf("(%d) hbase_conn.Get :: %s\n", query_count, err.Error())
    return "", err
  }

  // Print the result
  if data != nil && len(data) == 1 {
    buffer.WriteString(string(data[0].Value))
  }

  return buffer.String(), nil
}

func q3(req *http.Request) (string, error) {
  var buffer bytes.Buffer
  buffer.WriteString(RESP_FIRST_LINE)
  //fmt.Printf("%d Query %s\n", query_count, row_key)

  // Prepare
  user_id := req.FormValue("userid")
  query_count++

  // Query
  var conn *goh.HClient
  var err error
  if conn, err = connect_hbase(); err != nil {
    fmt.Printf("(%d) connect_hbase :: %s\n", query_count, err.Error())
    return "", err
  }
  defer conn.Close()
  data, err2 := conn.Get(Q3_TABLE, []byte(user_id), "retweeter_id", nil)

  // Handle error
  if err2 != nil {
    fmt.Printf("(%d) hbase_conn.Get :: %s\n", query_count, err2.Error())
    return "", err
  }

  // Print the result
  if data != nil && len(data) == 1 {
    buffer.WriteString(string(data[0].Value))
  }

  return buffer.String(), nil
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

func (s Server) ServeHTTP(resp http.ResponseWriter, req *http.Request) {
  var body string
  var err error
  switch (req.URL.Path) {
  case "/q1":
    body, err = q1(req)
  case "/q2":
    body, err = q2(req)
  case "/q3":
    body, err = q3(req)
  }
  if err != nil {
    http.Error(resp, err.Error(), 500)
    fmt.Println(err.Error())
    return
  }
  // Try to keep the same amount of headers
  resp.Header().Set("Server", "gophr")
  resp.Header().Set("Content-Length", fmt.Sprint(len(body)))
  fmt.Fprint(resp, body)
}

func main() {
  if len(os.Args) >= 2 {
    listen_port = os.Args[1]
  }
  if len(os.Args) >= 3 {
    pool_size, _ = strconv.Atoi(os.Args[2])
  }
  if len(os.Args) >= 4 {
    db_address = os.Args[3]
  }
/*
  if (db_address == "") {
    fmt.Println("WARNING: No database address specified.")
  } else {
    fmt.Println("Database address:", db_address)
    fmt.Println("Establishing connections to database. Please wait..")

    conn_ok_count := 0
    for i := 0; i < pool_size; i++ {
      conn, _ := connect_hbase()
      if conn != nil {
        hbase_conn_pool[i] = conn
        is_avail[i] = true
        conn_ok_count += 1
        if conn_ok_count % 1000 == 0 {
          fmt.Println(conn_ok_count, "from", pool_size)
        }
      } else {
        //fmt.Println("Could not connect to database. (", i, ")")
      }
    }
    fmt.Println(conn_ok_count, "connections connected!")
  }
*/
  // Start server
  sigchan := make(chan os.Signal, 1)
  signal.Notify(sigchan, os.Interrupt)
  signal.Notify(sigchan, syscall.SIGTERM)

  server := Server{}

  go func() {
    http.Handle("/q1", server)
    http.Handle("/q2", server)
    http.Handle("/q3", server)
    fmt.Println("Server started at port "+listen_port)
    if err := http.ListenAndServe(":" + listen_port, nil); err != nil {
        log.Fatal(err)
    }
  }()

  <-sigchan

}
