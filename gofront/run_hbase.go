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
  //"errors"
)
var get_conn_mutex = &sync.Mutex{}
var return_conn_mutex = &sync.Mutex{}
var mutex = &sync.Mutex{}

type Server struct {}

const LISTEN_PORT = "80"
const POOL_SIZE = 100
var hbase_conn_pool [POOL_SIZE]*goh.HClient
var avail_conn_queue []*goh.HClient
var is_avail [POOL_SIZE]bool
var db_address = "ec2-54-208-229-92.compute-1.amazonaws.com" // *** Put HBase address here! ***
var default_header = "GiraffeLovers,5148-7320-2582\n"
var query_count = 0
var active_conn_count = 0

func q1(req *http.Request) (string, error) {
  var buffer bytes.Buffer
  buffer.WriteString(default_header)
  t := time.Now()
  buffer.WriteString(fmt.Sprintf("%04d-%02d-%02d+%02d:%02d:%02d\n",t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second()))

  return buffer.String(), nil
}

func q2(req *http.Request) (string, error) {
  //if (active_conn_count == 0) {
    //return "", errors.New("No connection to database.")
  //}

  var buffer bytes.Buffer
  buffer.WriteString(default_header)
  //fmt.Printf("%d Query %s\n", query_count, row_key)

  // Prepare
  table := "q2phase2"
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
    return "", err
  }

  // Print the result
  if data != nil && len(data) == 1 {
    buffer.WriteString(string(data[0].Value))
  }

  return buffer.String(), nil
}

const q3table = "q3phase2"

func q3(req *http.Request) (string, error) {
  //if (active_conn_count == 0) {
    //return "", errors.New("No connection to database.")
  //}

  var buffer bytes.Buffer
  buffer.WriteString(default_header)
  //fmt.Printf("%d Query %s\n", query_count, row_key)

  // Prepare
  user_id := req.FormValue("userid")
  query_count++

  // Query
  //conn := get_connection()
  var conn *goh.HClient
  var conn_index int
  for conn == nil {
    for i, value := range is_avail {
      if value == true {
        conn = hbase_conn_pool[i]
        conn_index = i
        is_avail[i] = false
        break
      }
    }
    time.Sleep(1)
  }
  data, err := conn.Get(q3table, []byte(user_id), "retweeter_id", nil)
  is_avail[conn_index] = true
  //return_connection(conn)

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
  get_conn_mutex.Lock()
  for len(avail_conn_queue) == 0 {
    // Wait until there's an available connection
    //fmt.Println("Connection full. Wait..")
    time.Sleep(1)
  }
  // Dequeue
  conn := avail_conn_queue[0]
  avail_conn_queue = avail_conn_queue[1:]
  //fmt.Println("Got. ", len(avail_conn_queue), " conns left")
  get_conn_mutex.Unlock()
  return conn
}

func return_connection(conn *goh.HClient) {
  return_conn_mutex.Lock()
  // Enqueue
  avail_conn_queue = append(avail_conn_queue, conn)
  //fmt.Println("Returned. ", len(avail_conn_queue), " conns available")
  return_conn_mutex.Unlock()
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
    return
  }
  // Try to keep the same amount of headers
  resp.Header().Set("Server", "gophr")
  resp.Header().Set("Content-Length", fmt.Sprint(len(body)))
  fmt.Fprint(resp, body)
}

func main() {
  // if len(os.Args) < 2 {
  //   fmt.Println("PROGRAM <hbase-address>")
  //   os.Exit(1)
  // }
  // db_address = os.Args[1]
  if (db_address == "") {
    fmt.Println("WARNING: No database address specified.")
  } else {
    fmt.Println("Database address:", db_address)

    for i := 0; i < POOL_SIZE; i++ {
      conn, _ := connect_hbase()
      if conn != nil {
        hbase_conn_pool[i] = conn
        is_avail[i] = true
        //active_conn_count += 1
        // hbase_conn_pool = append(hbase_conn_pool, conn)
        //avail_conn_queue = append(avail_conn_queue, conn)
        fmt.Println("Database connected! (", i, ")")
      } else {
        fmt.Println("Could not connect to database. (", i, ")")
      }
    }
  }

  // Start server
  sigchan := make(chan os.Signal, 1)
  signal.Notify(sigchan, os.Interrupt)
  signal.Notify(sigchan, syscall.SIGTERM)

  server := Server{}

  go func() {
    http.Handle("/q1", server)
    http.Handle("/q2", server)
    http.Handle("/q3", server)
    fmt.Println("Server started at port "+LISTEN_PORT)
    if err := http.ListenAndServe(":" + LISTEN_PORT, nil); err != nil {
        log.Fatal(err)
    }
  }()

  <-sigchan

}
