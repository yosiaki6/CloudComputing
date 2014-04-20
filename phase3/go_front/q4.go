package main

import (
  "bytes"
  "database/sql"
  "fmt"
  "log"
  "net/http"
  "os"
  "os/signal"
  "syscall"
  "time"
  "runtime"
  "strings"
  "strconv"
  _ "github.com/go-sql-driver/mysql"
)

const (
  // Database
  CONNECTION_STRING     = "giraffe:giraffe@tcp(localhost:3306)/cloud"
  CONN_STRING           = "giraffe:giraffe@tcp(%s:3306)/cloud"
  MAX_CONNECTION_COUNT  = 256
  Q2_SELECT             = "SELECT tweet_id FROM q2 WHERE user_id = ? AND tweet_time = ?"
  Q4_SELECT             = "SELECT tweet_id, tweet_text FROM q4 WHERE tweet_time = ? ORDER BY tweet_id"
  Q6_SELECT             = "SELECT count(*) FROM q6 WHERE user_min = ? AND user_max = ?"

  RESP_FIRST_LINE       = "GiraffeLovers,5148-7320-2582\n"
  TIME_FORMAT           = "2006-01-02 15:04:05"
  BACKEND_COUNT         = 5
)

var (
  db         *sql.DB
  backend    [BACKEND_COUNT]*sql.DB
  q2_stmt    *sql.Stmt
  q4_stmt    *sql.Stmt
  upbound = []int64 {1000,2000,3000,4000} // unlimited upper bound for backend[4]
  address = []string {
    "ec2-54-85-49-4.compute-1.amazonaws.com",
    "ec2-54-86-50-175.compute-1.amazonaws.com",
    "ec2-54-86-5-148.compute-1.amazonaws.com",
    "ec2-54-86-55-55.compute-1.amazonaws.com",
    "ec2-54-86-9-193.compute-1.amazonaws.com",
  }
  q6_stmt    *sql.Stmt
)

type Server struct{}

func main() {
  runtime.GOMAXPROCS(runtime.NumCPU())

  // Connect MySQL
  var err error
  
  db, err = sql.Open("mysql", CONNECTION_STRING)
  if err != nil {
    log.Fatalf("Error %s", err.Error())
  }
  db.SetMaxIdleConns(MAX_CONNECTION_COUNT)
  err = db.Ping() // This DOES open a connection if necessary. This makes sure the database is accessible
  if err != nil {
    log.Fatalf("Error on opening database connection: %s", err.Error())
  }
  
  for i, v := range address {
    backend[i], err = sql.Open("mysql", fmt.Sprintf(CONN_STRING, v))
    if err != nil {
      log.Fatalf("Error")
    }
    if err = db.Ping(); err != nil {
      log.Fatalf("Error")
    }
  }

  // Prepare statements
  q2_stmt, err = db.Prepare(Q2_SELECT)
  if err != nil {
    log.Fatalf("Error preparing q2 statement: %s", err.Error())
  }
  q4_stmt, err = db.Prepare(Q4_SELECT)
  if err != nil {
    log.Fatalf("Error preparing q4 statement: %s", err.Error())
  }
  /*
  q6_stmt, err = db.Prepare(Q6_SELECT)
  if err != nil {
    log.Fatalf("Error preparing q6 statement: %s", err.Error())
  }
  */


  // Start web server 
  server := Server{}
  go func() {
    http.Handle("/", server)
    if err := http.ListenAndServe(":80", nil); err != nil {
      log.Fatalf("Error starting server: %s", err.Error())
    }
  }()
  fmt.Println("Server started")

  // Block until interrupted or SIGTERM
  sigchan := make(chan os.Signal, 1)
  signal.Notify(sigchan, os.Interrupt)
  signal.Notify(sigchan, syscall.SIGTERM)
  <-sigchan
}

func (s Server) ServeHTTP(resp http.ResponseWriter, req *http.Request) {
  switch req.URL.Path {
  case "/q1":
    s.q1(resp, req)
  case "/q2":
    s.q2(resp, req)
  case "/q4":
    s.q4(resp, req)
    //resp.Write([]byte("👍"))
  }
}

func (s Server) q1(resp http.ResponseWriter, req *http.Request) {
  var buffer bytes.Buffer
  var t = time.Now()
  buffer.WriteString(RESP_FIRST_LINE)
  buffer.WriteString(fmt.Sprintf("%04d-%02d-%02d+%02d:%02d:%02d\n", t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second()))
  resp.Write([]byte(buffer.String()))
}

func (s Server) q2(resp http.ResponseWriter, req *http.Request) {
  var buffer bytes.Buffer
  buffer.WriteString(RESP_FIRST_LINE)

  user_id := req.FormValue("userid")
  user_id_int, err := strconv.ParseInt(user_id, 10, 64)
  if err != nil {
    panic(err.Error())
    return
  }
  tweet_time := req.FormValue("tweet_time")
  tweet_time = strings.Replace(tweet_time, " ", "+", 1)

  conn := backend[BACKEND_COUNT-1]
  for i, v := range upbound {
    if user_id_int < v {
      conn = backend[i]
    }
  }
  var rows *sql.Rows
  rows, err = conn.Query(Q2_SELECT, user_id, tweet_time)
  if err != nil {
    panic(err.Error())
    return
  }
  var tweet_id int64
  for rows.Next() {
    err = rows.Scan(&tweet_id)
    if err != nil {
      panic(err.Error())
      return
    }
    buffer.WriteString(fmt.Sprintf("%d\n",tweet_id))
  }

  resp.Write([]byte(buffer.String()))
}

func (s Server) q4(resp http.ResponseWriter, req *http.Request) {
  var buffer bytes.Buffer
  buffer.WriteString(RESP_FIRST_LINE)

  // Get time param (must change to ms before query)
  input := strings.TrimSpace(req.FormValue("time"))
  t, err := time.Parse(TIME_FORMAT, input)
  if err != nil {
    log.Fatalf("Parameter error: %s", err.Error())
    return
  }
  tweet_time := t.Unix() * 1000
  //fmt.Println(input, "=>", tweet_time)

  // Query
  rows, err := q4_stmt.Query(tweet_time)
  if err != nil {
    log.Fatalf("Error in query: %s", err.Error())
    return
  }
  var tweet_id int64
  var tweet_text string
  for rows.Next() {
    err = rows.Scan(&tweet_id, &tweet_text)
    if err != nil {
      log.Fatalf("Error in rows scan: %s", err.Error())
      return
    }
    buffer.WriteString(fmt.Sprintf("%d:%s\n", tweet_id, tweet_text))
  }

  resp.Write([]byte(buffer.String()))
}

func (s Server) q6(resp http.ResponseWriter, req *http.Request) {
  var buffer bytes.Buffer
  buffer.WriteString(RESP_FIRST_LINE)

  var user_min int64
  var user_max int64
  var err error
  user_min, err = strconv.ParseInt(req.FormValue("user_min"), 10, 64)
  if err != nil {
    log.Fatalf("Parameter error user_min: %s", err.Error())
  }
  user_max, err = strconv.ParseInt(req.FormValue("user_max"), 10, 64)
  if err != nil {
    log.Fatalf("Parameter error user_max: %s", err.Error())
  }

  // Query
  var count int64
  err = q6_stmt.QueryRow(user_min, user_max).Scan(&count)
  if err != nil {
    log.Fatalf("Error in query: %s", err.Error())
    return
  }
  buffer.WriteString(fmt.Sprintf("%d\n", count))

  resp.Write([]byte(buffer.String()))
}


