package main

import (
  "bytes"
  "database/sql"
  "fmt"
  "log"
  "net/http"
  "os"
  "os/signal"
  "strconv"
  "strings"
  "syscall"
  "time"
  "log"
  "runtime"
  _ "github.com/go-sql-driver/mysql"
)

const (
  // Database
  CONNECTION_STRING     = "giraffe:giraffe@tcp(localhost:3306)/cloud"
  MAX_CONNECTION_COUNT  = 256
  Q4_SELECT             = "SELECT * FROM q4 WHERE tweet_time = ?"

  RESP_FIRST_LINE       = "GiraffeLovers,5148-7320-2582\n"
)

var (
  db         *sql.DB
  q4_stmt    *sql.Stmt
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

  // Prepare statements
  q4_stmt, err = db.Prepare(Q4_SELECT)
  if err != nil {
    log.Fatalf("Error preparing q4 statement: %s", err.Error())
  }

  // Start web server 
  server := Server{}
  go func() {
    http.Handle("/", server)
    if err := http.ListenAndServe(":80", nil); err != nil {
      log.Fatalf("Error starting server: %s", err.Error())
    }
  }()

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
   case "/q4":
     s.q4(resp, req)
  // case "/q6":
    // s.q6(resp, req)
  }
}

func (s Server) q1(resp http.ResponseWriter, req *http.Request) {
  var buffer bytes.Buffer
  var t = time.Now()
  buffer.WriteString(RESP_FIRST_LINE)
  buffer.WriteString(fmt.Sprintf("%04d-%02d-%02d+%02d:%02d:%02d\n", t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second()))
  resp.Write([]byte(buffer.String()))
}

func (s Server) q4(resp http.ResponseWriter, req *http.Request) {
  var buffer bytes.Buffer
  buffer.WriteString(RESP_FIRST_LINE)

  // Take parameters
  tweet_time, err := strconv.ParseInt(req.FormValue("time"), 10, 64)
  if err != nil {
    log.Fatalf("Parameter error: %s", err.Error())
    return
  }

  // Query
  rows, err := q4_stmt.Query(Q4_SELECT, tweet_time)
  if err != nil {
    log.Fatalf("Error in query: %s", err.Error())
    return
  }
  for rows.Next() {
    err = rows.Scan(&tweet_id)
    if err != nil {
      log.Fatalf("Error in rows scan: %s", err.Error())
      return
    }
    buffer.WriteString(tweet_id)
  }

  resp.Write([]byte(buffer.String()))
}

