package main

import (
    "fmt"
    "log"
    "net/http"
    "os"
    "os/signal"
    "syscall"
    "time"
    "bytes"
)

var (
    abort bool
)

const (
    SOCK = "/tmp/go.sock"
)

type Server struct {
}

func (s Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
  var buffer bytes.Buffer
  var t = time.Now()
  buffer.WriteString("GiraffeLovers,3823-5293-0215\n")
  buffer.WriteString(fmt.Sprintf("%04d-%02d-%02d+%02d:%02d:%02d\n",t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second()))
  var body = buffer.String()

    // Try to keep the same amount of headers
    w.Header().Set("Server", "gophr")
    w.Header().Set("Content-Length", fmt.Sprint(len(body)))
    fmt.Fprint(w, body)
}

func main() {
    sigchan := make(chan os.Signal, 1)
    signal.Notify(sigchan, os.Interrupt)
    signal.Notify(sigchan, syscall.SIGTERM)

    server := Server{}

    go func() {
        http.Handle("/q1", server)
        if err := http.ListenAndServe(":80", nil); err != nil {
            log.Fatal(err)
        }
    }()

    <-sigchan
}
