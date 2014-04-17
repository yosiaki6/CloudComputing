package main

import (
	"bytes"
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

var (
	abort bool
)

type Server struct{}

const POOL_SIZE = 0
const BACKEND_SIZE = 3

var db_conn_pool [][]*sql.DB
var stmtOut_pool [][]*sql.Stmt
var index_pool [][]int

func (s Server) q1(resp http.ResponseWriter, req *http.Request) {
	var buffer bytes.Buffer
	var t = time.Now()
	buffer.WriteString("GiraffeLovers,3823-5293-0215\n")
	buffer.WriteString(fmt.Sprintf("%04d-%02d-%02d+%02d:%02d:%02d\n", t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second()))
	resp.Write([]byte(buffer.String()))
}

func (s Server) q2(resp http.ResponseWriter, req *http.Request) {
	var tweet_id string
	var buffer bytes.Buffer
	buffer.WriteString("GiraffeLovers,3823-5293-0215\n")
	user_id := req.FormValue("userid")
	tweet_time := req.FormValue("tweet_time")
	tweet_time = strings.Replace(tweet_time, " ", "+", 1)

	var index [BACKEND_SIZE]int
	query_finished := make(chan bool)

	_server_index := 0
	go func(server_index int) {
		var db_conn *sql.DB
		db_conn, index[server_index] = s.getConnetion(server_index)

		rows, err := db_conn.Query("SELECT tweet_id FROM q2 WHERE user_id = ? and tweet_time = ?", user_id, tweet_time)
		if err != nil {
			db_conn, err = sql.Open("mysql", "giraffe:giraffe@tcp(backend-2058911627.us-east-1.elb.amazonaws.com:3306)/cloud")
			if err != nil {
				panic(err.Error()) // Just for example purpose. You should use proper error handling instead of panic
				return
			}
			rows, err = db_conn.Query("SELECT tweet_id FROM q2 WHERE user_id = ? and tweet_time = ?", user_id, tweet_time)
			if err != nil {
				panic(err.Error()) // proper error handling instead of panic in your app
				return
			}
		}

		for rows.Next() {
			err = rows.Scan(&tweet_id)
			if err != nil {
				panic(err.Error())
				return
			}
			buffer.WriteString(tweet_id)
			buffer.WriteString("\n")
		}
		query_finished <- true
	}(_server_index)

	for i := 0; i < BACKEND_SIZE; i++ {
		<-query_finished
	}
	s.releaseConnection(0, index[_server_index])

	resp.Write([]byte(buffer.String()))
}

func (s Server) getConnetion(server_id int) (*sql.DB, int) {
	if len(index_pool[server_id]) == 0 {
	}
	var index int
	index_pool[server_id], index = index_pool[server_id][:len(index_pool[server_id])-1], index_pool[server_id][len(index_pool[server_id])-1]
	return db_conn_pool[server_id][index], index
}

func (s Server) releaseConnection(server_id int, index int) {
	index_pool[server_id] = append(index_pool[server_id], index)
}

func (s Server) ServeHTTP(resp http.ResponseWriter, req *http.Request) {
	switch req.URL.Path {
	case "/q1":
		s.q1(resp, req)
	case "/q2":
		s.q2(resp, req)
		/*
		   case "/q3":
		     s.q3(resp, req)
		*/
	}
}

func main() {
	sigchan := make(chan os.Signal, 1)
	finished := make(chan bool)
	signal.Notify(sigchan, os.Interrupt)
	signal.Notify(sigchan, syscall.SIGTERM)
	for server_index := 0; server_index < BACKEND_SIZE; server_index++ {
		index_pool = append(index_pool, make([]int, 0))
		db_conn_pool = append(db_conn_pool, make([]*sql.DB, 0))
		for i := 0; i < POOL_SIZE; i++ {
			index_pool[server_index] = append(index_pool[server_index], i)
			db_conn, err := sql.Open("mysql", "giraffe:giraffe@tcp(backend-2058911627.us-east-1.elb.amazonaws.com:3306)/cloud")
			if err != nil {
				panic(err.Error()) // Just for example purpose. You should use proper error handling instead of panic
			}
			db_conn_pool[server_index] = append(db_conn_pool[server_index], db_conn)
			defer db_conn_pool[server_index][i].Close()
		}
	}

	/*
		for i := 0; i < POOL_SIZE; i++ {
			stmtOut, err := db_conn_pool[i].Prepare("SELECT tweet_id FROM q2 WHERE user_id = ? and tweet_time = ?")
			if err != nil {
				panic(err.Error()) // proper error handling instead of panic in your app
			}
			defer stmtOut_pool[i].Close()
			stmtOut_pool = append(stmtOut_pool, stmtOut)
		}
	*/
	fmt.Print("Done\n")
	j := 0

	log.Print("started.")

	funcs := []func(){
		func() {
			log.Print("sleep1 started.")
			log.Printf("1:%d\n", j)
			time.Sleep(1 * time.Second)
			log.Print("sleep1 finished.")
			finished <- true
		},
		func() {
			log.Print("sleep2 started.")
			log.Printf("2:%d\n", j)
			time.Sleep(1 * time.Second)
			time.Sleep(2 * time.Second)
			log.Print("sleep2 finished.")
			finished <- true
		},
		func() {
			log.Print("sleep3 started.")
			log.Printf("3:%d\n", j)
			time.Sleep(1 * time.Second)
			time.Sleep(3 * time.Second)
			j++
			log.Print("sleep3 finished.")
			finished <- true
		},
	}

	for _, sleep := range funcs {
		go sleep()
	}

	for i := 0; i < len(funcs); i++ {
		<-finished
	}

	log.Print("all finished.")

	server := Server{}

	go func() {
		http.Handle("/", server)
		if err := http.ListenAndServe(":80", nil); err != nil {
			log.Fatal(err)
		}
	}()

	<-sigchan
}
