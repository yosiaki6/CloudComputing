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
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	// Database
	user = "giraffe"
	pass = "giraffe"
	MAX_CONNECTION_COUNT = 4000
	Q4_SELECT            = "SELECT tweet_id, tweet_text FROM q4 WHERE tweet_time = ? ORDER BY tweet_id"

	RESP_FIRST_LINE = "GiraffeLovers,5148-7320-2582\n"
	TIME_FORMAT     = "2006-01-02 15:04:05"
)

var (
	db_server    = [5]string{"ec2-54-85-49-4.compute-1.amazonaws.com", "ec2-54-86-50-175.compute-1.amazonaws.com", "ec2-54-86-5-148.compute-1.amazonaws.com", "ec2-54-86-55-55.compute-1.amazonaws.com", "ec2-54-86-9-193.compute-1.amazonaws.com"}
	db_size      = [5]int64{14196778, 14196772, 14196791, 14196825, 14196692}
	db_conn_pool [][]*sql.DB
	index_pool   [][]int
	mutex_pool   []*sync.Mutex
	db           [5]*sql.DB
	q4_stmt      *sql.Stmt
	q6_stmt      *sql.Stmt
)

type Server struct{}

func (s Server) getConnetion(server_id int) (*sql.DB, int) {
	var index int
	mutex_pool[server_id].Lock()
	if len(index_pool[server_id]) == 0 {
	}
	index, index_pool[server_id] = index_pool[server_id][len(index_pool[server_id])-1], index_pool[server_id][:len(index_pool[server_id])-1]
	mutex_pool[server_id].Unlock()
	return db_conn_pool[server_id][index], index
}

func (s Server) releaseConnection(server_id int, index int) {
	mutex_pool[server_id].Lock()
	index_pool[server_id] = append(index_pool[server_id], index)
	mutex_pool[server_id].Unlock()
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	// Connect MySQL
	for server_index := 0; server_index < len(db_server); server_index++ {
		mutex_pool = append(mutex_pool, &sync.Mutex{})
		index_pool = append(index_pool, make([]int, 0))
		db_conn_pool = append(db_conn_pool, make([]*sql.DB, 0))
		for i := 0; i < MAX_CONNECTION_COUNT; i++ {
			index_pool[server_index] = append(index_pool[server_index], i)
			db_conn, err := sql.Open("mysql", user+":"+pass+"@tcp("+db_server[server_index]+":3306)/cloud")
			if err != nil {
				panic(err.Error()) // Just for example purpose. You should use proper error handling instead of panic
			}
			err = db_conn.Ping()
			if err != nil {
				log.Fatalf("Error on opening database connection: %s", err.Error())
			}
			db_conn_pool[server_index] = append(db_conn_pool[server_index], db_conn)
			defer db_conn_pool[server_index][i].Close()
		}
	}
	log.Println("Done")

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
	case "/q3":
		s.q3(resp, req)
	case "/q4":
		s.q4(resp, req)
	case "/q6":
		s.q6(resp, req)
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
	var tweet_id string
	user_id_string := req.FormValue("userid")
	tweet_time := req.FormValue("tweet_time")
	tweet_time = strings.Replace(tweet_time, " ", "+", 1)
	user_id_int64, err := strconv.ParseInt(user_id_string, 0, 64)
	var server_id int

	switch {
	case user_id_int64 <= 197834718:
		server_id = 0
	case 197834718 < user_id_int64 && user_id_int64 <= 396767602:
		server_id = 1
	case 396767602 < user_id_int64 && user_id_int64 <= 742870590:
		server_id = 2
	case 742870590 < user_id_int64 && user_id_int64 <= 1584744955:
		server_id = 3
	case 1584744955 < user_id_int64:
		server_id = 4
	}

	db, index := s.getConnetion(server_id)
	rows, err := db.Query("SELECT tweet_id FROM q2 WHERE user_id = ? and tweet_time = ?", user_id_int64, tweet_time)
	if err != nil {
		panic(err.Error()) // proper error handling instead of panic in your app
		return
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

	s.releaseConnection(server_id, index)

	resp.Write([]byte(buffer.String()))
}

func (s Server) q3(resp http.ResponseWriter, req *http.Request) {
	var buffer bytes.Buffer
	buffer.WriteString(RESP_FIRST_LINE)
	var retweet_users string
	user_id_string := req.FormValue("userid")
	tweet_time := req.FormValue("tweet_time")
	tweet_time = strings.Replace(tweet_time, " ", "+", 1)
	user_id_int64, err := strconv.ParseInt(user_id_string, 0, 64)
	var server_id int

	switch {
	case user_id_int64 < 171962889:
		server_id = 0
	case 171962889 <= user_id_int64 && user_id_int64 < 361283047:
		server_id = 1
	case 361283047 <= user_id_int64 && user_id_int64 < 591298618:
		server_id = 2
	case 591298618 <= user_id_int64 && user_id_int64 < 1344745392:
		server_id = 3
	case 1344745392 < user_id_int64:
		server_id = 4
	}

	db, index := s.getConnetion(server_id)
	err = db.QueryRow("SELECT retweet_users FROM q3 WHERE user_id = ?", user_id_int64).Scan(&retweet_users)
	if err != nil {
		panic(err.Error()) // proper error handling instead of panic in your app
		return
	}
	user_id_set := strings.Split(retweet_users, ";")
	for i := 0; i < len(user_id_set)-1; i++ {
		buffer.WriteString(user_id_set[i])
		buffer.WriteString("\n")
	}

	s.releaseConnection(server_id, index)

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

	// Select a db server from tweet_time
        var server_id int
        if tweet_time <= 1391214220000 {
          server_id = 0
        } else if tweet_time <= 1392843568000 {
          server_id = 1
        } else if tweet_time <= 1393717600000 {
          server_id = 2
        } else if tweet_time <= 1394577062000 {
          server_id = 3
        } else {
          server_id = 4
        }

        // Query
	db, index := s.getConnetion(server_id)
	rows, err := db.Query(Q4_SELECT, tweet_time)
	if err != nil {
		log.Fatalf("Error in query: %s", err.Error())
		return
	}

        // Get result
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

	s.releaseConnection(server_id, index)

	resp.Write([]byte(buffer.String()))
}

func (s Server) q6(resp http.ResponseWriter, req *http.Request) {
	var buffer bytes.Buffer
	buffer.WriteString(RESP_FIRST_LINE)
	finished_min := make(chan int64)
	finished_max := make(chan int64)

	var user int64
	var table_name string
	var user_min int64
	var user_max int64
	var server_id_min int
	var server_id_max int
	user = 0
	var err error
	user_min, err = strconv.ParseInt(req.FormValue("userid_min"), 10, 64)
	if err != nil {
		log.Fatalf("Parameter error user_min: %s", err.Error())
	}
	user_max, err = strconv.ParseInt(req.FormValue("userid_max"), 10, 64)
	if err != nil {
		log.Fatalf("Parameter error user_max: %s", err.Error())
	}

	switch {
	case user_min <= 197834718:
		server_id_min = 0
	case 197834718 < user_min && user_min <= 396767602:
		server_id_min = 1
	case 396767602 < user_min && user_min <= 742870590:
		server_id_min = 2
	case 742870590 < user_min && user_min <= 1584744955:
		server_id_min = 3
	case 1584744955 < user_min:
		server_id_min = 4
	}

	switch {
	case user_max <= 197834718:
		server_id_max = 0
	case 197834718 < user_max && user_max <= 396767602:
		server_id_max = 1
	case 396767602 < user_max && user_max <= 742870590:
		server_id_max = 2
	case 742870590 < user_max && user_max <= 1584744955:
		server_id_max = 3
	case 1584744955 < user_max:
		server_id_max = 4
	}

	go func() {
		var tmpUser int64
		db, index := s.getConnetion(server_id_min)
		err = db.QueryRow("select afterRowNum from q2 where user_id = ( select user_id from q2 where user_id >= ? limit 1) limit 1", user_min).Scan(&tmpUser)
		switch {
		case err == sql.ErrNoRows:
			tmpUser = 0
		case err != nil:
			log.Printf(table_name+":%d %d", user_min, user_max)
			log.Fatal(err)
		}
		s.releaseConnection(server_id_min, index)
		finished_min <- tmpUser
	}()

	go func() {
		var tmpUser int64
		db, index := s.getConnetion(server_id_max)
		err = db.QueryRow("select afterRowNum from q2 where user_id = ( select user_id from q2 where user_id > ? limit 1) limit 1", user_max).Scan(&tmpUser)
		switch {
		case err == sql.ErrNoRows:
			tmpUser = 0
		case err != nil:
			log.Printf(table_name+":%d %d", user_min, user_max)
			log.Fatal(err)
		}
		s.releaseConnection(server_id_max, index)
		finished_max <- tmpUser
	}()

	user += <-finished_min
	user -= <-finished_max
	for i := server_id_min + 1; i <= server_id_max; i++ {
		user += db_size[i]
	}

	buffer.WriteString(fmt.Sprintf("%d\n", user))

	resp.Write([]byte(buffer.String()))
}
