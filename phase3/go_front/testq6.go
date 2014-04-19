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
	"syscall"
	"time"
)

const (
	// Database
	/*
	   db_server_1 = "ec2-54-86-13-44.compute-1.amazonaws.com"
	   db_server_2 = "ec2-54-86-30-1.compute-1.amazonaws.com"
	   db_server_3 = "ec2-54-86-21-73.compute-1.amazonaws.com"
	   db_server_4 = "ec2-54-86-11-160.compute-1.amazonaws.com"
	   db_server_5 = "ec2-54-85-190-60.compute-1.amazonaws.com"
	*/
	user = "giraffe"
	pass = "giraffe"
	//  CONNECTION_STRING     = "giraffe:giraffe@tcp(localhost:3306)/cloud"
	MAX_CONNECTION_COUNT = 20
	Q4_SELECT            = "SELECT tweet_id, tweet_text FROM q4 WHERE tweet_time = ? ORDER BY tweet_id"
	Q6_SELECT            = "SELECT count(*) FROM q6 WHERE user_min = ? AND user_max = ?"

	RESP_FIRST_LINE = "GiraffeLovers,5148-7320-2582\n"
	TIME_FORMAT     = "2006-01-02 15:04:05"
)

var (
	db_server = [5]string{"ec2-54-86-13-44.compute-1.amazonaws.com", "ec2-54-86-30-1.compute-1.amazonaws.com", "ec2-54-86-21-73.compute-1.amazonaws.com", "ec2-54-86-11-160.compute-1.amazonaws.com", "ec2-54-85-190-60.compute-1.amazonaws.com"}
	db_size   = [5]int64{14196778, 14196772, 14196791, 14196825, 14196692}
	db        [5]*sql.DB
	q4_stmt   *sql.Stmt
	q6_stmt   *sql.Stmt
)

type Server struct{}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	// Connect MySQL
	var err error
	for i := 0; i < 5; i++ {
		db[i], err = sql.Open("mysql", user+":"+pass+"@tcp("+db_server[i]+":3306)/cloud")
		if err != nil {
			log.Fatalf("Error %s", err.Error())
		}

		db[i].SetMaxIdleConns(MAX_CONNECTION_COUNT)
		db[i].SetMaxOpenConns(MAX_CONNECTION_COUNT)
		err = db[i].Ping() // This DOES open a connection if necessary. This makes sure the database is accessible
		if err != nil {
			log.Fatalf("Error on opening database connection: %s", err.Error())
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
	finished := make(chan int64)

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

	if server_id_min == server_id_max {
		switch {
		case server_id_min == 0:
			table_name = "q2_1"
		case server_id_min == 1:
			table_name = "q2_2"
		case server_id_min == 2:
			table_name = "q2_3"
		case server_id_min == 3:
			table_name = "q2_4"
		case server_id_min == 4:
			table_name = "q2_5"
		}		
		err = db[server_id_min].QueryRow("select count(*) from " + table_name + " where user_id between ? and ?",  user_min, user_max).Scan(&user)
		switch {
			case err == sql.ErrNoRows:
			log.Printf("No user with that ID.")
			case err != nil:
			log.Printf(table_name + ":%d %d", user_min, user_max)
			log.Fatal(err)
		}
	} else {
		go func(){
		var tmp_user int64
		switch {
		case server_id_min == 0:
			table_name = "q2_1"
		case server_id_min == 1:
			table_name = "q2_2"
		case server_id_min == 2:
			table_name = "q2_3"
		case server_id_min == 3:
			table_name = "q2_4"
		case server_id_min == 4:
			table_name = "q2_5"
		}		
		err = db[server_id_min].QueryRow("select count(*) from " + table_name + " where user_id >= ?", user_min).Scan(&tmp_user)
		switch {
			case err == sql.ErrNoRows:
			log.Printf("No user with that ID.")
			case err != nil:
			log.Printf(table_name + ":%d", user_min)
			log.Fatal(err)
		}
		finished <- tmp_user
		}()

		go func(){
		var tmp_user int64
		switch {
		case server_id_max == 0:
			table_name = "q2_1"
		case server_id_max == 1:
			table_name = "q2_2"
		case server_id_max == 2:
			table_name = "q2_3"
		case server_id_max == 3:
			table_name = "q2_4"
		case server_id_max == 4:
			table_name = "q2_5"
		}		
		err = db[server_id_max].QueryRow("select count(*) from " + table_name +"  where user_id <= ?",  user_max).Scan(&tmp_user)
		switch {
			case err == sql.ErrNoRows:
			log.Printf("No user with that ID.")
			case err != nil:
			log.Printf(table_name + ":%d", user_max)
			log.Fatal(err)
		}
		finished <- tmp_user
		}()
		
		user += <- finished
		user += <- finished
		for i:= server_id_min +1; i < server_id_max; i++{
			user += db_size[i]
		}
	}

	buffer.WriteString(fmt.Sprintf("%d\n", user))

	resp.Write([]byte(buffer.String()))
}
