package main

import (
        "database/sql"
        "fmt"
        _ "github.com/go-sql-driver/mysql"
        "log"
)

const ( 
        SERVER = "ec2-54-86-42-102.compute-1.amazonaws.com"
        USER   = "giraffe"
        PASS   = "giraffe"
)

var db *sql.DB

func main() {
        db, err := sql.Open("mysql", USER+":"+PASS+"@tcp("+SERVER+":3306)/cloud")
        if err != nil {
                log.Fatalf("Error opening database: %v", err)
        }
        db.SetMaxOpenConns(10)
        stmt, err := db.Prepare("select count(*) from plan1")
        if err != nil {
                log.Fatal(err)
        }

        finished := make(chan bool)

        for i := 0; i < 1000; i++ {
                go func() {
                        var count int64
                        stmt.QueryRow().Scan(&count)
                        log.Println(count)
                        finished <- true
                }()
        }

        for i := 0; i < 1000; i++ {
                <-finished
        }
        fmt.Println("finish")
}
