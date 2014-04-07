package main

import (
  "net/http" //package for http based web programs
  "fmt"
  "log"
)

func handler(w http.ResponseWriter, r *http.Request) { 
  // fmt.Println("Inside handler")
  user_id := r.FormValue("userid")
  // tweet_time := r.FormValue("tweet_time")
  url := fmt.Sprintf("http://ec2-54-84-168-231.compute-1.amazonaws.com:8080/q3phase2/%s/retweeter_id", user_id)
  http.Redirect(w, r, url, http.StatusFound)
}

func main() {
  http.HandleFunc("/q3", handler) // redirect all urls to the handler function
  if err := http.ListenAndServe(":1234", nil); err != nil {
    log.Fatal(err)
  }
}