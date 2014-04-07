package main

import (
  "net/http" //package for http based web programs
  "fmt"
)

func handler(w http.ResponseWriter, r *http.Request) { 
    fmt.Println("Inside handler")
    // fmt.Fprintf(w, "Hello world from my Go program!")
    http.Redirect(w, r, "http://localhost:8080/", http.StatusFound)
}

func main() {
    http.HandleFunc("/q2", handler) // redirect all urls to the handler function
    // http.Handle("/q2", server)
    if err := http.ListenAndServe(":80", nil); err != nil {
        panic(err)
    }
}