package main

import (
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"time"
)

func main() {
	http.Handle("/", http.FileServer(http.Dir("static")))
	http.HandleFunc("/beat", beatHandler)
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func beatHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Content-Type", "text/event-stream")

	// Send a heartbeat every 2 seconds
	for {
		rand.Seed(time.Now().UnixNano())
		fmt.Fprintf(w, "id: heartbeat \ndata: %d \n\n", rand.Intn(100))
		w.(http.Flusher).Flush()
		time.Sleep(2 * time.Second)
	}
}
