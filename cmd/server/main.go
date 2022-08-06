package main

import (
	"fmt"
	"log"
	"os"

	"github.com/denisschmidt/golog/internal/server"
)

func main() {
	log.Print("Starting golog server")
	port := os.Getenv("PORT")
	if port == "" {
		port = "4001"
	}
	log.Printf("Listening on %s", port)
	server := server.NewHTTPServer(fmt.Sprintf(":%s", port))
	log.Fatal(server.ListenAndServe())
}
