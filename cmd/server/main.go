package main

import (
	"log"
	"net/http"
	"os"

	"proxy-monitor/internal/handler"
	"proxy-monitor/internal/monitor"
	"proxy-monitor/internal/store"
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	// Initialise shared state.
	s := store.New()

	// Start the background polling engine.
	mon := monitor.New(s)
	go mon.Run()

	// Wire up HTTP routes.
	mux := handler.NewRouter(s, mon)

	log.Printf("🚀 proxy-monitor listening on :%s", port)
	if err := http.ListenAndServe(":"+port, mux); err != nil {
		log.Fatalf("server failed: %v", err)
	}
}
