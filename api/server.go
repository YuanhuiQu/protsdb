package api

import (
	"context"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
)

// Server represents the API server
type Server struct {
	mux    *http.ServeMux
	server *http.Server
}

// New creates a new API server
func New() *Server {
	mux := http.NewServeMux()

	server := &Server{
		mux: mux,
		server: &http.Server{
			Addr:         ":9090",
			Handler:      mux,
			ReadTimeout:  30 * time.Second,
			WriteTimeout: 30 * time.Second,
		},
	}

	// Set up routes
	server.routes()

	return server
}

// routes sets up all the API routes
func (s *Server) routes() {
	s.mux.HandleFunc("/api/v1/write", s.handleRemoteWrite)
	s.mux.HandleFunc("/api/v1/health", s.handleHealth)
}

// Start starts the HTTP server
func (s *Server) Start() error {
	log.Printf("Server listening on %s", s.server.Addr)
	return s.server.ListenAndServe()
}

// Shutdown gracefully shuts down the server
func (s *Server) Shutdown(ctx context.Context) error {
	return s.server.Shutdown(ctx)
}

// handleRemoteWrite handles Prometheus remote write requests
func (s *Server) handleRemoteWrite(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	compressed, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Error reading request body", http.StatusInternalServerError)
		return
	}
	defer r.Body.Close()

	// Prometheus remote write uses snappy compression
	reqBuf, err := snappy.Decode(nil, compressed)
	if err != nil {
		http.Error(w, "Error decompressing request body", http.StatusBadRequest)
		return
	}

	// Parse the protobuf message
	var writeRequest prompb.WriteRequest
	if err := proto.Unmarshal(reqBuf, &writeRequest); err != nil {
		http.Error(w, "Error unmarshaling request", http.StatusBadRequest)
		return
	}

	// TODO: Store the time series data directly using prompb types
	// For now, just log the number of time series received
	log.Printf("Received %d time series", len(writeRequest.Timeseries))
	w.WriteHeader(http.StatusOK)
}

// handleHealth handles health check requests
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}
