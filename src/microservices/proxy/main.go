package main

import (
	"log"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"time"
)

var (
	monolithURL       *url.URL
	moviesServiceURL  *url.URL
	eventsServiceURL  *url.URL
	gradualMigration  bool
	migrationPercent  int
)

func main() {
	rand.Seed(time.Now().UnixNano())

	var err error
	monolithURL, err = url.Parse(getEnv("MONOLITH_URL", "http://localhost:8080"))
	if err != nil {
		log.Fatal("Invalid MONOLITH_URL:", err)
	}

	moviesServiceURL, err = url.Parse(getEnv("MOVIES_SERVICE_URL", "http://localhost:8081"))
	if err != nil {
		log.Fatal("Invalid MOVIES_SERVICE_URL:", err)
	}

	eventsServiceURL, err = url.Parse(getEnv("EVENTS_SERVICE_URL", "http://localhost:8082"))
	if err != nil {
		log.Fatal("Invalid EVENTS_SERVICE_URL:", err)
	}

	gradualMigration = getEnv("GRADUAL_MIGRATION", "true") == "true"
	migrationPercent, err = strconv.Atoi(getEnv("MOVIES_MIGRATION_PERCENT", "50"))
	if err != nil {
		log.Fatal("Invalid MOVIES_MIGRATION_PERCENT:", err)
	}

	log.Printf("Starting Strangler Fig Proxy on port %s", getEnv("PORT", "8000"))
	log.Printf("Monolith URL: %s", monolithURL.String())
	log.Printf("Movies Service URL: %s", moviesServiceURL.String())
	log.Printf("Events Service URL: %s", eventsServiceURL.String())
	log.Printf("Gradual Migration: %v", gradualMigration)
	log.Printf("Movies Migration Percent: %d%%", migrationPercent)

	http.HandleFunc("/health", healthHandler)
	http.HandleFunc("/api/movies", moviesHandler)
	http.HandleFunc("/api/movies/health", moviesHealthHandler)
	http.HandleFunc("/api/users", proxyToMonolith)
	http.HandleFunc("/api/payments", proxyToMonolith)
	http.HandleFunc("/api/subscriptions", proxyToMonolith)
	http.HandleFunc("/api/events/", eventsHandler)

	port := getEnv("PORT", "8000")
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Strangler Fig Proxy is healthy"))
}

func moviesHandler(w http.ResponseWriter, r *http.Request) {
	var targetURL *url.URL

	if gradualMigration {
		if rand.Intn(100) < migrationPercent {
			targetURL = moviesServiceURL
			log.Printf("Routing /api/movies to Movies Microservice (migration: %d%%)", migrationPercent)
		} else {
			targetURL = monolithURL
			log.Printf("Routing /api/movies to Monolith (migration: %d%%)", migrationPercent)
		}
	} else {
		targetURL = moviesServiceURL
		log.Println("Routing /api/movies to Movies Microservice (100%)")
	}

	proxy := httputil.NewSingleHostReverseProxy(targetURL)
	proxy.Director = func(req *http.Request) {
		req.URL.Scheme = targetURL.Scheme
		req.URL.Host = targetURL.Host
		req.URL.Path = r.URL.Path
		req.URL.RawQuery = r.URL.RawQuery
		req.Host = targetURL.Host
	}

	proxy.ServeHTTP(w, r)
}

func moviesHealthHandler(w http.ResponseWriter, r *http.Request) {
	proxy := httputil.NewSingleHostReverseProxy(moviesServiceURL)
	proxy.Director = func(req *http.Request) {
		req.URL.Scheme = moviesServiceURL.Scheme
		req.URL.Host = moviesServiceURL.Host
		req.URL.Path = r.URL.Path
		req.URL.RawQuery = r.URL.RawQuery
		req.Host = moviesServiceURL.Host
	}

	proxy.ServeHTTP(w, r)
}

func eventsHandler(w http.ResponseWriter, r *http.Request) {
	proxy := httputil.NewSingleHostReverseProxy(eventsServiceURL)
	proxy.Director = func(req *http.Request) {
		req.URL.Scheme = eventsServiceURL.Scheme
		req.URL.Host = eventsServiceURL.Host
		req.URL.Path = r.URL.Path
		req.URL.RawQuery = r.URL.RawQuery
		req.Host = eventsServiceURL.Host
	}

	proxy.ServeHTTP(w, r)
}

func proxyToMonolith(w http.ResponseWriter, r *http.Request) {
	proxy := httputil.NewSingleHostReverseProxy(monolithURL)
	proxy.Director = func(req *http.Request) {
		req.URL.Scheme = monolithURL.Scheme
		req.URL.Host = monolithURL.Host
		req.URL.Path = r.URL.Path
		req.URL.RawQuery = r.URL.RawQuery
		req.Host = monolithURL.Host
	}

	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		log.Printf("Proxy error: %v", err)
		http.Error(w, "Service unavailable", http.StatusServiceUnavailable)
	}

	proxy.ServeHTTP(w, r)
}

func getEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}
