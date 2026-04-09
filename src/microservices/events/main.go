package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

var (
	kafkaBrokers []string
	kafkaWriter  *kafka.Writer
)

type Event struct {
	ID        string                 `json:"id"`
	Type      string                 `json:"type"`
	Timestamp string                 `json:"timestamp"`
	Payload   map[string]interface{} `json:"payload"`
}

type MovieEvent struct {
	MovieID     int      `json:"movie_id"`
	Title       string   `json:"title"`
	Action      string   `json:"action"`
	UserID      *int     `json:"user_id,omitempty"`
	Rating      *float64 `json:"rating,omitempty"`
	Genres      []string `json:"genres,omitempty"`
	Description *string  `json:"description,omitempty"`
}

type UserEvent struct {
	UserID    int     `json:"user_id"`
	Username  *string `json:"username,omitempty"`
	Email     *string `json:"email,omitempty"`
	Action    string  `json:"action"`
	Timestamp string  `json:"timestamp"`
}

type PaymentEvent struct {
	PaymentID  int     `json:"payment_id"`
	UserID     int     `json:"user_id"`
	Amount     float64 `json:"amount"`
	Status     string  `json:"status"`
	Timestamp  string  `json:"timestamp"`
	MethodType *string `json:"method_type,omitempty"`
}

type EventResponse struct {
	Status    string `json:"status"`
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
	Event     Event  `json:"event"`
}

func main() {
	brokersEnv := getEnv("KAFKA_BROKERS", "localhost:9092")
	kafkaBrokers = strings.Split(brokersEnv, ",")

	log.Printf("Starting Events Service on port %s", getEnv("PORT", "8082"))
	log.Printf("Kafka Brokers: %v", kafkaBrokers)

	go startConsumer("movie-events")
	go startConsumer("user-events")
	go startConsumer("payment-events")

	http.HandleFunc("/api/events/health", healthHandler)
	http.HandleFunc("/api/events/movie", movieEventHandler)
	http.HandleFunc("/api/events/user", userEventHandler)
	http.HandleFunc("/api/events/payment", paymentEventHandler)

	port := getEnv("PORT", "8082")
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"status": true})
}

func movieEventHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var movieEvent MovieEvent
	if err := json.NewDecoder(r.Body).Decode(&movieEvent); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	event := Event{
		ID:        fmt.Sprintf("movie-%d-%s", movieEvent.MovieID, movieEvent.Action),
		Type:      "movie",
		Timestamp: time.Now().Format(time.RFC3339),
		Payload:   make(map[string]interface{}),
	}

	eventBytes, _ := json.Marshal(movieEvent)
	json.Unmarshal(eventBytes, &event.Payload)

	partition, offset, err := publishEvent("movie-events", event)
	if err != nil {
		log.Printf("Error publishing movie event: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	response := EventResponse{
		Status:    "success",
		Partition: partition,
		Offset:    offset,
		Event:     event,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(response)
}

func userEventHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var userEvent UserEvent
	if err := json.NewDecoder(r.Body).Decode(&userEvent); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	event := Event{
		ID:        fmt.Sprintf("user-%d-%s", userEvent.UserID, userEvent.Action),
		Type:      "user",
		Timestamp: userEvent.Timestamp,
		Payload:   make(map[string]interface{}),
	}

	eventBytes, _ := json.Marshal(userEvent)
	json.Unmarshal(eventBytes, &event.Payload)

	partition, offset, err := publishEvent("user-events", event)
	if err != nil {
		log.Printf("Error publishing user event: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	response := EventResponse{
		Status:    "success",
		Partition: partition,
		Offset:    offset,
		Event:     event,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(response)
}

func paymentEventHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var paymentEvent PaymentEvent
	if err := json.NewDecoder(r.Body).Decode(&paymentEvent); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	event := Event{
		ID:        fmt.Sprintf("payment-%d-%s", paymentEvent.PaymentID, paymentEvent.Status),
		Type:      "payment",
		Timestamp: paymentEvent.Timestamp,
		Payload:   make(map[string]interface{}),
	}

	eventBytes, _ := json.Marshal(paymentEvent)
	json.Unmarshal(eventBytes, &event.Payload)

	partition, offset, err := publishEvent("payment-events", event)
	if err != nil {
		log.Printf("Error publishing payment event: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	response := EventResponse{
		Status:    "success",
		Partition: partition,
		Offset:    offset,
		Event:     event,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(response)
}

func publishEvent(topic string, event Event) (int, int64, error) {
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  kafkaBrokers,
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
		Async:    false,
	})
	defer writer.Close()

	eventBytes, err := json.Marshal(event)
	if err != nil {
		return 0, 0, err
	}

	msg := kafka.Message{
		Key:   []byte(event.ID),
		Value: eventBytes,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = writer.WriteMessages(ctx, msg)
	if err != nil {
		return 0, 0, err
	}

	log.Printf("Published event to topic %s: %s", topic, event.ID)
	
	return 0, 0, nil
}

func startConsumer(topic string) {
	time.Sleep(5 * time.Second)

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        kafkaBrokers,
		Topic:          topic,
		GroupID:        fmt.Sprintf("%s-consumer-group", topic),
		MinBytes:       10e3,
		MaxBytes:       10e6,
		CommitInterval: time.Second,
		StartOffset:    kafka.LastOffset,
	})
	defer reader.Close()

	log.Printf("Started consumer for topic: %s", topic)

	for {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		msg, err := reader.ReadMessage(ctx)
		cancel()

		if err != nil {
			if err == context.DeadlineExceeded {
				continue
			}
			log.Printf("Error reading from topic %s: %v", topic, err)
			time.Sleep(time.Second)
			continue
		}

		var event Event
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Printf("Error unmarshaling event from topic %s: %v", topic, err)
			continue
		}

		log.Printf("Consumed event from topic %s: ID=%s, Type=%s, Timestamp=%s",
			topic, event.ID, event.Type, event.Timestamp)
	}
}

func getEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}
