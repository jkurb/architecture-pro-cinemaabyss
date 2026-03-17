package main

import (
    "context"
    "encoding/json"
    "errors"
    "log"
    "net/http"
    "os"
    "os/signal"
    "strings"
    "syscall"
    "time"

    "github.com/segmentio/kafka-go"
)

type successResponse struct {
    Status string `json:"status"`
}

type healthResponse struct {
    Status bool `json:"status"`
}

type eventMessage struct {
    EventType string         `json:"event_type"`
    Payload   map[string]any `json:"payload"`
    Timestamp time.Time      `json:"timestamp"`
}

type eventService struct {
    writers map[string]*kafka.Writer
}

func newEventService(brokers []string) *eventService {
    writers := map[string]*kafka.Writer{
        "movie": newWriter(brokers, "movie-events"),
        "user": newWriter(brokers, "user-events"),
        "payment": newWriter(brokers, "payment-events"),
    }

    return &eventService{
        writers: writers,
    }
}

func main() {
    brokers := parseBrokers(os.Getenv("KAFKA_BROKERS"))
    service := newEventService(brokers)
    defer service.close()

    ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
    defer cancel()
    service.startConsumers(ctx, brokers)

    mux := http.NewServeMux()
    mux.HandleFunc("/api/events/health", healthHandler)
    mux.HandleFunc("/api/events/movie", service.eventHandler("movie"))
    mux.HandleFunc("/api/events/user", service.eventHandler("user"))
    mux.HandleFunc("/api/events/payment", service.eventHandler("payment"))

    port := os.Getenv("PORT")
    if port == "" {
        port = "8082"
    }

    server := &http.Server{
        Addr:    ":" + port,
        Handler: mux,
    }

    log.Printf("Starting events service on port %s", port)
    log.Printf("Kafka brokers: %s", strings.Join(brokers, ","))

    go func() {
        <-ctx.Done()
        shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
        defer shutdownCancel()
        if err := server.Shutdown(shutdownCtx); err != nil {
            log.Printf("HTTP server shutdown error: %v", err)
        }
    }()

    if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
        log.Fatal(err)
    }
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
    if r.Method != http.MethodGet {
        http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
        return
    }

    w.Header().Set("Content-Type", "application/json")
    _ = json.NewEncoder(w).Encode(healthResponse{Status: true})
}

func (s *eventService) eventHandler(eventType string) http.HandlerFunc {
    return func(w http.ResponseWriter, r *http.Request) {
        if r.Method != http.MethodPost {
            http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
            return
        }

        var payload map[string]any
        if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
            http.Error(w, "invalid json payload", http.StatusBadRequest)
            return
        }

        msg := eventMessage{
            EventType: eventType,
            Payload:   payload,
            Timestamp: time.Now().UTC(),
        }

        if err := s.publishWithRetry(r.Context(), eventType, msg); err != nil {
            log.Printf("failed to publish %s event: %v", eventType, err)
            http.Error(w, "failed to publish event", http.StatusServiceUnavailable)
            return
        }

        log.Printf("published %s event: %+v", eventType, payload)

        w.Header().Set("Content-Type", "application/json")
        w.WriteHeader(http.StatusCreated)
        _ = json.NewEncoder(w).Encode(successResponse{Status: "success"})
    }
}

func (s *eventService) publishWithRetry(ctx context.Context, eventType string, msg eventMessage) error {
    writer, ok := s.writers[eventType]
    if !ok {
        return errors.New("unsupported event type")
    }

    payload, err := json.Marshal(msg)
    if err != nil {
        return err
    }

    var lastErr error
    for i := 0; i < 3; i++ {
        writeErr := writer.WriteMessages(ctx, kafka.Message{
            Key:   []byte(eventType),
            Value: payload,
            Time:  time.Now().UTC(),
        })
        if writeErr == nil {
            return nil
        }
        lastErr = writeErr
        time.Sleep(200 * time.Millisecond)
    }

    return lastErr
}

func (s *eventService) startConsumers(ctx context.Context, brokers []string) {
    go runConsumer(ctx, brokers, "movie-events", "events-service-movie")
    go runConsumer(ctx, brokers, "user-events", "events-service-user")
    go runConsumer(ctx, brokers, "payment-events", "events-service-payment")
}

func (s *eventService) close() {
    for _, writer := range s.writers {
        if writer == nil {
            continue
        }
        if err := writer.Close(); err != nil {
            log.Printf("failed to close writer: %v", err)
        }
    }
}

func runConsumer(ctx context.Context, brokers []string, topic string, groupID string) {
    reader := kafka.NewReader(kafka.ReaderConfig{
        Brokers:     brokers,
        GroupID:     groupID,
        Topic:       topic,
        MinBytes:    1,
        MaxBytes:    10e6,
        StartOffset: kafka.FirstOffset,
    })
    defer func() {
        if err := reader.Close(); err != nil {
            log.Printf("failed to close consumer for topic %s: %v", topic, err)
        }
    }()

    for {
        msg, err := reader.ReadMessage(ctx)
        if err != nil {
            if errors.Is(err, context.Canceled) {
                return
            }
            log.Printf("consumer read error for topic %s: %v", topic, err)
            time.Sleep(500 * time.Millisecond)
            continue
        }

        log.Printf("processed event from topic=%s key=%s value=%s", topic, string(msg.Key), string(msg.Value))
    }
}

func newWriter(brokers []string, topic string) *kafka.Writer {
    return &kafka.Writer{
        Addr:                   kafka.TCP(brokers...),
        Topic:                  topic,
        Balancer:               &kafka.LeastBytes{},
        AllowAutoTopicCreation: true,
        RequiredAcks:           kafka.RequireOne,
        WriteTimeout:           5 * time.Second,
        ReadTimeout:            5 * time.Second,
    }
}

func parseBrokers(raw string) []string {
    if strings.TrimSpace(raw) == "" {
        return []string{"localhost:9092"}
    }

    parts := strings.Split(raw, ",")
    brokers := make([]string, 0, len(parts))
    for _, part := range parts {
        broker := strings.TrimSpace(part)
        if broker != "" {
            brokers = append(brokers, broker)
        }
    }
    if len(brokers) == 0 {
        return []string{"localhost:9092"}
    }
    return brokers
}
