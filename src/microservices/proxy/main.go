package main

import (
    "encoding/json"
    "io"
    "log"
    "math/rand"
    "net/http"
    "net/url"
    "os"
    "strconv"
    "strings"
    "time"
)

type proxyConfig struct {
    monolithURL           string
    moviesServiceURL      string
    eventsServiceURL      string
    gradualMigration      bool
    moviesMigrationPct    int
    requestTimeoutSeconds int
}

type targetRoute struct {
    baseURL     string
    serviceName string
}

func main() {
    rand.Seed(time.Now().UnixNano())

    cfg := loadConfig()
    client := &http.Client{
        Timeout: time.Duration(cfg.requestTimeoutSeconds) * time.Second,
    }

    mux := http.NewServeMux()
    mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
        w.Header().Set("Content-Type", "application/json")
        _ = json.NewEncoder(w).Encode(map[string]any{
            "status":                  true,
            "gradual_migration":       cfg.gradualMigration,
            "movies_migration_percent": cfg.moviesMigrationPct,
        })
    })

    mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
        route, err := chooseTargetRoute(cfg, r.URL.Path)
        if err != nil {
            http.Error(w, err.Error(), http.StatusInternalServerError)
            return
        }
        w.Header().Set("X-Upstream-Service", route.serviceName)
        proxyRequest(client, route.baseURL, route.serviceName, w, r)
    })

    port := os.Getenv("PORT")
    if port == "" {
        port = "8000"
    }

    log.Printf("Starting proxy service on port %s", port)
    log.Printf("Monolith URL: %s", cfg.monolithURL)
    log.Printf("Movies URL: %s", cfg.moviesServiceURL)
    log.Printf("Events URL: %s", cfg.eventsServiceURL)
    log.Printf("Gradual migration: %v, movies percent: %d", cfg.gradualMigration, cfg.moviesMigrationPct)

    log.Fatal(http.ListenAndServe(":"+port, mux))
}

func loadConfig() proxyConfig {
    monolithURL := getenvOrDefault("MONOLITH_URL", "http://localhost:8080")
    moviesServiceURL := getenvOrDefault("MOVIES_SERVICE_URL", "http://localhost:8081")
    eventsServiceURL := getenvOrDefault("EVENTS_SERVICE_URL", "http://localhost:8082")
    gradualMigration := strings.EqualFold(getenvOrDefault("GRADUAL_MIGRATION", "false"), "true")
    migrationPercent := parsePercent(getenvOrDefault("MOVIES_MIGRATION_PERCENT", "0"))

    timeoutSeconds := 10
    if raw := os.Getenv("REQUEST_TIMEOUT_SECONDS"); raw != "" {
        if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
            timeoutSeconds = parsed
        }
    }

    return proxyConfig{
        monolithURL:           strings.TrimRight(monolithURL, "/"),
        moviesServiceURL:      strings.TrimRight(moviesServiceURL, "/"),
        eventsServiceURL:      strings.TrimRight(eventsServiceURL, "/"),
        gradualMigration:      gradualMigration,
        moviesMigrationPct:    migrationPercent,
        requestTimeoutSeconds: timeoutSeconds,
    }
}

func chooseTargetRoute(cfg proxyConfig, path string) (targetRoute, error) {
    switch {
    case strings.HasPrefix(path, "/api/movies"):
        if shouldRouteMoviesToMicroservice(cfg) {
            return targetRoute{
                baseURL:     cfg.moviesServiceURL,
                serviceName: "movies-service",
            }, nil
        }
        return targetRoute{
            baseURL:     cfg.monolithURL,
            serviceName: "monolith",
        }, nil
    case strings.HasPrefix(path, "/api/events"):
        return targetRoute{
            baseURL:     cfg.eventsServiceURL,
            serviceName: "events-service",
        }, nil
    default:
        return targetRoute{
            baseURL:     cfg.monolithURL,
            serviceName: "monolith",
        }, nil
    }
}

func shouldRouteMoviesToMicroservice(cfg proxyConfig) bool {
    if !cfg.gradualMigration {
        return true
    }
    if cfg.moviesMigrationPct <= 0 {
        return false
    }
    if cfg.moviesMigrationPct >= 100 {
        return true
    }
    return rand.Intn(100) < cfg.moviesMigrationPct
}

func proxyRequest(client *http.Client, baseURL string, serviceName string, w http.ResponseWriter, r *http.Request) {
    targetURL, err := buildTargetURL(baseURL, r.URL)
    if err != nil {
        http.Error(w, "failed to build target URL", http.StatusInternalServerError)
        return
    }

    var body io.Reader
    if r.Body != nil {
        defer r.Body.Close()
        payload, readErr := io.ReadAll(r.Body)
        if readErr != nil {
            http.Error(w, "failed to read request body", http.StatusBadRequest)
            return
        }
        body = strings.NewReader(string(payload))
    }

    req, err := http.NewRequestWithContext(r.Context(), r.Method, targetURL, body)
    if err != nil {
        http.Error(w, "failed to create upstream request", http.StatusInternalServerError)
        return
    }

    copyHeaders(r.Header, req.Header)
    req.Header.Set("X-Forwarded-Host", r.Host)
    req.Header.Set("X-Forwarded-Proto", forwardedProto(r))
    req.Header.Set("X-Forwarded-For", forwardedFor(r))

    resp, err := client.Do(req)
    if err != nil {
        http.Error(w, "upstream request failed", http.StatusBadGateway)
        return
    }
    defer resp.Body.Close()

    copyHeaders(resp.Header, w.Header())
    w.Header().Set("X-Upstream-Service", serviceName)
    w.WriteHeader(resp.StatusCode)
    _, _ = io.Copy(w, resp.Body)
}

func buildTargetURL(base string, requestURL *url.URL) (string, error) {
    parsed, err := url.Parse(base)
    if err != nil {
        return "", err
    }

    parsed.Path = singleJoiningSlash(parsed.Path, requestURL.Path)
    parsed.RawQuery = requestURL.RawQuery
    return parsed.String(), nil
}

func parsePercent(raw string) int {
    val, err := strconv.Atoi(raw)
    if err != nil {
        return 0
    }
    if val < 0 {
        return 0
    }
    if val > 100 {
        return 100
    }
    return val
}

func getenvOrDefault(key, fallback string) string {
    if val := os.Getenv(key); val != "" {
        return val
    }
    return fallback
}

func copyHeaders(src, dst http.Header) {
    for k, values := range src {
        if isHopByHopHeader(k) {
            continue
        }
        for _, v := range values {
            dst.Add(k, v)
        }
    }
}

func isHopByHopHeader(header string) bool {
    switch strings.ToLower(header) {
    case "connection", "proxy-connection", "keep-alive", "te", "trailer", "transfer-encoding", "upgrade":
        return true
    default:
        return false
    }
}

func singleJoiningSlash(a, b string) string {
    aSlash := strings.HasSuffix(a, "/")
    bSlash := strings.HasPrefix(b, "/")
    switch {
    case aSlash && bSlash:
        return a + b[1:]
    case !aSlash && !bSlash:
        return a + "/" + b
    default:
        return a + b
    }
}

func forwardedProto(r *http.Request) string {
    if r.TLS != nil {
        return "https"
    }
    return "http"
}

func forwardedFor(r *http.Request) string {
    xff := r.Header.Get("X-Forwarded-For")
    if xff == "" {
        return r.RemoteAddr
    }
    return xff + ", " + r.RemoteAddr
}
