package httpapi

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"math"
	"net"
	"net/http"
	"runtime/debug"
	"strconv"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/trace"

	"github.com/bekonat/payledger/internal/observability"
)

type ctxKey int

const requestIDKey ctxKey = iota

type Limiter interface {
	Allow(ctx context.Context, key string) (bool, time.Duration, error)
}

// chain applies middlewares so that the first one is the outermost.
func chain(h http.Handler, mws ...func(http.Handler) http.Handler) http.Handler {
	for i := len(mws) - 1; i >= 0; i-- {
		h = mws[i](h)
	}
	return h
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func tracing(next http.Handler) http.Handler {
	return otelhttp.NewHandler(next, "http.server")
}

func requestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Request-ID")
		if id == "" || len(id) > 64 {
			b := make([]byte, 8)
			_, _ = rand.Read(b)
			id = hex.EncodeToString(b)
		}
		w.Header().Set("X-Request-ID", id)
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), requestIDKey, id)))
	})
}

func logging(log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(rec, r)
			id, _ := r.Context().Value(requestIDKey).(string)
			log.InfoContext(r.Context(), "request",
				slog.String("method", r.Method),
				slog.String("path", r.URL.Path),
				slog.Int("status", rec.status),
				slog.Duration("duration", time.Since(start)),
				slog.String("request_id", id),
			)
		})
	}
}

func recoverer(log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if v := recover(); v != nil {
					log.ErrorContext(r.Context(), "panic recovered",
						slog.Any("panic", v),
						slog.String("stack", string(debug.Stack())),
					)
					writeError(w, http.StatusInternalServerError, "internal server error")
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}

// metrics labels requests with the ServeMux pattern, not the raw path, so
// /api/v1/accounts/{id} is one time series instead of one per account.
func metrics(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		route := r.Pattern
		if route == "" {
			route = "unmatched"
		}
		trace.SpanFromContext(r.Context()).SetName(route)
		observability.HTTPRequests.WithLabelValues(route, r.Method, strconv.Itoa(rec.status)).Inc()
		observability.HTTPDuration.WithLabelValues(route).Observe(time.Since(start).Seconds())
	})
}

// rateLimit is fail-open: if Redis is down, requests go through and the
// error is counted. Losing rate limiting for a while is better than turning
// a Redis outage into a full payments outage.
func rateLimit(l Limiter, log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		if l == nil {
			return next
		}
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			allowed, retryAfter, err := l.Allow(r.Context(), clientKey(r))
			switch {
			case err != nil:
				observability.RateLimitDecisions.WithLabelValues("error").Inc()
				log.WarnContext(r.Context(), "rate limiter unavailable", slog.String("error", err.Error()))
			case !allowed:
				observability.RateLimitDecisions.WithLabelValues("limited").Inc()
				w.Header().Set("Retry-After", strconv.Itoa(int(math.Ceil(retryAfter.Seconds()))))
				writeError(w, http.StatusTooManyRequests, "rate limit exceeded")
				return
			default:
				observability.RateLimitDecisions.WithLabelValues("allowed").Inc()
			}
			next.ServeHTTP(w, r)
		})
	}
}

// clientKey identifies the caller. Behind an API gateway this would be the
// authenticated client id; X-Client-ID stands in for it here.
func clientKey(r *http.Request) string {
	if id := r.Header.Get("X-Client-ID"); id != "" && len(id) <= 64 {
		return "client:" + id
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	return "ip:" + host
}
