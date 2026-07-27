package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-playground/validator/v10"

	"github.com/asvikov/smlogistics/internal/domain"
	"github.com/asvikov/smlogistics/internal/service"
	"github.com/asvikov/smlogistics/internal/store"
)

var validate = validator.New()

// NewRouter constructs the chi router with all middleware and routes.
func NewRouter(
	dispatchSvc *service.DispatchService,
	idempotencySvc *service.IdempotencyService,
	pgStore *store.PGStore,
	logger *slog.Logger,
) chi.Router {
	r := chi.NewRouter()

	// Middleware
	r.Use(middleware.RequestID)
	r.Use(slogMiddleware(logger))
	r.Use(middleware.Recoverer)
	r.Use(middleware.Timeout(30 * time.Second))

	// Health check
	r.Get("/health", healthHandler(pgStore, logger))

	// API v1
	r.Route("/api/v1", func(r chi.Router) {
		r.Post("/notifications/bulk", dispatchHandler(dispatchSvc, idempotencySvc, logger))
		r.Get("/subscribers/{subscriber_id}/notifications", statusHandler(pgStore, logger))
	})

	return r
}

// for POST /api/v1/notifications/bulk.
type BulkNotificationRequest struct {
	Channel        string   `json:"channel"         validate:"required,oneof=sms email"`
	Message        string   `json:"message"         validate:"required,max=2000"`
	Recipients     []string `json:"recipients"      validate:"required,min=1,max=1000,dive,max=64"`
	Priority       string   `json:"priority"        validate:"required,oneof=transactional marketing default"`
	IdempotencyKey string   `json:"idempotency_key" validate:"required,uuid"`
}

// ***** Handlers

func dispatchHandler(dispatchSvc *service.DispatchService, idempotencySvc *service.IdempotencyService, logger *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req BulkNotificationRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusUnprocessableEntity, map[string]string{
				"error": "Invalid JSON body",
			})
			return
		}

		if err := validate.Struct(req); err != nil {
			writeJSON(w, http.StatusUnprocessableEntity, map[string]any{
				"error":  "Validation failed",
				"fields": formatValidationErrors(err),
			})
			return
		}

		ok, err := idempotencySvc.CheckOrCreate(r.Context(), req.IdempotencyKey)
		if err != nil {
			logger.Error("dispatch: idempotency check failed", "error", err)
			writeJSON(w, http.StatusInternalServerError, map[string]string{
				"error": "Internal server error",
			})
			return
		}
		if !ok {
			writeJSON(w, http.StatusConflict, map[string]string{
				"status":          "duplicate",
				"message":         "This request has already been processed.",
				"idempotency_key": req.IdempotencyKey,
			})
			return
		}

		result, err := dispatchSvc.DispatchBulk(r.Context(), service.DispatchRequest{
			Channel:        req.Channel,
			Message:        req.Message,
			Recipients:     req.Recipients,
			Priority:       req.Priority,
			IdempotencyKey: req.IdempotencyKey,
		})
		if err != nil {
			// Release idempotency lock so client can retry
			if relErr := idempotencySvc.Release(r.Context(), req.IdempotencyKey); relErr != nil {
				logger.Error("dispatch: failed to release idempotency lock", "error", relErr)
			}
			logger.Error("dispatch: failed", "error", err)
			writeJSON(w, http.StatusInternalServerError, map[string]string{
				"error": "Failed to dispatch notifications",
			})
			return
		}

		writeJSON(w, http.StatusAccepted, map[string]any{
			"status":         "accepted",
			"batch_id":       result.BatchID,
			"accepted_count": result.AcceptedCount,
		})
	}
}

func statusHandler(pgStore *store.PGStore, logger *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		subscriberID := chi.URLParam(r, "subscriber_id")
		if subscriberID == "" {
			writeJSON(w, http.StatusUnprocessableEntity, map[string]string{
				"error": "subscriber_id is required",
			})
			return
		}

		// Optional query filters
		var channel *domain.Channel
		if c := r.URL.Query().Get("channel"); c != "" {
			if !domain.ValidChannel(c) {
				writeJSON(w, http.StatusUnprocessableEntity, map[string]string{
					"error": "Invalid channel filter",
				})
				return
			}
			ch := domain.Channel(c)
			channel = &ch
		}

		var status *domain.Status
		if s := r.URL.Query().Get("status"); s != "" {
			if !domain.ValidStatus(s) {
				writeJSON(w, http.StatusUnprocessableEntity, map[string]string{
					"error": "Invalid status filter",
				})
				return
			}
			st := domain.Status(s)
			status = &st
		}

		perPage := 20
		if p := r.URL.Query().Get("per_page"); p != "" {
			n, err := strconv.Atoi(p)
			if err != nil || n < 1 || n > 100 {
				writeJSON(w, http.StatusUnprocessableEntity, map[string]string{
					"error": "per_page must be between 1 and 100",
				})
				return
			}
			perPage = n
		}

		page := 1
		if p := r.URL.Query().Get("page"); p != "" {
			n, err := strconv.Atoi(p)
			if err == nil && n > 0 {
				page = n
			}
		}

		offset := (page - 1) * perPage

		notifications, total, err := pgStore.GetBySubscriber(r.Context(), subscriberID, channel, status, perPage, offset)
		if err != nil {
			logger.Error("status: query failed", "error", err)
			writeJSON(w, http.StatusInternalServerError, map[string]string{
				"error": "Internal server error",
			})
			return
		}

		lastPage := (total + perPage - 1) / perPage
		if lastPage < 1 {
			lastPage = 1
		}

		writeJSON(w, http.StatusOK, map[string]any{
			"data": notifications,
			"meta": map[string]any{
				"current_page": page,
				"last_page":    lastPage,
				"per_page":     perPage,
				"total":        total,
			},
		})
	}
}

func healthHandler(pgStore *store.PGStore, logger *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()

		status := "ok"
		details := map[string]string{}

		if err := pgStore.Pool().Ping(ctx); err != nil {
			status = "degraded"
			details["db"] = "unreachable"
		} else {
			details["db"] = "ok"
		}

		resp := map[string]any{
			"status":  status,
			"details": details,
		}

		code := http.StatusOK
		if status != "ok" {
			code = http.StatusServiceUnavailable
		}
		writeJSON(w, code, resp)
	}
}

// ***** Helpers

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func formatValidationErrors(err error) map[string]string {
	errs := map[string]string{}
	if ve, ok := err.(validator.ValidationErrors); ok {
		for _, fe := range ve {
			errs[fe.Field()] = fe.Tag()
		}
	}
	return errs
}

// slogMiddleware logs each request
func slogMiddleware(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
			next.ServeHTTP(ww, r)
			logger.Info("http",
				"method", r.Method,
				"path", r.URL.Path,
				"status", ww.Status(),
				"duration_ms", time.Since(start).Milliseconds(),
				"request_id", middleware.GetReqID(r.Context()),
			)
		})
	}
}
