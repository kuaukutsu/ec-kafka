package app

import (
	"log/slog"
	"os"

	"kafka/internal/config"
)

// NewLogger constructs a structured logger configured for the given environment.
// Local env uses human-readable text at Debug level; production uses JSON at Warn
// level; everything else (staging, CI) uses JSON at Info level.
func NewLogger(env config.Env) *slog.Logger {
	switch env {
	case config.EnvLocal:
		return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	case config.EnvProd:
		return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn}))
	default:
		return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	}
}
