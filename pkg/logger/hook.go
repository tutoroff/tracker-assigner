package logger

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// ErrorReporter defines the interface for creating bug tickets from logs.
type ErrorReporter interface {
	ReportError(ctx context.Context, endpoint string, err error, requestBody []byte) (string, error)
}

// WithTrackerHook adds a Zap hook that catches Error and Fatal logs
// and reports them to Yandex Tracker using the provided ErrorReporter.
func WithTrackerHook(l *zap.Logger, reporter ErrorReporter) *zap.Logger {
	hook := func(entry zapcore.Entry) error {
		// Only catch Error and Fatal levels
		if entry.Level == zapcore.ErrorLevel || entry.Level == zapcore.FatalLevel {
			err := fmt.Errorf("%s", entry.Message)
			endpoint := entry.Caller.TrimmedPath()
			if endpoint == "" {
				endpoint = "logger"
			}

			stack := []byte(entry.Stack)
			if len(stack) == 0 {
				stack = []byte("(no stack trace provided by logger)")
			}

			reportFunc := func() {
				// Use a timeout context to ensure we don't block indefinitely on fatal crash or leak goroutines
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				// The reporter should handle deduplication and actual network request
				_, _ = reporter.ReportError(ctx, endpoint, err, stack)
			}

			if entry.Level == zapcore.FatalLevel {
				// Block synchronously before zap calls os.Exit(2)
				reportFunc()
			} else {
				// Run in a goroutine so it does not block the application's execution on regular errors
				go reportFunc()
			}
		}
		return nil
	}
	return l.WithOptions(zap.Hooks(hook))
}
