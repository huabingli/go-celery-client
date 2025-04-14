package go_celery_client

import (
	"context"
	"log/slog"
	"os"
)

type SlogLogger struct {
	logger *slog.Logger
	ctx    context.Context
}

func NewSlogLogger() *SlogLogger {
	ctx := context.Background()
	return &SlogLogger{
		logger: slog.Default(),
		ctx:    ctx,
	}
}

func (l *SlogLogger) Fatalf(format string, args ...interface{}) {
	l.logger.Log(l.ctx, slog.LevelError, format, args...) // slog 没有 Fatal 级别，需手动退出
	os.Exit(1)
}

func (l *SlogLogger) Errorf(format string, args ...interface{}) {
	l.logger.Log(l.ctx, slog.LevelError, format, args...)
}

func (l *SlogLogger) Warnf(format string, args ...interface{}) {
	l.logger.Log(l.ctx, slog.LevelWarn, format, args...)
}

func (l *SlogLogger) Infof(format string, args ...interface{}) {
	l.logger.Log(l.ctx, slog.LevelInfo, format, args...)
}

func (l *SlogLogger) Debugf(format string, args ...interface{}) {
	l.logger.Log(l.ctx, slog.LevelDebug, format, args...)
}
