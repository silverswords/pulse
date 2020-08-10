// use dapr logger https://github.com/dapr/dapr
package logger

import (
	"os"
	"time"

	"github.com/silverswords/whisper/pkg/version"
	"github.com/sirupsen/logrus"
)

// whisperLogger is the implementation for logrus
type whisperLogger struct {
	// name is the name of logger that is published to log as a scope
	name string
	// loger is the instance of logrus logger
	logger *logrus.Entry
}

func newDaprLogger(name string) *whisperLogger {
	newLogger := logrus.New()
	newLogger.SetOutput(os.Stdout)

	dl := &whisperLogger{
		name: name,
		logger: newLogger.WithFields(logrus.Fields{
			logFieldScope: name,
			logFieldType:  LogTypeLog,
		}),
	}

	dl.EnableJSONOutput(defaultJSONOutput)

	return dl
}

// EnableJSONOutput enables JSON formatted output log
func (l *whisperLogger) EnableJSONOutput(enabled bool) {
	var formatter logrus.Formatter

	fieldMap := logrus.FieldMap{
		// If time field name is conflicted, logrus adds "fields." prefix.
		// So rename to unused field @time to avoid the confliction.
		logrus.FieldKeyTime:  logFieldTimeStamp,
		logrus.FieldKeyLevel: logFieldLevel,
		logrus.FieldKeyMsg:   logFieldMessage,
	}

	hostname, _ := os.Hostname()
	l.logger.Data = logrus.Fields{
		logFieldScope:    l.logger.Data[logFieldScope],
		logFieldType:     LogTypeLog,
		logFieldInstance: hostname,
		logFieldDaprVer:  version.Version(),
	}

	if enabled {
		formatter = &logrus.JSONFormatter{
			TimestampFormat: time.RFC3339Nano,
			FieldMap:        fieldMap,
		}
	} else {
		formatter = &logrus.TextFormatter{
			TimestampFormat: time.RFC3339Nano,
			FieldMap:        fieldMap,
		}
	}

	l.logger.Logger.SetFormatter(formatter)
}

// SetAppID sets app_id field in log. Default value is empty string
func (l *whisperLogger) SetAppID(id string) {
	l.logger = l.logger.WithField(logFieldAppID, id)
}

func toLogrusLevel(lvl LogLevel) logrus.Level {
	// ignore error because it will never happens
	l, _ := logrus.ParseLevel(string(lvl))
	return l
}

// SetOutputLevel sets log output level
func (l *whisperLogger) SetOutputLevel(outputLevel LogLevel) {
	l.logger.Logger.SetLevel(toLogrusLevel(outputLevel))
}

// WithLogType specify the log_type field in log. Default value is LogTypeLog
func (l *whisperLogger) WithLogType(logType string) Logger {
	return &whisperLogger{
		name:   l.name,
		logger: l.logger.WithField(logFieldType, logType),
	}
}

// Info logs a message at level Info.
func (l *whisperLogger) Info(args ...interface{}) {
	l.logger.Log(logrus.InfoLevel, args...)
}

// Infof logs a message at level Info.
func (l *whisperLogger) Infof(format string, args ...interface{}) {
	l.logger.Logf(logrus.InfoLevel, format, args...)
}

// Debug logs a message at level Debug.
func (l *whisperLogger) Debug(args ...interface{}) {
	l.logger.Log(logrus.DebugLevel, args...)
}

// Debugf logs a message at level Debug.
func (l *whisperLogger) Debugf(format string, args ...interface{}) {
	l.logger.Logf(logrus.DebugLevel, format, args...)
}

// Warn logs a message at level Warn.
func (l *whisperLogger) Warn(args ...interface{}) {
	l.logger.Log(logrus.WarnLevel, args...)
}

// Warnf logs a message at level Warn.
func (l *whisperLogger) Warnf(format string, args ...interface{}) {
	l.logger.Logf(logrus.WarnLevel, format, args...)
}

// Error logs a message at level Error.
func (l *whisperLogger) Error(args ...interface{}) {
	l.logger.Log(logrus.ErrorLevel, args...)
}

// Errorf logs a message at level Error.
func (l *whisperLogger) Errorf(format string, args ...interface{}) {
	l.logger.Logf(logrus.ErrorLevel, format, args...)
}

// Fatal logs a message at level Fatal then the process will exit with status set to 1.
func (l *whisperLogger) Fatal(args ...interface{}) {
	l.logger.Fatal(args...)
}

// Fatalf logs a message at level Fatal then the process will exit with status set to 1.
func (l *whisperLogger) Fatalf(format string, args ...interface{}) {
	l.logger.Fatalf(format, args...)
}
