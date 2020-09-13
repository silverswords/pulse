package logger

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"testing"
	"time"

	"github.com/silverswords/pulse/pkg/version"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

const fakeLoggerName = "fakeLogger"

func getTestLogger(buf io.Writer) *pulseLogger {
	l := newPulseLogger(fakeLoggerName)
	l.logger.Logger.SetOutput(buf)

	return l
}

func TestEnableJSON(t *testing.T) {
	var buf bytes.Buffer
	testLogger := getTestLogger(&buf)

	expectedHost, _ := os.Hostname()
	testLogger.EnableJSONOutput(true)
	_, okJSON := testLogger.logger.Logger.Formatter.(*logrus.JSONFormatter)
	assert.True(t, okJSON)
	assert.Equal(t, "fakeLogger", testLogger.logger.Data[logFieldScope])
	assert.Equal(t, LogTypeLog, testLogger.logger.Data[logFieldType])
	assert.Equal(t, expectedHost, testLogger.logger.Data[logFieldInstance])
	assert.Equal(t, version.Version(), testLogger.logger.Data[logFieldPulseVer])

	testLogger.EnableJSONOutput(false)
	_, okText := testLogger.logger.Logger.Formatter.(*logrus.TextFormatter)
	assert.True(t, okText)
	assert.Equal(t, "fakeLogger", testLogger.logger.Data[logFieldScope])
	assert.Equal(t, LogTypeLog, testLogger.logger.Data[logFieldType])
	assert.Equal(t, expectedHost, testLogger.logger.Data[logFieldInstance])
	assert.Equal(t, version.Version(), testLogger.logger.Data[logFieldPulseVer])
}

func TestJSONLoggerFields(t *testing.T) {
	tests := []struct {
		name        string
		outputLevel LogLevel
		level       string
		appID       string
		message     string
		instance    string
		fn          func(*pulseLogger, string)
	}{
		{
			"info()",
			InfoLevel,
			"info",
			"pulse_app",
			"King pulse",
			"pulse-pod",
			func(l *pulseLogger, msg string) {
				l.Info(msg)
			},
		},
		{
			"infof()",
			InfoLevel,
			"info",
			"pulse_app",
			"King pulse",
			"pulse-pod",
			func(l *pulseLogger, msg string) {
				l.Infof("%s", msg)
			},
		},
		{
			"debug()",
			DebugLevel,
			"debug",
			"pulse_app",
			"King pulse",
			"pulse-pod",
			func(l *pulseLogger, msg string) {
				l.Debug(msg)
			},
		},
		{
			"debugf()",
			DebugLevel,
			"debug",
			"pulse_app",
			"King pulse",
			"pulse-pod",
			func(l *pulseLogger, msg string) {
				l.Debugf("%s", msg)
			},
		},
		{
			"error()",
			InfoLevel,
			"error",
			"pulse_app",
			"King pulse",
			"pulse-pod",
			func(l *pulseLogger, msg string) {
				l.Error(msg)
			},
		},
		{
			"errorf()",
			InfoLevel,
			"error",
			"pulse_app",
			"King pulse",
			"pulse-pod",
			func(l *pulseLogger, msg string) {
				l.Errorf("%s", msg)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			testLogger := getTestLogger(&buf)
			testLogger.EnableJSONOutput(true)
			testLogger.SetAppID(tt.appID)
			testLogger.SetOutputLevel(tt.outputLevel)
			testLogger.logger.Data[logFieldInstance] = tt.instance

			tt.fn(testLogger, tt.message)

			b, _ := buf.ReadBytes('\n')
			var o map[string]interface{}
			_ = json.Unmarshal(b, &o)

			// assert
			assert.Equal(t, tt.appID, o[logFieldAppID])
			assert.Equal(t, tt.instance, o[logFieldInstance])
			assert.Equal(t, tt.level, o[logFieldLevel])
			assert.Equal(t, LogTypeLog, o[logFieldType])
			assert.Equal(t, fakeLoggerName, o[logFieldScope])
			assert.Equal(t, tt.message, o[logFieldMessage])
			_, err := time.Parse(time.RFC3339, o[logFieldTimeStamp].(string))
			assert.NoError(t, err)
		})
	}
}

func TestWithTypeFields(t *testing.T) {
	var buf bytes.Buffer
	testLogger := getTestLogger(&buf)
	testLogger.EnableJSONOutput(true)
	testLogger.SetAppID("pulse_app")
	testLogger.SetOutputLevel(InfoLevel)

	// WithLogType will return new Logger with request log type
	// Meanwhile, testLogger uses the default logtype
	loggerWithRequestType := testLogger.WithLogType(LogTypeRequest)
	loggerWithRequestType.Info("call user app")

	b, _ := buf.ReadBytes('\n')
	var o map[string]interface{}
	_ = json.Unmarshal(b, &o)

	assert.Equalf(t, LogTypeRequest, o[logFieldType], "new logger must be %s type", LogTypeRequest)

	// Log our via testLogger to ensure that testLogger still uses the default logtype
	testLogger.Info("testLogger with log LogType")

	b, _ = buf.ReadBytes('\n')
	_ = json.Unmarshal(b, &o)

	assert.Equalf(t, LogTypeLog, o[logFieldType], "testLogger must be %s type", LogTypeLog)
}

func TestToLogrusLevel(t *testing.T) {
	t.Run("pulse DebugLevel to Logrus.DebugLevel", func(t *testing.T) {
		assert.Equal(t, logrus.DebugLevel, toLogrusLevel(DebugLevel))
	})

	t.Run("pulse InfoLevel to Logrus.InfoLevel", func(t *testing.T) {
		assert.Equal(t, logrus.InfoLevel, toLogrusLevel(InfoLevel))
	})

	t.Run("pulse WarnLevel to Logrus.WarnLevel", func(t *testing.T) {
		assert.Equal(t, logrus.WarnLevel, toLogrusLevel(WarnLevel))
	})

	t.Run("pulse ErrorLevel to Logrus.ErrorLevel", func(t *testing.T) {
		assert.Equal(t, logrus.ErrorLevel, toLogrusLevel(ErrorLevel))
	})

	t.Run("pulse FatalLevel to Logrus.FatalLevel", func(t *testing.T) {
		assert.Equal(t, logrus.FatalLevel, toLogrusLevel(FatalLevel))
	})
}
