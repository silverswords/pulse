// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package logger

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOptions(t *testing.T) {
	t.Run("default options", func(t *testing.T) {
		o := DefaultOptions()
		assert.Equal(t, defaultJSONOutput, o.JSONFormatEnabled)
		assert.Equal(t, undefinedAppID, o.appID)
		assert.Equal(t, defaultOutputLevel, o.OutputLevel)
	})

	t.Run("set pulse ID", func(t *testing.T) {
		o := DefaultOptions()
		assert.Equal(t, undefinedAppID, o.appID)

		o.SetAppID("pulse-app")
		assert.Equal(t, "pulse-app", o.appID)
	})

	t.Run("attaching log related cmd flags", func(t *testing.T) {
		o := DefaultOptions()

		logLevelAsserted := false
		testStringVarFn := func(p *string, name string, value string, usage string) {
			if name == "log-level" && value == defaultOutputLevel {
				logLevelAsserted = true
			}
		}

		logAsJSONAsserted := false
		testBoolVarFn := func(p *bool, name string, value bool, usage string) {
			if name == "log-as-json" && value == defaultJSONOutput {
				logAsJSONAsserted = true
			}
		}

		o.AttachCmdFlags(testStringVarFn, testBoolVarFn)

		// assert
		assert.True(t, logLevelAsserted)
		assert.True(t, logAsJSONAsserted)
	})
}

func TestApplyOptionsToLoggers(t *testing.T) {
	testOptions := Options{
		JSONFormatEnabled: true,
		appID:             "pulse-app",
		OutputLevel:       "debug",
	}

	// Create two loggers
	var testLoggers = []Logger{
		NewLogger("testLogger0"),
		NewLogger("testLogger1"),
	}

	for _, l := range testLoggers {
		l.EnableJSONOutput(false)
		l.SetOutputLevel(InfoLevel)
	}

	_ = ApplyOptionsToLoggers(&testOptions)

	for _, l := range testLoggers {
		assert.Equal(
			t,
			"pulse-app",
			(l.(*pulseLogger)).logger.Data[logFieldAppID])
		assert.Equal(
			t,
			toLogrusLevel(DebugLevel),
			(l.(*pulseLogger)).logger.Logger.GetLevel())
	}
}
