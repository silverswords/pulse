package main

import "github.com/silverswords/pulse/pkg/logger"

var (
	log                      = logger.NewLogger("pulse.runtime")
	logContrib logger.Logger = logger.NewLogger("pulse.contrib")
)

func main() {
	log.Info("Starting")
}
