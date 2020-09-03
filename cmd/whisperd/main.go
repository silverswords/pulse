package main

import "github.com/silverswords/pulse/pkg/logger"

var (
	log                      = logger.NewLogger("dapr.runtime")
	logContrib logger.Logger = logger.NewLogger("dapr.contrib")
)

func main() {
	log.Info("Starting")
}
