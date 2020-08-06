package main

import "github.com/silverswords/whisper/pkg/logger"

var (
	log = logger.NewLogger("dapr.runtime")
	logContrib = logger.NewLogger("dapr.contrib")
)

func main() {
	log.Info("Starting")
}
