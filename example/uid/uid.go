package main

import (
	"github.com/nats-io/nuid"
	"log"
)

func main() {
	var uidGen = nuid.New()
	var repeatMap = make(map[string]bool)
	for {
		uid := uidGen.Next()
		if repeatMap[uid] {
			log.Println("repeat")
		}
		repeatMap[uid] = true
	}
}
