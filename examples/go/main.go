// Subscribe to nats-lens violation events from Go.
// Run: go run main.go
package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/nats-io/nats.go"
)

type ViolationEvent struct {
	StreamName   string `json:"stream_name"`
	ConsumerName string `json:"consumer_name"`
	Severity     string `json:"severity"`
	Description  string `json:"description"`
	Violation    struct {
		Type       string `json:"type"`
		FixCommand string `json:"fix_command"`
	} `json:"violation"`
}

func main() {
	nc, err := nats.Connect("nats://localhost:4222")
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	fmt.Println("Listening for nats-lens violations...")
	nc.Subscribe("nats.lens.health.violations.>", func(msg *nats.Msg) {
		var event ViolationEvent
		if err := json.Unmarshal(msg.Data, &event); err != nil {
			log.Printf("parse error: %v", err)
			return
		}
		fmt.Printf("[%s] %s on %s/%s\n",
			event.Severity, event.Violation.Type,
			event.StreamName, event.ConsumerName)
		fmt.Printf("  Fix: %s\n", event.Violation.FixCommand)
	})

	select {} // block forever
}
