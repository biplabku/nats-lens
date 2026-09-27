// Go consumer that deliberately triggers AckWaitViolation and NakStorm.
// Used in the paper evaluation to demonstrate nats-lens detects violations
// regardless of the consumer language or client library.
//
// Usage:
//   go run . --nats nats://localhost:4222 --scenario ack_wait
//   go run . --nats nats://localhost:4222 --scenario nak_storm
package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats.go"
)

func main() {
	natsURL  := flag.String("nats", "nats://localhost:4222", "NATS server URL")
	scenario := flag.String("scenario", "ack_wait", "Scenario: ack_wait | nak_storm | healthy")
	duration := flag.Duration("duration", 30*time.Second, "How long to run")
	flag.Parse()

	nc, err := nats.Connect(*natsURL)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		log.Fatalf("jetstream: %v", err)
	}

	switch *scenario {
	case "ack_wait":
		runAckWait(js, *duration)
	case "nak_storm":
		runNakStorm(js, *duration)
	case "healthy":
		runHealthy(js, *duration)
	default:
		log.Fatalf("unknown scenario: %s", *scenario)
	}
}

// ── AckWaitViolation ──────────────────────────────────────────────────────────
// Creates a consumer with ack_wait=4s. Pulls messages and holds them for 6s
// without acking. NATS redelivers → num_redelivered grows → nats-lens detects.
func runAckWait(js nats.JetStreamContext, dur time.Duration) {
	const stream   = "GOLANG_ACK"
	const consumer = "go-ack-consumer"
	const subject  = "go.ack.msg"

	// Ensure stream (create or get existing)
	js.AddStream(&nats.StreamConfig{
		Name:     stream,
		Subjects: []string{subject},
		Storage:  nats.MemoryStorage,
		MaxMsgs:  5000,
	})

	// Delete consumer if it exists with stale config — AddConsumer silently fails
	// on config mismatch, leaving the old (possibly wrong) config in place.
	js.DeleteConsumer(stream, consumer)

	// Purge stale messages from previous runs
	js.PurgeStream(stream)

	// Consumer with very short ack_wait — each pull cycle delivers NEW messages
	// to num_redelivered, causing it to grow across engine poll snapshots.
	// ack_wait=2s ensures fast redelivery; max_ack_pending=20 allows pulling
	// new messages each cycle (growing num_redelivered with distinct messages).
	js.AddConsumer(stream, &nats.ConsumerConfig{
		Durable:       consumer,
		AckPolicy:     nats.AckExplicitPolicy,
		AckWait:       2 * time.Second,
		MaxAckPending: 20,
	})

	// Publish enough messages to sustain multiple pull cycles
	for i := 0; i < 50; i++ {
		js.Publish(subject, []byte(fmt.Sprintf(`{"i":%d}`, i)))
	}

	sub, _ := js.PullSubscribe(subject, consumer, nats.Bind(stream, consumer))
	deadline := time.Now().Add(dur)

	fmt.Println("[Go AckWait] Running. nats-lens should detect AckWaitViolation.")
	for time.Now().Before(deadline) {
		// Pull 5 messages, hold 3s (> ack_wait=2s) — NATS redelivers them.
		// On next iteration, fetch pulls ADDITIONAL new messages from the stream
		// (max_ack_pending=20, so 15 slots remain), growing num_redelivered.
		msgs, err := sub.Fetch(5, nats.MaxWait(1*time.Second))
		if err != nil {
			time.Sleep(300 * time.Millisecond)
			continue
		}
		fmt.Printf("[Go AckWait] pulled %d, holding 3s (ack_wait=2s)\n", len(msgs))
		time.Sleep(3 * time.Second)
		// msgs out of scope — ack_wait fires, num_redelivered grows
	}

	// Cleanup
	js.DeleteConsumer(stream, consumer)
	js.PurgeStream(stream)
	fmt.Println("[Go AckWait] Done.")
}

// ── NakStorm ──────────────────────────────────────────────────────────────────
// Creates a consumer and immediately NAKs every message with 500ms delay.
// num_redelivered stays elevated → nats-lens detects NakStorm.
func runNakStorm(js nats.JetStreamContext, dur time.Duration) {
	const stream   = "GOLANG_NAK"
	const consumer = "go-nak-consumer"
	const subject  = "go.nak.msg"

	js.AddStream(&nats.StreamConfig{
		Name:     stream,
		Subjects: []string{subject},
		Storage:  nats.MemoryStorage,
		MaxMsgs:  5000,
	})

	// Delete stale consumer, purge stream, recreate fresh
	js.DeleteConsumer(stream, consumer)
	js.PurgeStream(stream)

	js.AddConsumer(stream, &nats.ConsumerConfig{
		Durable:       consumer,
		AckPolicy:     nats.AckExplicitPolicy,
		AckWait:       30 * time.Second,
		MaxAckPending: 50,
	})

	for i := 0; i < 50; i++ {
		js.Publish(subject, []byte(fmt.Sprintf(`{"i":%d}`, i)))
	}

	sub, _ := js.PullSubscribe(subject, consumer, nats.Bind(stream, consumer))
	deadline := time.Now().Add(dur)

	fmt.Println("[Go NakStorm] Running. nats-lens should detect NakStorm.")
	for time.Now().Before(deadline) {
		msgs, err := sub.Fetch(5, nats.MaxWait(1*time.Second))
		if err != nil {
			time.Sleep(200 * time.Millisecond)
			continue
		}
		for _, msg := range msgs {
			// NAK with explicit 500ms backoff
			msg.NakWithDelay(500 * time.Millisecond)
		}
		fmt.Printf("[Go NakStorm] NAK'd %d messages\n", len(msgs))
		time.Sleep(600 * time.Millisecond)
	}

	js.DeleteConsumer(stream, consumer)
	js.PurgeStream(stream)
	fmt.Println("[Go NakStorm] Done.")
}

// ── Healthy ───────────────────────────────────────────────────────────────────
// Correctly-configured consumer — pulls and promptly acks. No violations.
func runHealthy(js nats.JetStreamContext, dur time.Duration) {
	const stream   = "GOLANG_HEALTHY"
	const consumer = "go-healthy-consumer"
	const subject  = "go.healthy.msg"

	js.AddStream(&nats.StreamConfig{
		Name:     stream,
		Subjects: []string{subject},
		Storage:  nats.MemoryStorage,
		MaxMsgs:  10000,
	})

	js.DeleteConsumer(stream, consumer)
	js.PurgeStream(stream)
	js.AddConsumer(stream, &nats.ConsumerConfig{
		Durable:       consumer,
		AckPolicy:     nats.AckExplicitPolicy,
		AckWait:       300 * time.Second,
		MaxAckPending: 512,
	})

	// Publisher goroutine
	stop := make(chan struct{})
	go func() {
		i := 0
		for {
			select {
			case <-stop:
				return
			default:
				js.Publish(subject, []byte(fmt.Sprintf(`{"seq":%d}`, i)))
				i++
				time.Sleep(200 * time.Millisecond)
			}
		}
	}()

	sub, _ := js.PullSubscribe(subject, consumer, nats.Bind(stream, consumer))
	deadline := time.Now().Add(dur)

	fmt.Println("[Go Healthy] Running correctly. nats-lens should NOT detect violations.")
	for time.Now().Before(deadline) {
		msgs, _ := sub.Fetch(10, nats.MaxWait(500*time.Millisecond))
		for _, msg := range msgs {
			msg.Ack() // promptly ack — no violation
		}
	}

	close(stop)
	js.DeleteConsumer(stream, consumer)
	js.PurgeStream(stream)
	fmt.Println("[Go Healthy] Done. 0 violations expected.")
}
