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
	case "seq_gap":
		runSeqGap(js, *duration)
	case "max_pending":
		runMaxPending(js, *duration)
	case "missing_progress":
		runMissingProgress(js, *duration)
	default:
		log.Fatalf("unknown scenario: %s", *scenario)
	}
}

// ── AckWaitViolation ──────────────────────────────────────────────────────────
// Creates a consumer with ack_wait=2s. Pulls messages and holds them for 3s
// without acking. NATS redelivers → num_redelivered grows → nats-lens detects.
//
// NOTE on fetch-buffer semantics (nats.go v1.37):
// PullSubscribe.Fetch() delivers messages in NATS sequence order. After
// ack_wait fires, redelivered messages hold their original sequence position
// and are delivered before new messages on the next Fetch(). This causes the
// same N messages to cycle continuously, keeping num_redelivered stable at N
// rather than growing — matching the NAK_STORM metric signature instead of
// ACK_WAIT_VIOLATION. nats-lens correctly fires NAK_STORM for this pattern.
// See the multilang evaluation section of the paper for the full analysis.
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

// ── SequenceGap ───────────────────────────────────────────────────────────────
// Stream retains only last 10 messages; consumer falls behind → gap detected.
func runSeqGap(js nats.JetStreamContext, dur time.Duration) {
	const stream   = "GOLANG_SEQ"
	const consumer = "go-seq-consumer"
	const subject  = "go.seq.msg"

	js.AddStream(&nats.StreamConfig{
		Name:     stream,
		Subjects: []string{subject},
		Storage:  nats.MemoryStorage,
		MaxMsgs:  10, // tiny retention
	})
	js.DeleteConsumer(stream, consumer)
	js.PurgeStream(stream)
	js.AddConsumer(stream, &nats.ConsumerConfig{
		Durable:       consumer,
		AckPolicy:     nats.AckExplicitPolicy,
		AckWait:       300 * time.Second,
		MaxAckPending: 512,
	})

	// Publish 200 messages into a 10-msg stream → evicts first 190 → gap
	for i := 0; i < 200; i++ {
		js.Publish(subject, []byte(fmt.Sprintf(`{"i":%d}`, i)))
	}
	fmt.Println("[Go SeqGap] 190 messages evicted. nats-lens should detect SequenceGap.")

	deadline := time.Now().Add(dur)
	for time.Now().Before(deadline) {
		time.Sleep(2 * time.Second)
	}

	js.DeleteConsumer(stream, consumer)
	js.PurgeStream(stream)
	fmt.Println("[Go SeqGap] Done.")
}

// ── MaxPendingThrottle ────────────────────────────────────────────────────────
// num_ack_pending == max_ack_pending AND num_pending > 0 → delivery throttled.
func runMaxPending(js nats.JetStreamContext, dur time.Duration) {
	const stream   = "GOLANG_MAX"
	const consumer = "go-max-consumer"
	const subject  = "go.max.msg"

	js.AddStream(&nats.StreamConfig{
		Name:     stream,
		Subjects: []string{subject},
		Storage:  nats.MemoryStorage,
		MaxMsgs:  5000,
	})
	js.DeleteConsumer(stream, consumer)
	js.PurgeStream(stream)
	js.AddConsumer(stream, &nats.ConsumerConfig{
		Durable:       consumer,
		AckPolicy:     nats.AckExplicitPolicy,
		AckWait:       300 * time.Second,
		MaxAckPending: 5, // tiny window
	})

	for i := 0; i < 100; i++ {
		js.Publish(subject, []byte(fmt.Sprintf(`{"i":%d}`, i)))
	}

	sub, _ := js.PullSubscribe(subject, consumer, nats.Bind(stream, consumer))
	// Pull without acking → fills the 5-slot pending window
	sub.Fetch(5, nats.MaxWait(2*time.Second))
	fmt.Println("[Go MaxPending] Window full. nats-lens should detect MaxPendingThrottle.")

	deadline := time.Now().Add(dur)
	for time.Now().Before(deadline) {
		time.Sleep(2 * time.Second)
	}

	js.DeleteConsumer(stream, consumer)
	js.PurgeStream(stream)
	fmt.Println("[Go MaxPending] Done.")
}

// ── MissingProgress ───────────────────────────────────────────────────────────
// num_ack_pending/max_ack_pending >= 0.9 AND ack_wait > 30s.
func runMissingProgress(js nats.JetStreamContext, dur time.Duration) {
	const stream   = "GOLANG_MISS"
	const consumer = "go-miss-consumer"
	const subject  = "go.miss.msg"

	js.AddStream(&nats.StreamConfig{
		Name:     stream,
		Subjects: []string{subject},
		Storage:  nats.MemoryStorage,
		MaxMsgs:  5000,
	})
	js.DeleteConsumer(stream, consumer)
	js.PurgeStream(stream)
	js.AddConsumer(stream, &nats.ConsumerConfig{
		Durable:       consumer,
		AckPolicy:     nats.AckExplicitPolicy,
		AckWait:       300 * time.Second,
		MaxAckPending: 10,
	})

	for i := 0; i < 50; i++ {
		js.Publish(subject, []byte(fmt.Sprintf(`{"i":%d}`, i)))
	}

	sub, _ := js.PullSubscribe(subject, consumer, nats.Bind(stream, consumer))
	// Pull 9/10 of max_ack_pending without acking → ratio = 0.9
	sub.Fetch(9, nats.MaxWait(2*time.Second))
	fmt.Println("[Go MissingProgress] ratio=0.9. nats-lens should detect MissingProgress.")

	deadline := time.Now().Add(dur)
	for time.Now().Before(deadline) {
		time.Sleep(2 * time.Second)
	}

	js.DeleteConsumer(stream, consumer)
	js.PurgeStream(stream)
	fmt.Println("[Go MissingProgress] Done.")
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
