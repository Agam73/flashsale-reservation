package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	kafka "github.com/segmentio/kafka-go"

	"github.com/Agam73/flashsale-reservation/internal/admission"
	"github.com/Agam73/flashsale-reservation/internal/kafkax"
	"github.com/Agam73/flashsale-reservation/internal/redisx"
)

// requireKafka skips the test if no broker answers at addr within a
// short timeout -- mirrors cmd/checkout-api's helper of the same name
// (duplicated rather than shared, same as testRedis, since this
// project's convention is small per-package test helpers over a
// shared test-utility package).
func requireKafka(t *testing.T, addr string) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Skipf("skipping: no local Kafka available at %s: %v", addr, err)
	}
	conn.Close()
}

// testRedis connects to the same local Redis this service targets by
// default, skipping cleanly if it's not reachable -- same pattern
// cmd/checkout-api's tests already use.
func testRedis(t *testing.T) *redis.Client {
	t.Helper()
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Skipf("skipping: no local Redis available: %v", err)
	}
	t.Cleanup(func() { client.Close() })
	return client
}

func doQueueDepth(t *testing.T, registry *admission.Registry, redisClient *redis.Client, itemID string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest("GET", "/items/"+itemID+"/queue", nil)
	req.SetPathValue("itemID", itemID)
	rec := httptest.NewRecorder()
	handleQueueDepth(registry, redisClient)(rec, req)
	return rec
}

func TestHandleQueueDepth_MissingItemID(t *testing.T) {
	redisClient := testRedis(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	registry := admission.NewRegistry(ctx, 5)

	req := httptest.NewRequest("GET", "/items//queue", nil)
	req.SetPathValue("itemID", "")
	rec := httptest.NewRecorder()
	handleQueueDepth(registry, redisClient)(rec, req)

	if rec.Code != 400 {
		t.Errorf("expected 400 for missing item id, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestHandleQueueDepth_ZeroBeforeAnyoneJoins(t *testing.T) {
	redisClient := testRedis(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	registry := admission.NewRegistry(ctx, 5)

	itemID := "queue-test-empty"
	rec := doQueueDepth(t, registry, redisClient, itemID)
	if rec.Code != 200 {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp queueDepthResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decoding response: %v", err)
	}
	if resp.Waiting != 0 {
		t.Errorf("expected 0 waiting before anyone joins, got %d", resp.Waiting)
	}
}

// TestHandleQueueDepth_ReflectsWaitingBuyersAndCachesToRedis checks
// both halves of this handler: it reports the Admitter's real depth,
// and it publishes that same number into Redis for other readers.
func TestHandleQueueDepth_ReflectsWaitingBuyersAndCachesToRedis(t *testing.T) {
	redisClient := testRedis(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	registry := admission.NewRegistry(ctx, 1) // slow admit rate so joins outpace it

	itemID := "queue-test-waiting"
	a := registry.For(itemID)

	for i := 0; i < 2; i++ {
		go func() {
			_, admitted, err := a.Join(context.Background())
			if err == nil {
				<-admitted
			}
		}()
	}

	// Wait for both joins to register before asserting -- the
	// Admitter serializes them one at a time over its requests
	// channel.
	deadline := time.After(500 * time.Millisecond)
	var rec *httptest.ResponseRecorder
	for {
		rec = doQueueDepth(t, registry, redisClient, itemID)
		var resp queueDepthResponse
		if err := json.NewDecoder(rec.Body).Decode(&resp); err == nil && resp.Waiting == 2 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("expected queue depth to reach 2, last response: %s", rec.Body.String())
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}

	n, err := redisx.GetQueueDepth(context.Background(), redisClient, itemID)
	if err != nil {
		t.Fatalf("GetQueueDepth: %v", err)
	}
	if n != 2 {
		t.Errorf("expected redis to cache queue depth 2, got %d", n)
	}
}

// --- Phase 10: handleJoin also publishes a BuyerJoined event. ---

// waitWithTimeout reports whether wg finished within timeout, without
// blocking the test forever if it didn't -- a plain wg.Wait() call
// has no such escape hatch.
func waitWithTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-time.After(timeout):
		return false
	}
}

// TestHandleJoin_AdmitsEvenWhenKafkaUnreachable is this phase's actual
// correctness requirement: publishing the BuyerJoined event must never
// be able to slow down or block admission. It points the writer at a
// broker that doesn't exist and checks the HTTP response still comes
// back quickly and successfully -- the publish failure only shows up
// in the background goroutine that publishWG lets the test (and, in
// production, shutdown) wait for separately.
func TestHandleJoin_AdmitsEvenWhenKafkaUnreachable(t *testing.T) {
	redisClient := testRedis(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	registry := admission.NewRegistry(ctx, 10)

	kafkaWriter := kafkax.NewWaitingRoomWriter([]string{"127.0.0.1:1"}) // nothing listens here
	defer kafkaWriter.Close()
	var publishWG sync.WaitGroup

	body, _ := json.Marshal(joinRequest{UserID: "alice"})
	req := httptest.NewRequest("POST", "/items/concert-ticket/join", bytes.NewReader(body))
	req.SetPathValue("itemID", "concert-ticket")
	rec := httptest.NewRecorder()

	start := time.Now()
	handleJoin(registry, redisClient, kafkaWriter, &publishWG, time.Minute)(rec, req)
	elapsed := time.Since(start)

	if rec.Code != 200 {
		t.Fatalf("expected 200 even though kafka is unreachable, got %d: %s", rec.Code, rec.Body.String())
	}
	if elapsed > time.Second {
		t.Errorf("expected handleJoin to return quickly regardless of kafka reachability, took %s", elapsed)
	}

	var resp joinResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decoding response: %v", err)
	}
	if resp.Status != "admitted" {
		t.Errorf("expected status=admitted, got %q", resp.Status)
	}

	// The background publish attempt should still eventually finish
	// (and fail, since nothing's listening) rather than leaking
	// forever -- it's bounded by its own 5s internal timeout.
	if !waitWithTimeout(&publishWG, 10*time.Second) {
		t.Error("expected the background publish goroutine to finish within its own timeout budget")
	}
}

func TestHandleJoin_MissingUserID(t *testing.T) {
	redisClient := testRedis(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	registry := admission.NewRegistry(ctx, 10)
	kafkaWriter := kafkax.NewWaitingRoomWriter([]string{"127.0.0.1:1"})
	defer kafkaWriter.Close()
	var publishWG sync.WaitGroup

	req := httptest.NewRequest("POST", "/items/concert-ticket/join", bytes.NewReader([]byte(`{}`)))
	req.SetPathValue("itemID", "concert-ticket")
	rec := httptest.NewRecorder()

	handleJoin(registry, redisClient, kafkaWriter, &publishWG, time.Minute)(rec, req)

	if rec.Code != 400 {
		t.Errorf("expected 400 for a missing user_id, got %d: %s", rec.Code, rec.Body.String())
	}
}

// TestHandleJoin_PublishesReadableBuyerJoinedEvent is the real
// end-to-end check for this phase: an admitted buyer must actually
// produce a readable BuyerJoined message on the Kafka topic, matching
// what the HTTP response says happened. Complements
// TestHandleJoin_AdmitsEvenWhenKafkaUnreachable, which proves the
// *absence* of blocking; this proves the *presence* of a correct
// message when a broker is actually there to receive it. Skips
// cleanly if one isn't (see requireKafka), same as
// checkout-api's equivalent test for CheckoutAttemptsTopic.
//
// Same scan-forward-from-FirstOffset approach as checkout-api's
// version, for the same reason: avoids a join-timing race against a
// fresh consumer group positioned at LastOffset.
func TestHandleJoin_PublishesReadableBuyerJoinedEvent(t *testing.T) {
	requireKafka(t, "localhost:9092")
	redisClient := testRedis(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	registry := admission.NewRegistry(ctx, 10)

	kafkaWriter := kafkax.NewWaitingRoomWriter([]string{"localhost:9092"})
	defer kafkaWriter.Close()
	var publishWG sync.WaitGroup

	itemID := fmt.Sprintf("test-item-join-%d", time.Now().UnixNano())
	userID := fmt.Sprintf("test-user-%d", time.Now().UnixNano())

	groupReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{"localhost:9092"},
		Topic:       kafkax.WaitingRoomJoinsTopic,
		GroupID:     fmt.Sprintf("test-waiting-room-join-%d", time.Now().UnixNano()),
		StartOffset: kafka.FirstOffset,
	})
	defer groupReader.Close()

	body, _ := json.Marshal(joinRequest{UserID: userID})
	req := httptest.NewRequest("POST", "/items/"+itemID+"/join", bytes.NewReader(body))
	req.SetPathValue("itemID", itemID)
	rec := httptest.NewRecorder()

	handleJoin(registry, redisClient, kafkaWriter, &publishWG, time.Minute)(rec, req)
	if rec.Code != 200 {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	// The publish itself happens in a background goroutine (see
	// handleJoin) -- wait for it to actually finish before trying to
	// read it back, rather than assuming the HTTP response implies
	// the message already landed.
	if !waitWithTimeout(&publishWG, 10*time.Second) {
		t.Fatal("expected the background publish goroutine to finish")
	}

	fetchCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var found *kafkax.BuyerJoined
	var foundKey []byte
	for {
		msg, err := groupReader.FetchMessage(fetchCtx)
		if err != nil {
			break // timeout: fall through to the failure check below
		}
		if event, decodeErr := kafkax.DecodeBuyerJoined(msg.Value); decodeErr == nil && event.UserID == userID && event.ItemID == itemID {
			e := event
			found = &e
			foundKey = msg.Key
			_ = groupReader.CommitMessages(fetchCtx, msg)
			break
		}
		_ = groupReader.CommitMessages(fetchCtx, msg)
	}

	if found == nil {
		t.Fatal("did not find a Kafka message matching this join within 15s of scanning from the beginning of the topic")
	}
	if found.RemoteAddr == "" {
		t.Error("expected a non-empty remote_addr")
	}
	if found.JoinedAt.IsZero() {
		t.Error("expected a non-zero joined_at")
	}
	if string(foundKey) != userID {
		t.Errorf("expected message key=%q (user ID, for partitioning), got %q", userID, string(foundKey))
	}
}
