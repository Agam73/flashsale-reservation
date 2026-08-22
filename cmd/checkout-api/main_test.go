package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	kafka "github.com/segmentio/kafka-go"

	"github.com/Agam73/flashsale-reservation/internal/kafkax"
	"github.com/Agam73/flashsale-reservation/internal/redisx"
)

const testAdmissionTTL = 2 * time.Minute

// testRedis connects to the same local Redis this service targets by
// default, skipping if it's not reachable -- same pattern
// internal/decision/processor_test.go already uses for Postgres.
func testRedis(t *testing.T) *redis.Client {
	t.Helper()
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Skipf("skipping: no local Redis available: %v", err)
	}
	t.Cleanup(func() { client.Close() })
	return client
}

// unreachableRedisClient and unreachableKafkaWriter back the
// validation-only tests below, which must never actually reach the
// network -- if they did, that would itself be the bug (validation
// should short-circuit before either client is touched).
func unreachableRedisClient() *redis.Client {
	return redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
}

func unreachableKafkaWriter() *kafka.Writer {
	return kafkax.NewWriter([]string{"127.0.0.1:1"})
}

func doCheckout(t *testing.T, redisClient *redis.Client, kafkaWriter *kafka.Writer, itemID string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			t.Fatalf("encoding request body: %v", err)
		}
	}
	req := httptest.NewRequest("POST", "/items/"+itemID+"/checkout", &buf)
	req.SetPathValue("itemID", itemID)
	rec := httptest.NewRecorder()
	handleCheckout(redisClient, kafkaWriter, testAdmissionTTL)(rec, req)
	return rec
}

// --- Validation paths: no live infra needed or touched. ---

func TestHandleCheckout_MissingItemID(t *testing.T) {
	redisClient := unreachableRedisClient()
	defer redisClient.Close()
	kafkaWriter := unreachableKafkaWriter()
	defer kafkaWriter.Close()

	req := httptest.NewRequest("POST", "/items//checkout", nil)
	req.SetPathValue("itemID", "")
	rec := httptest.NewRecorder()
	handleCheckout(redisClient, kafkaWriter, testAdmissionTTL)(rec, req)

	if rec.Code != 400 {
		t.Errorf("expected 400 for missing item id, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestHandleCheckout_InvalidJSON(t *testing.T) {
	redisClient := unreachableRedisClient()
	defer redisClient.Close()
	kafkaWriter := unreachableKafkaWriter()
	defer kafkaWriter.Close()

	req := httptest.NewRequest("POST", "/items/concert-ticket/checkout", bytes.NewBufferString("not json"))
	req.SetPathValue("itemID", "concert-ticket")
	rec := httptest.NewRecorder()
	handleCheckout(redisClient, kafkaWriter, testAdmissionTTL)(rec, req)

	if rec.Code != 400 {
		t.Errorf("expected 400 for invalid JSON body, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestHandleCheckout_MissingUserID(t *testing.T) {
	redisClient := unreachableRedisClient()
	defer redisClient.Close()
	kafkaWriter := unreachableKafkaWriter()
	defer kafkaWriter.Close()

	rec := doCheckout(t, redisClient, kafkaWriter, "concert-ticket", checkoutRequest{UserID: "", Quantity: 1})
	if rec.Code != 400 {
		t.Errorf("expected 400 for missing user_id, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestHandleCheckout_NonPositiveQuantity(t *testing.T) {
	redisClient := unreachableRedisClient()
	defer redisClient.Close()
	kafkaWriter := unreachableKafkaWriter()
	defer kafkaWriter.Close()

	for _, qty := range []int64{0, -1} {
		rec := doCheckout(t, redisClient, kafkaWriter, "concert-ticket", checkoutRequest{UserID: "alice", Quantity: qty})
		if rec.Code != 400 {
			t.Errorf("expected 400 for quantity=%d, got %d: %s", qty, rec.Code, rec.Body.String())
		}
	}
}

// --- Live-infra paths: require local Redis + Kafka, same ones Phase
// 5/6 CLI testing already used. Skip cleanly if unavailable. ---

func TestHandleCheckout_NotAdmitted(t *testing.T) {
	redisClient := testRedis(t)
	kafkaWriter := kafkax.NewWriter([]string{"localhost:9092"})
	defer kafkaWriter.Close()

	itemID := fmt.Sprintf("test-item-not-admitted-%d", time.Now().UnixNano())
	if err := redisx.SeedInventory(context.Background(), redisClient, itemID, 5); err != nil {
		t.Fatalf("seeding inventory: %v", err)
	}
	// Deliberately never calling redisx.GrantAdmission for this buyer --
	// that's the whole point of this test.

	rec := doCheckout(t, redisClient, kafkaWriter, itemID, checkoutRequest{UserID: "alice", Quantity: 1})
	if rec.Code != 403 {
		t.Errorf("expected 403 for a buyer who never joined the waiting room, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestHandleCheckout_SoldOut(t *testing.T) {
	redisClient := testRedis(t)
	kafkaWriter := kafkax.NewWriter([]string{"localhost:9092"})
	defer kafkaWriter.Close()

	itemID := fmt.Sprintf("test-item-sold-out-%d", time.Now().UnixNano())
	if err := redisx.SeedInventory(context.Background(), redisClient, itemID, 0); err != nil {
		t.Fatalf("seeding inventory: %v", err)
	}
	if err := redisx.GrantAdmission(context.Background(), redisClient, itemID, "alice", testAdmissionTTL); err != nil {
		t.Fatalf("granting admission: %v", err)
	}

	rec := doCheckout(t, redisClient, kafkaWriter, itemID, checkoutRequest{UserID: "alice", Quantity: 1})
	if rec.Code != 409 {
		t.Errorf("expected 409 sold out, got %d: %s", rec.Code, rec.Body.String())
	}
}

// TestHandleCheckout_SuccessPublishesToKafka is the real end-to-end
// check for this phase: a successful checkout must actually produce a
// readable message on the Kafka topic, keyed by item ID, matching what
// the HTTP response claims was sent.
//
// Uses a unique, fresh consumer group reading from FirstOffset and
// scans forward for the matching idempotency key, rather than assuming
// it's the very next message -- avoids a join-timing race against a
// GroupID positioned at LastOffset, at the cost of scanning past
// whatever history already exists on the topic from manual CLI
// testing. Fine locally; would need rethinking against a topic with
// serious message volume.
func TestHandleCheckout_SuccessPublishesToKafka(t *testing.T) {
	redisClient := testRedis(t)
	writerForCheckout := kafkax.NewWriter([]string{"localhost:9092"})
	defer writerForCheckout.Close()

	itemID := fmt.Sprintf("test-item-success-%d", time.Now().UnixNano())
	ctx := context.Background()

	if err := redisx.SeedInventory(ctx, redisClient, itemID, 5); err != nil {
		t.Fatalf("seeding inventory: %v", err)
	}
	if err := redisx.GrantAdmission(ctx, redisClient, itemID, "alice", testAdmissionTTL); err != nil {
		t.Fatalf("granting admission: %v", err)
	}

	groupReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{"localhost:9092"},
		Topic:       kafkax.CheckoutAttemptsTopic,
		GroupID:     fmt.Sprintf("test-checkout-success-%d", time.Now().UnixNano()),
		StartOffset: kafka.FirstOffset,
	})
	defer groupReader.Close()

	rec := doCheckout(t, redisClient, writerForCheckout, itemID, checkoutRequest{UserID: "alice", Quantity: 2})
	if rec.Code != 200 {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp checkoutResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decoding response: %v", err)
	}
	if resp.Remaining != 3 {
		t.Errorf("expected remaining=3 (5-2), got %d", resp.Remaining)
	}
	if resp.IdempotencyKey == "" {
		t.Fatal("expected a non-empty idempotency key")
	}

	fetchCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	var found *kafkax.PurchaseAttempted
	var foundKey []byte
	for {
		msg, err := groupReader.FetchMessage(fetchCtx)
		if err != nil {
			break // timeout: fall through to the failure check below
		}
		if attempt, decodeErr := kafkax.DecodeAttempt(msg.Value); decodeErr == nil && attempt.IdempotencyKey == resp.IdempotencyKey {
			a := attempt
			found = &a
			foundKey = msg.Key
			_ = groupReader.CommitMessages(fetchCtx, msg)
			break
		}
		_ = groupReader.CommitMessages(fetchCtx, msg)
	}

	if found == nil {
		t.Fatal("did not find a Kafka message matching this checkout's idempotency key within 15s of scanning from the beginning of the topic")
	}
	if found.ItemID != itemID {
		t.Errorf("expected published item_id=%q, got %q", itemID, found.ItemID)
	}
	if found.UserID != "alice" {
		t.Errorf("expected published user_id=%q, got %q", "alice", found.UserID)
	}
	if found.Quantity != 2 {
		t.Errorf("expected published quantity=2, got %d", found.Quantity)
	}
	if string(foundKey) != itemID {
		t.Errorf("expected message key=%q (item ID, for partitioning), got %q", itemID, string(foundKey))
	}
}
