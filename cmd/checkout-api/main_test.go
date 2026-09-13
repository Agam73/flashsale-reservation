package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net"
	"net/http/httptest"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
	kafka "github.com/segmentio/kafka-go"

	"github.com/Agam73/flashsale-reservation/internal/kafkax"
	"github.com/Agam73/flashsale-reservation/internal/redisx"
)

// requireKafka skips the test if no broker answers at addr within a
// short timeout, instead of letting a real produce/consume call hang
// or fail with a confusing error. Mirrors testRedis/testDB's
// skip-cleanly-if-unavailable philosophy, applied to the one test in
// this file that actually needs a live broker rather than just
// constructing a Writer it never calls WriteMessages on.
func requireKafka(t *testing.T, addr string) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Skipf("skipping: no local Kafka available at %s: %v", addr, err)
	}
	conn.Close()
}

// testDB connects to the same Postgres instance the rest of this
// project's tests target, same skip-cleanly-if-unavailable pattern as
// testRedis below.
func testDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("postgres", "postgres://flashsale:flashsale@localhost:5432/flashsale?sslmode=disable")
	if err != nil {
		t.Skipf("skipping: no local Postgres available: %v", err)
	}
	if err := db.Ping(); err != nil {
		t.Skipf("skipping: no local Postgres available: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// seedPGItem inserts an on-sale item with the given available
// inventory directly into Postgres and returns its ID.
func seedPGItem(t *testing.T, db *sql.DB, available int64) string {
	t.Helper()
	var id string
	err := db.QueryRow(`
		INSERT INTO items (name, price_cents, total_inventory, available_inventory, status)
		VALUES ('Test Item', 1000, $1, $1, 'on_sale')
		RETURNING id
	`, available).Scan(&id)
	if err != nil {
		t.Fatalf("seeding postgres item: %v", err)
	}
	t.Cleanup(func() { db.Exec(`DELETE FROM items WHERE id = $1`, id) })
	return id
}

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
	requireKafka(t, "localhost:9092")
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

// --- Phase 9: reconciliation endpoint. Requires live Postgres + Redis. ---

func doReconcile(t *testing.T, db *sql.DB, redisClient *redis.Client, itemID string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest("POST", "/items/"+itemID+"/reconcile", nil)
	req.SetPathValue("itemID", itemID)
	rec := httptest.NewRecorder()
	handleReconcileItem(db, redisClient)(rec, req)
	return rec
}

// TestHandleReconcileItem_SeedsRedisFromPostgres is the end-to-end
// check for this phase's actual new behavior: an item that only exists
// in Postgres, with no Redis counter at all yet, becomes checkout-able
// after a single call to this endpoint.
func TestHandleReconcileItem_SeedsRedisFromPostgres(t *testing.T) {
	db := testDB(t)
	redisClient := testRedis(t)

	itemID := seedPGItem(t, db, 6)

	if _, err := redisx.GetInventory(context.Background(), redisClient, itemID); err == nil {
		t.Fatal("expected no redis inventory to exist before reconciling")
	}

	rec := doReconcile(t, db, redisClient, itemID)
	if rec.Code != 200 {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decoding response: %v", err)
	}
	if resp["available"].(float64) != 6 {
		t.Errorf("expected available=6 in response, got %v", resp["available"])
	}

	n, err := redisx.GetInventory(context.Background(), redisClient, itemID)
	if err != nil {
		t.Fatalf("GetInventory after reconcile: %v", err)
	}
	if n != 6 {
		t.Errorf("expected redis seeded to 6, got %d", n)
	}
}

// TestHandleReconcileItem_OverwritesDriftedRedisValue checks the drift-
// correction case: Redis already has a (wrong) value, and reconciling
// forces it back to whatever Postgres currently says.
func TestHandleReconcileItem_OverwritesDriftedRedisValue(t *testing.T) {
	db := testDB(t)
	redisClient := testRedis(t)

	itemID := seedPGItem(t, db, 20)
	if err := redisx.SeedInventory(context.Background(), redisClient, itemID, 999); err != nil {
		t.Fatalf("seeding stale redis value: %v", err)
	}

	rec := doReconcile(t, db, redisClient, itemID)
	if rec.Code != 200 {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	n, err := redisx.GetInventory(context.Background(), redisClient, itemID)
	if err != nil {
		t.Fatalf("GetInventory after reconcile: %v", err)
	}
	if n != 20 {
		t.Errorf("expected postgres's value 20 to win over the stale 999, got %d", n)
	}
}

func TestHandleReconcileItem_UnknownItemReturns404(t *testing.T) {
	db := testDB(t)
	redisClient := testRedis(t)

	rec := doReconcile(t, db, redisClient, "00000000-0000-0000-0000-000000000000")
	if rec.Code != 404 {
		t.Errorf("expected 404 for an unknown item, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestHandleReconcileItem_MissingItemID(t *testing.T) {
	db := testDB(t)
	redisClient := testRedis(t)

	req := httptest.NewRequest("POST", "/items//reconcile", nil)
	req.SetPathValue("itemID", "")
	rec := httptest.NewRecorder()
	handleReconcileItem(db, redisClient)(rec, req)

	if rec.Code != 400 {
		t.Errorf("expected 400 for missing item id, got %d: %s", rec.Code, rec.Body.String())
	}
}
