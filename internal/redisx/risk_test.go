package redisx

import (
	"context"
	"testing"
	"time"
)

func TestSetAndGetRiskScore(t *testing.T) {
	ctx := context.Background()
	client := testClient(t)

	if err := SetRiskScore(ctx, client, "item-1", "user-alice", 0.73, time.Minute); err != nil {
		t.Fatalf("SetRiskScore: %v", err)
	}

	score, found, err := GetRiskScore(ctx, client, "item-1", "user-alice")
	if err != nil {
		t.Fatalf("GetRiskScore: %v", err)
	}
	if !found {
		t.Fatal("expected a cached score to be found")
	}
	if score != 0.73 {
		t.Errorf("expected 0.73, got %v", score)
	}
}

func TestGetRiskScoreNotFound(t *testing.T) {
	ctx := context.Background()
	client := testClient(t)

	_, found, err := GetRiskScore(ctx, client, "item-1", "user-never-scored")
	if err != nil {
		t.Fatalf("GetRiskScore: %v", err)
	}
	if found {
		t.Error("expected found=false for a buyer nobody has scored yet")
	}
}

func TestRiskScoreExpires(t *testing.T) {
	ctx := context.Background()
	client := testClient(t)

	if err := SetRiskScore(ctx, client, "item-1", "user-alice", 0.9, 50*time.Millisecond); err != nil {
		t.Fatalf("SetRiskScore: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	_, found, err := GetRiskScore(ctx, client, "item-1", "user-alice")
	if err != nil {
		t.Fatalf("GetRiskScore: %v", err)
	}
	if found {
		t.Error("expected the cached score to have expired")
	}
}

func TestSetRiskScoreRejectsNonPositiveTTL(t *testing.T) {
	ctx := context.Background()
	client := testClient(t)

	for _, ttl := range []time.Duration{0, -time.Second} {
		if err := SetRiskScore(ctx, client, "item-1", "user-alice", 0.5, ttl); err == nil {
			t.Errorf("expected an error for ttl=%s, got nil", ttl)
		}
	}
}

func TestRiskScoreIsPerItemAndUser(t *testing.T) {
	ctx := context.Background()
	client := testClient(t)

	if err := SetRiskScore(ctx, client, "item-1", "user-alice", 0.2, time.Minute); err != nil {
		t.Fatalf("SetRiskScore: %v", err)
	}
	if err := SetRiskScore(ctx, client, "item-1", "user-bob", 0.8, time.Minute); err != nil {
		t.Fatalf("SetRiskScore: %v", err)
	}
	if err := SetRiskScore(ctx, client, "item-2", "user-alice", 0.4, time.Minute); err != nil {
		t.Fatalf("SetRiskScore: %v", err)
	}

	aliceOnItem1, _, err := GetRiskScore(ctx, client, "item-1", "user-alice")
	if err != nil {
		t.Fatalf("GetRiskScore: %v", err)
	}
	if aliceOnItem1 != 0.2 {
		t.Errorf("expected alice's item-1 score 0.2, got %v", aliceOnItem1)
	}

	aliceOnItem2, _, err := GetRiskScore(ctx, client, "item-2", "user-alice")
	if err != nil {
		t.Fatalf("GetRiskScore: %v", err)
	}
	if aliceOnItem2 != 0.4 {
		t.Errorf("expected alice's item-2 score 0.4, got %v", aliceOnItem2)
	}
}
