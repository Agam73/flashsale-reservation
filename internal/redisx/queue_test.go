package redisx

import (
	"context"
	"testing"
)

func TestSetAndGetQueueDepth(t *testing.T) {
	ctx := context.Background()
	client := testClient(t)

	if err := SetQueueDepth(ctx, client, "item-1", 7); err != nil {
		t.Fatalf("SetQueueDepth: %v", err)
	}

	n, err := GetQueueDepth(ctx, client, "item-1")
	if err != nil {
		t.Fatalf("GetQueueDepth: %v", err)
	}
	if n != 7 {
		t.Errorf("expected 7, got %d", n)
	}
}

func TestGetQueueDepthDefaultsToZero(t *testing.T) {
	ctx := context.Background()
	client := testClient(t)

	n, err := GetQueueDepth(ctx, client, "never-recorded")
	if err != nil {
		t.Fatalf("GetQueueDepth: %v", err)
	}
	if n != 0 {
		t.Errorf("expected 0 for an item nobody has ever queued for, got %d", n)
	}
}

func TestSetQueueDepthRejectsNegative(t *testing.T) {
	ctx := context.Background()
	client := testClient(t)

	if err := SetQueueDepth(ctx, client, "item-1", -1); err == nil {
		t.Error("expected an error setting a negative queue depth, got nil")
	}
}

func TestSetQueueDepthOverwritesPreviousValue(t *testing.T) {
	ctx := context.Background()
	client := testClient(t)

	if err := SetQueueDepth(ctx, client, "item-1", 10); err != nil {
		t.Fatalf("SetQueueDepth: %v", err)
	}
	if err := SetQueueDepth(ctx, client, "item-1", 2); err != nil {
		t.Fatalf("SetQueueDepth: %v", err)
	}

	n, err := GetQueueDepth(ctx, client, "item-1")
	if err != nil {
		t.Fatalf("GetQueueDepth: %v", err)
	}
	if n != 2 {
		t.Errorf("expected the latest value 2 to win, got %d", n)
	}
}
