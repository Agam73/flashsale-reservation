package kafkax

import "time"

// WaitingRoomJoinsTopic holds every BuyerJoined event -- one per buyer
// actually admitted into an item's waiting room. Phase 10's
// risk-service consumes this topic to score bot/scalper risk
// asynchronously, before a buyer ever reaches checkout-api.
//
// Like CheckoutAttemptsTopic, this needs an explicit partition count
// before either service runs (see scripts/setup_kafka_topic.sh) so it
// doesn't get auto-created with the broker's default.
const WaitingRoomJoinsTopic = "waiting-room-joins"

// BuyerJoined is what waiting-room-api publishes once a buyer has
// actually been admitted off the queue -- not when they first request
// to join. Position/order only matters once, at admission; publishing
// at request time would mean scoring buyers who never made it off the
// queue at all, which risk-service has no use for.
//
// RemoteAddr is the client's IP as waiting-room-api itself observed it
// (from the HTTP connection, not anything in the request body a
// client could spoof) -- a cheap, widely-used signal for spotting many
// distinct identities being driven from one machine, at the cost of
// being unreliable behind NAT, shared corporate proxies, or carrier-
// grade NAT on mobile networks. risk-service is expected to treat it
// as one weak signal among others, never as a standalone verdict.
type BuyerJoined struct {
	ItemID     string    `json:"item_id"`
	UserID     string    `json:"user_id"`
	RemoteAddr string    `json:"remote_addr"`
	JoinedAt   time.Time `json:"joined_at"`
}
