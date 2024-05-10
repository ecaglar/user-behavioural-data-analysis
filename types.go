package main

// Record represents a single record in the data stream.
type Record struct {
	ID        string            `json:"id"`
	Type      string            `json:"type"`
	Name      string            `json:"name,omitempty"`
	UserID    string            `json:"user_id"`
	Data      map[string]string `json:"data"`
	Timestamp int64             `json:"timestamp"`
}

// UserData stores the state of a user including attributes and events.
// ProcessedEventIDs used to store event ids to prevent counting them multiple times
type UserData struct {
	Attributes        map[string]AttributeDataWithTimestamp
	Events            map[string]int
	ProcessedEventIDs map[string]bool
}

// AttributeDataWithTimestamp wraps an attribute's data with its update timestamp.
// Timestamp is needed because more recent attributes should override older ones
type AttributeDataWithTimestamp struct {
	Data      string
	Timestamp int64
}
