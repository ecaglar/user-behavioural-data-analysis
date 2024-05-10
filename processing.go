package main

import (
	"fmt"

	"github.com/customerio/homework/stream"
)

// processRecord processes a single record from the data stream.
// It checks for record validity, locks for concurrency safety, and processes based on record type.
// processRecord updates or creates user data in a shard-specific map based on the incoming record.

func processRecord(users map[string]*UserData, record *stream.Record) error {
	if record == nil {
		return fmt.Errorf("record is nil")
	}

	if !validate(record) {
		return fmt.Errorf("invalid record: %v", record)
	}

	user, exists := users[record.UserID]
	if !exists {
		user = &UserData{
			Attributes:        make(map[string]AttributeDataWithTimestamp),
			Events:            make(map[string]int),
			ProcessedEventIDs: make(map[string]bool),
		}
		users[record.UserID] = user
	}

	switch record.Type {
	case EVENT:
		incrementEvent(user, record.Name, record.ID)
	case ATTRIBUTES:
		processAttribute(user, record)
	}

	return nil
}

// processAttribute updates or adds new attributes to the user's attribute map.
// It only updates an attribute if the new attribute's timestamp is more recent.

func processAttribute(user *UserData, record *stream.Record) {
	for key, value := range record.Data {
		if attr, exists := user.Attributes[key]; exists && record.Timestamp > attr.Timestamp {
			user.Attributes[key] = AttributeDataWithTimestamp{
				Data:      value,
				Timestamp: record.Timestamp,
			}
		} else if !exists {
			user.Attributes[key] = AttributeDataWithTimestamp{
				Data:      value,
				Timestamp: record.Timestamp,
			}
		}
	}
}

// incrementEvent increments the event count for a user and marks the event ID as processed.
// It ensures that each event is only counted once.

func incrementEvent(user *UserData, eventName string, eventID string) {
	if _, ok := user.ProcessedEventIDs[eventID]; !ok {
		user.Events[eventName]++
		user.ProcessedEventIDs[eventID] = true
	}
}
