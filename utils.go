package main

import (
	"bufio"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/customerio/homework/stream"
)

// isEmpty checks if a given string is empty after trimming spaces.
// It takes a string as input and returns a boolean indicating if it is empty.
func isEmpty(s string) bool {
	return strings.TrimSpace(s) == ""
}

// validate checks if the record contains all required fields.
// It ensures the record is not missing critical information.
// Returns true if the record is valid.
func validate(record *stream.Record) bool {
	return !(isEmpty(record.ID) || isEmpty(record.UserID) || isEmpty(record.Type))
}

// output writes the user data to a specified CSV file in a sorted manner.
// It accepts a map of UserData keyed by userID and a fileName string as parameters.
func output(shards []*UserDataShard, fileName string) error {
	outputFile, err := os.Create(fileName)
	if err != nil {
		return err
	}
	defer outputFile.Close()

	writer := bufio.NewWriter(outputFile)
	defer writer.Flush()

	allUsers := make(map[string]*UserData)
	for _, shard := range shards {
		shard.lock.Lock()
		for userID, data := range shard.m {
			allUsers[userID] = data
		}
		shard.lock.Unlock()
	}

	keys := make([]string, 0, len(allUsers))
	for k := range allUsers {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, userID := range keys {
		user := allUsers[userID]
		var parts []string

		attrKeys := make([]string, 0, len(user.Attributes))
		for k := range user.Attributes {
			attrKeys = append(attrKeys, k)
		}
		sort.Strings(attrKeys)

		for _, k := range attrKeys {
			parts = append(parts, fmt.Sprintf("%s=%s", k, user.Attributes[k].Data))
		}

		eventKeys := make([]string, 0, len(user.Events))
		for k := range user.Events {
			eventKeys = append(eventKeys, k)
		}
		sort.Strings(eventKeys)

		for _, k := range eventKeys {
			parts = append(parts, fmt.Sprintf("%s=%d", k, user.Events[k]))
		}

		line := fmt.Sprintf("%s,%s\n", userID, strings.Join(parts, ","))
		if _, err := writer.WriteString(line); err != nil {
			return err
		}
	}

	return nil
}
