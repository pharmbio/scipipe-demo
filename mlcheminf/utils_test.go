package main

import (
	"testing"
	"time"
)

func TestFmtDuration(t *testing.T) {
	for duration, expectedDurStr := range map[time.Duration]string{
		3600 * time.Second:                                           "0-01:00:00",
		3601 * time.Second:                                           "0-01:00:01",
		1*24*time.Hour + 2*time.Hour + 3*time.Minute + 4*time.Second: "1-02:03:04",
	} {
		actualDurStr := fmtDuration(duration)
		if actualDurStr != expectedDurStr {
			t.Errorf("Wrong duration string:\nEXPECTED:\n%s\nACTUAL:\n%s\n", expectedDurStr, actualDurStr)
		}
	}
}
