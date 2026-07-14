package logging

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"
)

func TestConsoleHandlerFormatsReadableLine(t *testing.T) {
	var out bytes.Buffer
	handler := NewConsoleHandler(&out, false)
	record := slog.NewRecord(time.Date(2026, 7, 13, 2, 35, 0, 0, time.Local), slog.LevelInfo, "stream link obtained", 0)
	record.Add("component", "stream", "file", "Movies/Blade Runner 2049.mkv", "provider", "torbox", "valid_for", "45m0s")
	if err := handler.Handle(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	want := `2026-07-13 02:35:00 - stream - info: stream link obtained file="Movies/Blade Runner 2049.mkv" provider=torbox valid_for=45m0s` + "\n"
	if out.String() != want {
		t.Fatalf("unexpected log line:\n%s\nwant:\n%s", out.String(), want)
	}
}

func TestConsoleHandlerAddsAndSanitizesANSI(t *testing.T) {
	var out bytes.Buffer
	logger := slog.New(NewConsoleHandler(&out, true))
	logger.Warn("refresh failed\x1b[31m", "component", "stream", "error", "provider\nfailed")
	got := out.String()
	if !strings.Contains(got, yellow+bold+"warn"+reset) || !strings.Contains(got, bold+"refresh failed[31m"+reset) {
		t.Fatalf("expected styled level and message: %q", got)
	}
	if strings.Contains(got, "provider\nfailed") {
		t.Fatalf("expected newlines to be escaped: %q", got)
	}
}

func TestBufferRetainsStructuredRecentEntries(t *testing.T) {
	buffer := NewBuffer(2)
	logger := slog.New(buffer.Handler(slog.LevelDebug)).With("component", "resolver")
	logger.Debug("first", "title", "One")
	logger.Info("second", "title", "Two")
	logger.Warn("third", "error", "not ready")

	entries := buffer.Entries()
	if len(entries) != 2 {
		t.Fatalf("expected two retained entries, got %d", len(entries))
	}
	if entries[0].Message != "second" || entries[1].Message != "third" {
		t.Fatalf("unexpected retained entries: %#v", entries)
	}
	if entries[1].Level != "warn" || entries[1].Component != "resolver" || entries[1].Fields["error"] != `"not ready"` {
		t.Fatalf("unexpected structured entry: %#v", entries[1])
	}
	if entries[0].ID >= entries[1].ID {
		t.Fatalf("entry IDs are not increasing: %#v", entries)
	}
}

func TestTeeHandlerHonorsEachHandlerLevel(t *testing.T) {
	var console bytes.Buffer
	buffer := NewBuffer(10)
	logger := slog.New(NewTeeHandler(NewConsoleHandler(&console, false), buffer.Handler(slog.LevelDebug)))
	logger.Debug("dashboard-only")
	logger.Info("everywhere")

	if strings.Contains(console.String(), "dashboard-only") || !strings.Contains(console.String(), "everywhere") {
		t.Fatalf("unexpected console output: %q", console.String())
	}
	if entries := buffer.Entries(); len(entries) != 2 {
		t.Fatalf("expected both records in dashboard buffer, got %#v", entries)
	}
}
