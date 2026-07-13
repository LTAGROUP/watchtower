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
