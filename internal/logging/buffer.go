package logging

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"time"
)

// Entry is the structured, JSON-safe representation of a log record shown by
// the dashboard.
type Entry struct {
	ID        uint64            `json:"id"`
	Timestamp time.Time         `json:"timestamp"`
	Level     string            `json:"level"`
	Component string            `json:"component"`
	Message   string            `json:"message"`
	Fields    map[string]string `json:"fields,omitempty"`
}

// Buffer retains a bounded number of recent log entries in memory. It is safe
// for concurrent use by slog handlers and dashboard requests.
type Buffer struct {
	mu      sync.RWMutex
	entries []Entry
	nextID  uint64
	limit   int
}

func NewBuffer(limit int) *Buffer {
	if limit < 1 {
		limit = 1000
	}
	return &Buffer{limit: limit, entries: make([]Entry, 0, limit)}
}

// Handler returns an slog handler that writes records at or above level into
// the buffer.
func (b *Buffer) Handler(level slog.Leveler) slog.Handler {
	if level == nil {
		level = slog.LevelInfo
	}
	return &bufferHandler{buffer: b, level: level}
}

// Entries returns a snapshot ordered from oldest to newest.
func (b *Buffer) Entries() []Entry {
	b.mu.RLock()
	defer b.mu.RUnlock()
	entries := make([]Entry, len(b.entries))
	copy(entries, b.entries)
	return entries
}

func (b *Buffer) Capacity() int {
	return b.limit
}

type bufferHandler struct {
	buffer *Buffer
	attrs  []slog.Attr
	groups []string
	level  slog.Leveler
}

func (h *bufferHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.level.Level()
}

func (h *bufferHandler) Handle(_ context.Context, record slog.Record) error {
	attrs := append([]slog.Attr(nil), h.attrs...)
	record.Attrs(func(attr slog.Attr) bool {
		attrs = append(attrs, attr)
		return true
	})
	entry := Entry{
		Timestamp: record.Time.UTC(),
		Level:     strings.ToLower(record.Level.String()),
		Component: "core",
		Message:   clean(record.Message),
		Fields:    map[string]string{},
	}
	if entry.Timestamp.IsZero() {
		entry.Timestamp = time.Now().UTC()
	}
	for _, attr := range attrs {
		h.appendAttr(entry.Fields, "", attr, &entry.Component)
	}
	if len(entry.Fields) == 0 {
		entry.Fields = nil
	}

	h.buffer.mu.Lock()
	defer h.buffer.mu.Unlock()
	h.buffer.nextID++
	entry.ID = h.buffer.nextID
	if len(h.buffer.entries) == h.buffer.limit {
		copy(h.buffer.entries, h.buffer.entries[1:])
		h.buffer.entries[len(h.buffer.entries)-1] = entry
	} else {
		h.buffer.entries = append(h.buffer.entries, entry)
	}
	return nil
}

func (h *bufferHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	clone := h.clone()
	clone.attrs = append(clone.attrs, attrs...)
	return clone
}

func (h *bufferHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	clone := h.clone()
	clone.groups = append(clone.groups, name)
	return clone
}

func (h *bufferHandler) clone() *bufferHandler {
	clone := *h
	clone.attrs = append([]slog.Attr(nil), h.attrs...)
	clone.groups = append([]string(nil), h.groups...)
	return &clone
}

func (h *bufferHandler) appendAttr(fields map[string]string, prefix string, attr slog.Attr, component *string) {
	attr.Value = attr.Value.Resolve()
	if attr.Equal(slog.Attr{}) {
		return
	}
	key := strings.Join(append(append([]string(nil), h.groups...), prefix+attr.Key), ".")
	if attr.Value.Kind() == slog.KindGroup {
		groupPrefix := attr.Key + "."
		if prefix != "" {
			groupPrefix = prefix + groupPrefix
		}
		for _, child := range attr.Value.Group() {
			h.appendAttr(fields, groupPrefix, child, component)
		}
		return
	}
	if key == "component" {
		*component = clean(attr.Value.String())
		return
	}
	fields[key] = clean(formatValue(attr.Value))
}

// TeeHandler sends each record to every enabled handler.
type TeeHandler struct {
	handlers []slog.Handler
}

func NewTeeHandler(handlers ...slog.Handler) *TeeHandler {
	return &TeeHandler{handlers: append([]slog.Handler(nil), handlers...)}
}

func (h *TeeHandler) Enabled(ctx context.Context, level slog.Level) bool {
	for _, handler := range h.handlers {
		if handler.Enabled(ctx, level) {
			return true
		}
	}
	return false
}

func (h *TeeHandler) Handle(ctx context.Context, record slog.Record) error {
	for _, handler := range h.handlers {
		if handler.Enabled(ctx, record.Level) {
			if err := handler.Handle(ctx, record.Clone()); err != nil {
				return err
			}
		}
	}
	return nil
}

func (h *TeeHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	handlers := make([]slog.Handler, 0, len(h.handlers))
	for _, handler := range h.handlers {
		handlers = append(handlers, handler.WithAttrs(attrs))
	}
	return NewTeeHandler(handlers...)
}

func (h *TeeHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	handlers := make([]slog.Handler, 0, len(h.handlers))
	for _, handler := range h.handlers {
		handlers = append(handlers, handler.WithGroup(name))
	}
	return NewTeeHandler(handlers...)
}
