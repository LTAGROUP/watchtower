package logging

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	reset   = "\x1b[0m"
	bold    = "\x1b[1m"
	italic  = "\x1b[3m"
	dim     = "\x1b[2m"
	red     = "\x1b[31m"
	green   = "\x1b[32m"
	yellow  = "\x1b[33m"
	blue    = "\x1b[34m"
	magenta = "\x1b[35m"
	cyan    = "\x1b[36m"
)

type ConsoleHandler struct {
	w      io.Writer
	mu     *sync.Mutex
	attrs  []slog.Attr
	groups []string
	color  bool
	level  slog.Leveler
}

func NewConsoleHandler(w io.Writer, color bool) *ConsoleHandler {
	return &ConsoleHandler{w: w, mu: &sync.Mutex{}, color: color, level: slog.LevelInfo}
}

func (h *ConsoleHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.level.Level()
}

func (h *ConsoleHandler) Handle(_ context.Context, record slog.Record) error {
	attrs := append([]slog.Attr(nil), h.attrs...)
	record.Attrs(func(attr slog.Attr) bool {
		attrs = append(attrs, attr)
		return true
	})

	component := "core"
	fields := make([]string, 0, len(attrs))
	for _, attr := range attrs {
		h.appendAttr(&fields, "", attr, &component)
	}

	level := strings.ToLower(record.Level.String())
	timestamp := record.Time.Local().Format("2006-01-02 15:04:05")
	var line strings.Builder
	line.WriteString(h.style(dim, timestamp))
	line.WriteString(" - ")
	line.WriteString(h.style(blue+bold, component))
	line.WriteString(" - ")
	line.WriteString(h.style(levelStyle(record.Level)+bold, level))
	line.WriteString(": ")
	line.WriteString(h.style(bold, clean(record.Message)))
	if len(fields) > 0 {
		line.WriteByte(' ')
		line.WriteString(h.style(dim+italic, strings.Join(fields, " ")))
	}
	line.WriteByte('\n')

	h.mu.Lock()
	defer h.mu.Unlock()
	_, err := io.WriteString(h.w, line.String())
	return err
}

func (h *ConsoleHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	clone := h.clone()
	clone.attrs = append(clone.attrs, attrs...)
	return clone
}

func (h *ConsoleHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	clone := h.clone()
	clone.groups = append(clone.groups, name)
	return clone
}

func (h *ConsoleHandler) clone() *ConsoleHandler {
	clone := *h
	clone.attrs = append([]slog.Attr(nil), h.attrs...)
	clone.groups = append([]string(nil), h.groups...)
	return &clone
}

func (h *ConsoleHandler) appendAttr(fields *[]string, prefix string, attr slog.Attr, component *string) {
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
	*fields = append(*fields, key+"="+formatValue(attr.Value))
}

func (h *ConsoleHandler) style(code, value string) string {
	if !h.color {
		return value
	}
	return code + value + reset
}

func levelStyle(level slog.Level) string {
	switch {
	case level >= slog.LevelError:
		return red
	case level >= slog.LevelWarn:
		return yellow
	case level >= slog.LevelInfo:
		return green
	default:
		return cyan
	}
}

func formatValue(value slog.Value) string {
	var raw string
	switch value.Kind() {
	case slog.KindString:
		raw = value.String()
	case slog.KindTime:
		raw = value.Time().Format(time.RFC3339)
	case slog.KindDuration:
		raw = value.Duration().String()
	case slog.KindInt64:
		raw = strconv.FormatInt(value.Int64(), 10)
	case slog.KindUint64:
		raw = strconv.FormatUint(value.Uint64(), 10)
	case slog.KindFloat64:
		raw = strconv.FormatFloat(value.Float64(), 'g', -1, 64)
	case slog.KindBool:
		raw = strconv.FormatBool(value.Bool())
	default:
		raw = fmt.Sprint(value.Any())
	}
	raw = clean(raw)
	if raw == "" || strings.ContainsAny(raw, " \t=\"") {
		return strconv.Quote(raw)
	}
	return raw
}

func clean(value string) string {
	return strings.NewReplacer("\n", "\\n", "\r", "\\r", "\x1b", "").Replace(value)
}
