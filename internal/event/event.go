// Package event defines StreamMesh's canonical event schema.
//
// Every source normalises into an [Event] and every sink serialises one, so the
// routing core only ever reasons about this single shape. Keeping the schema in
// one place is what lets a new connector be added without touching the engine.
package event

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Severity is an ordered severity level. Higher values are more severe, so
// matchers such as "severity >= warning" are plain integer comparisons.
type Severity int

// SeverityUnknown is the zero value, so an event built without a severity is
// recognisable as such and can be defaulted to info instead of silently
// becoming the least severe real level.
const (
	SeverityUnknown Severity = iota
	SeverityDebug
	SeverityInfo
	SeverityNotice
	SeverityWarning
	SeverityError
	SeverityCritical
	SeverityAlert
	SeverityEmergency
)

var severityNames = [...]string{
	"unknown", "debug", "info", "notice", "warning",
	"error", "critical", "alert", "emergency",
}

// severityAliases maps the spellings seen in the wild onto a canonical level.
var severityAliases = map[string]Severity{
	"debug": SeverityDebug, "trace": SeverityDebug, "verbose": SeverityDebug,
	"info": SeverityInfo, "informational": SeverityInfo, "information": SeverityInfo,
	"notice": SeverityNotice,
	"warn":   SeverityWarning, "warning": SeverityWarning,
	"err": SeverityError, "error": SeverityError,
	"crit": SeverityCritical, "critical": SeverityCritical, "fatal": SeverityCritical,
	"alert": SeverityAlert,
	"emerg": SeverityEmergency, "emergency": SeverityEmergency, "panic": SeverityEmergency,
}

func (s Severity) String() string {
	if int(s) < 0 || int(s) >= len(severityNames) {
		return "unknown"
	}
	if s == SeverityUnknown {
		return "unknown"
	}
	return severityNames[s]
}

// MarshalJSON emits the level name so archived events stay readable.
func (s Severity) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.String())
}

// UnmarshalJSON accepts either a level name ("warn") or a number (0-7).
func (s *Severity) UnmarshalJSON(b []byte) error {
	var raw string
	if err := json.Unmarshal(b, &raw); err == nil {
		sev, err2 := ParseSeverity(raw)
		if err2 != nil {
			return err2
		}
		*s = sev
		return nil
	}
	var n int
	if err := json.Unmarshal(b, &n); err != nil {
		return fmt.Errorf("event: severity must be a string or number: %w", err)
	}
	if n < 0 || n > int(SeverityEmergency) {
		return fmt.Errorf("event: severity %d out of range 0-%d", n, int(SeverityEmergency))
	}
	*s = Severity(n)
	return nil
}

// ParseSeverity resolves a textual or numeric severity.
func ParseSeverity(s string) (Severity, error) {
	trimmed := strings.TrimSpace(strings.ToLower(s))
	if trimmed == "" {
		return SeverityInfo, nil
	}
	if sev, ok := severityAliases[trimmed]; ok {
		return sev, nil
	}
	if n, err := strconv.Atoi(trimmed); err == nil {
		if n < 0 || n > int(SeverityEmergency) {
			return SeverityInfo, fmt.Errorf("event: severity %d out of range 0-%d", n, int(SeverityEmergency))
		}
		return Severity(n), nil
	}
	return SeverityInfo, fmt.Errorf("event: unknown severity %q", s)
}

// Event is the canonical record that flows through the pipeline.
type Event struct {
	ID        string         `json:"id"`
	Timestamp time.Time      `json:"timestamp"`
	Source    string         `json:"source_id"`
	Host      string         `json:"host,omitempty"`
	Severity  Severity       `json:"severity"`
	Category  string         `json:"category,omitempty"`
	Actor     string         `json:"actor,omitempty"`
	Resource  string         `json:"resource,omitempty"`
	TraceID   string         `json:"trace_id,omitempty"`
	Attrs     map[string]any `json:"attrs,omitempty"`
	Tags      []string       `json:"tags,omitempty"`

	// ReceivedAt records when StreamMesh accepted the event, which is what
	// latency metrics are measured against.
	ReceivedAt time.Time `json:"received_at,omitempty"`
}

// New builds an event with sensible defaults for the given source.
func New(source string) Event {
	now := time.Now().UTC()
	return Event{
		Timestamp:  now,
		Source:     source,
		Severity:   SeverityInfo,
		ReceivedAt: now,
		Attrs:      map[string]any{},
	}
}

// Normalize fills in defaults and repairs values that would otherwise leak
// into every downstream consumer.
func (e *Event) Normalize() {
	now := time.Now().UTC()
	if e.Timestamp.IsZero() {
		e.Timestamp = now
	}
	e.Timestamp = e.Timestamp.UTC()
	if e.ReceivedAt.IsZero() {
		e.ReceivedAt = now
	}
	e.ReceivedAt = e.ReceivedAt.UTC()
	if e.Severity == SeverityUnknown || int(e.Severity) < 0 || int(e.Severity) > int(SeverityEmergency) {
		e.Severity = SeverityInfo
	}
	e.Source = strings.TrimSpace(e.Source)
	e.Host = strings.TrimSpace(e.Host)
	e.Category = strings.TrimSpace(e.Category)
	e.Actor = strings.TrimSpace(e.Actor)
	e.Resource = strings.TrimSpace(e.Resource)
	e.TraceID = strings.TrimSpace(e.TraceID)
	if e.Attrs == nil {
		e.Attrs = map[string]any{}
	}
}

// Digest derives a deterministic content hash for the event. Identical payloads
// always produce the same ID, which is what makes retry-side dedup possible.
func (e Event) Digest() string {
	clone := e
	clone.ID = ""
	clone.ReceivedAt = time.Time{}
	if clone.Timestamp.IsZero() {
		clone.Timestamp = time.Time{}
	}
	raw, err := json.Marshal(clone)
	if err != nil {
		// Marshalling the canonical shape cannot fail in practice; fall back to
		// the human-readable form rather than panicking in a data path.
		raw = []byte(fmt.Sprintf("%s|%s|%s|%s", clone.Source, clone.Host, clone.Category, clone.Resource))
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:16])
}

// EnsureID assigns a content-derived ID when the source did not provide one.
func (e *Event) EnsureID() {
	if strings.TrimSpace(e.ID) == "" {
		e.ID = e.Digest()
	}
}

// Clone deep-copies the event. Fan-out hands every sink its own copy so one
// sink's mutation can never be observed by another.
func (e Event) Clone() Event {
	clone := e
	if e.Attrs != nil {
		clone.Attrs = deepCopyMap(e.Attrs)
	}
	if e.Tags != nil {
		clone.Tags = append([]string(nil), e.Tags...)
	}
	return clone
}

func deepCopyMap(in map[string]any) map[string]any {
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = deepCopyValue(v)
	}
	return out
}

func deepCopyValue(v any) any {
	switch typed := v.(type) {
	case map[string]any:
		return deepCopyMap(typed)
	case []any:
		out := make([]any, len(typed))
		for i, item := range typed {
			out[i] = deepCopyValue(item)
		}
		return out
	default:
		return v
	}
}

// Field reads a value by dotted path, for example "severity", "host" or
// "attrs.http.status"; numeric path segments index arrays.
func (e *Event) Field(path string) (any, bool) {
	switch path {
	case "id":
		return nonEmpty(e.ID)
	case "timestamp", "time":
		if e.Timestamp.IsZero() {
			return nil, false
		}
		return e.Timestamp, true
	case "source", "source_id":
		return nonEmpty(e.Source)
	case "host", "hostname":
		return nonEmpty(e.Host)
	case "severity", "level":
		return e.Severity, true
	case "category":
		return nonEmpty(e.Category)
	case "actor", "user":
		return nonEmpty(e.Actor)
	case "resource":
		return nonEmpty(e.Resource)
	case "trace_id", "trace":
		return nonEmpty(e.TraceID)
	case "tags":
		if len(e.Tags) == 0 {
			return nil, false
		}
		values := make([]any, len(e.Tags))
		for i, tag := range e.Tags {
			values[i] = tag
		}
		return values, true
	case "attrs":
		return e.Attrs, true
	}

	rest, ok := strings.CutPrefix(path, "attrs.")
	if !ok {
		return nil, false
	}
	var current any = e.Attrs
	for _, segment := range strings.Split(rest, ".") {
		switch node := current.(type) {
		case map[string]any:
			value, found := node[segment]
			if !found {
				return nil, false
			}
			current = value
		case []any:
			index, err := strconv.Atoi(segment)
			if err != nil || index < 0 || index >= len(node) {
				return nil, false
			}
			current = node[index]
		default:
			return nil, false
		}
	}
	return current, current != nil
}

// SetField writes a value by dotted path, creating nested maps as needed.
func (e *Event) SetField(path string, value any) bool {
	switch path {
	case "id":
		e.ID = ValueString(value)
		return true
	case "host", "hostname":
		e.Host = ValueString(value)
		return true
	case "category":
		e.Category = ValueString(value)
		return true
	case "actor", "user":
		e.Actor = ValueString(value)
		return true
	case "resource":
		e.Resource = ValueString(value)
		return true
	case "trace_id", "trace":
		e.TraceID = ValueString(value)
		return true
	case "source", "source_id":
		e.Source = ValueString(value)
		return true
	case "severity", "level":
		if sev, err := ParseSeverity(ValueString(value)); err == nil {
			e.Severity = sev
			return true
		}
		return false
	}

	rest, ok := strings.CutPrefix(path, "attrs.")
	if !ok || rest == "" {
		return false
	}
	if e.Attrs == nil {
		e.Attrs = map[string]any{}
	}
	segments := strings.Split(rest, ".")
	node := e.Attrs
	for _, segment := range segments[:len(segments)-1] {
		child, found := node[segment]
		if !found {
			next := map[string]any{}
			node[segment] = next
			node = next
			continue
		}
		next, ok := child.(map[string]any)
		if !ok {
			next = map[string]any{}
			node[segment] = next
		}
		node = next
	}
	node[segments[len(segments)-1]] = value
	return true
}

func nonEmpty(s string) (any, bool) {
	if s == "" {
		return nil, false
	}
	return s, true
}

// ValueString renders any decoded JSON value for text comparisons and output.
func ValueString(v any) string {
	switch typed := v.(type) {
	case nil:
		return ""
	case string:
		return typed
	case bool:
		return strconv.FormatBool(typed)
	case float64:
		if typed == float64(int64(typed)) {
			return strconv.FormatInt(int64(typed), 10)
		}
		return strconv.FormatFloat(typed, 'f', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(typed), 'f', -1, 32)
	case int:
		return strconv.Itoa(typed)
	case int64:
		return strconv.FormatInt(typed, 10)
	case json.Number:
		return typed.String()
	case time.Time:
		return typed.UTC().Format(time.RFC3339Nano)
	default:
		return fmt.Sprintf("%v", typed)
	}
}

// ValueFloat attempts to interpret a value numerically.
func ValueFloat(v any) (float64, bool) {
	switch typed := v.(type) {
	case float64:
		return typed, true
	case float32:
		return float64(typed), true
	case int:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case json.Number:
		f, err := typed.Float64()
		return f, err == nil
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(typed), 64)
		return f, err == nil
	default:
		return 0, false
	}
}
