// Package parse turns raw bytes from a source into canonical events.
//
// Parsers are looked up by name so a source config selects one with a single
// line, and a new format is added by registering one function.
package parse

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/JumanaBaharul/streammesh/internal/event"
)

// Parser converts raw source bytes into zero or more canonical events. A parser
// returning no error and no events means "nothing to do", not a failure.
type Parser interface {
	Name() string
	Parse(source string, data []byte) ([]event.Event, error)
}

// BuildFunc constructs a parser from a source's options map.
type BuildFunc func(opts map[string]string) (Parser, error)

var registry = map[string]BuildFunc{}

func register(name string, fn BuildFunc) { registry[name] = fn }

func init() {
	register("json", func(map[string]string) (Parser, error) { return JSON{}, nil })
	register("ndjson", func(map[string]string) (Parser, error) { return JSON{}, nil })
	register("csv", func(opts map[string]string) (Parser, error) { return newCSV(opts) })
	register("syslog", func(map[string]string) (Parser, error) { return Syslog{}, nil })
	register("kv", func(map[string]string) (Parser, error) { return KeyValue{}, nil })
	register("regex", func(opts map[string]string) (Parser, error) { return newRegex(opts) })
	register("raw", func(map[string]string) (Parser, error) { return Raw{}, nil })
}

// Build resolves a parser by name.
func Build(name string, opts map[string]string) (Parser, error) {
	trimmed := strings.ToLower(strings.TrimSpace(name))
	if trimmed == "" {
		trimmed = "json"
	}
	fn, ok := registry[trimmed]
	if !ok {
		return nil, fmt.Errorf("parser: unknown parser %q (available: %s)", name, strings.Join(Names(), ", "))
	}
	return fn(opts)
}

// Names lists the registered parser names.
func Names() []string {
	names := make([]string, 0, len(registry))
	for name := range registry {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// ---------------------------------------------------------------------------
// JSON / NDJSON
// ---------------------------------------------------------------------------

// JSON handles a single object, an array of objects, or newline-delimited
// objects, auto-detected from the payload.
type JSON struct{}

// Name implements Parser.
func (JSON) Name() string { return "json" }

// Parse implements Parser.
func (JSON) Parse(source string, data []byte) ([]event.Event, error) {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 {
		return nil, nil
	}

	if trimmed[0] == '[' {
		var records []map[string]any
		if err := json.Unmarshal(trimmed, &records); err != nil {
			return nil, fmt.Errorf("parser: decode json array: %w", err)
		}
		events := make([]event.Event, 0, len(records))
		for _, record := range records {
			events = append(events, MapToEvent(source, record))
		}
		return events, nil
	}

	// A payload containing a newline is newline-delimited. Detecting that first
	// avoids decoding the whole buffer as a single object and then throwing the
	// result away, which used to double the work for the most common ingest shape.
	var single map[string]any
	if bytes.IndexByte(trimmed, '\n') < 0 {
		if err := json.Unmarshal(trimmed, &single); err != nil {
			return nil, fmt.Errorf("parser: decode json: %w", err)
		}
		return []event.Event{MapToEvent(source, single)}, nil
	}

	// A bad line is reported with its line number, and the good lines are still
	// returned: callers keep the data and count the error, so one malformed record
	// cannot poison a healthy stream.
	var events []event.Event
	var firstErr error
	for i, line := range bytes.Split(trimmed, []byte("\n")) {
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}
		var record map[string]any
		if err := json.Unmarshal(line, &record); err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("parser: line %d: %w", i+1, err)
			}
			continue
		}
		events = append(events, MapToEvent(source, record))
	}
	return events, firstErr
}

// ---------------------------------------------------------------------------
// CSV
// ---------------------------------------------------------------------------

type csvParser struct {
	delimiter rune
	header    bool
	columns   []string
}

func newCSV(opts map[string]string) (Parser, error) {
	p := &csvParser{delimiter: ',', header: true}
	if raw, ok := opts["delimiter"]; ok && raw != "" {
		runes := []rune(raw)
		if len(runes) != 1 {
			return nil, fmt.Errorf("parser: csv delimiter must be one character, got %q", raw)
		}
		p.delimiter = runes[0]
	}
	if raw, ok := opts["header"]; ok && strings.EqualFold(raw, "false") {
		p.header = false
	}
	if raw, ok := opts["columns"]; ok && raw != "" {
		for _, name := range strings.Split(raw, ",") {
			p.columns = append(p.columns, strings.TrimSpace(name))
		}
	}
	if !p.header && len(p.columns) == 0 {
		return nil, fmt.Errorf("parser: csv without a header requires a columns option")
	}
	return p, nil
}

func (p *csvParser) Name() string { return "csv" }

func (p *csvParser) Parse(source string, data []byte) ([]event.Event, error) {
	reader := csv.NewReader(bytes.NewReader(data))
	reader.Comma = p.delimiter
	reader.FieldsPerRecord = -1

	rows, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("parser: read csv: %w", err)
	}
	if len(rows) == 0 {
		return nil, nil
	}

	columns := p.columns
	start := 0
	if p.header {
		columns = rows[0]
		start = 1
	}

	events := make([]event.Event, 0, len(rows)-start)
	for _, row := range rows[start:] {
		record := make(map[string]any, len(columns))
		for i, value := range row {
			if i >= len(columns) {
				break
			}
			record[strings.TrimSpace(columns[i])] = value
		}
		events = append(events, MapToEvent(source, record))
	}
	return events, nil
}

// ---------------------------------------------------------------------------
// key=value
// ---------------------------------------------------------------------------

// KeyValue parses "a=1 b=\"two words\" c='three'" style payloads.
type KeyValue struct{}

var kvPattern = regexp.MustCompile(`([A-Za-z_][A-Za-z0-9_.\-]*)=("([^"]*)"|'([^']*)'|[^\s]+)`)

// Name implements Parser.
func (KeyValue) Name() string { return "kv" }

// Parse implements Parser.
func (KeyValue) Parse(source string, data []byte) ([]event.Event, error) {
	var events []event.Event
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		record := map[string]any{}
		for _, match := range kvPattern.FindAllStringSubmatch(line, -1) {
			value := match[3]
			if value == "" {
				value = match[4]
			}
			if value == "" {
				value = match[2]
			}
			record[match[1]] = value
		}
		if len(record) == 0 {
			continue
		}
		events = append(events, MapToEvent(source, record))
	}
	return events, nil
}

// ---------------------------------------------------------------------------
// regex capture
// ---------------------------------------------------------------------------

type regexParser struct {
	pattern *regexp.Regexp
	names   []string
}

func newRegex(opts map[string]string) (Parser, error) {
	pattern, ok := opts["pattern"]
	if !ok || pattern == "" {
		return nil, fmt.Errorf("parser: regex parser requires a pattern option")
	}
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, fmt.Errorf("parser: compile regex: %w", err)
	}
	return &regexParser{pattern: re, names: re.SubexpNames()}, nil
}

func (p *regexParser) Name() string { return "regex" }

func (p *regexParser) Parse(source string, data []byte) ([]event.Event, error) {
	var events []event.Event
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		match := p.pattern.FindStringSubmatch(line)
		if match == nil {
			continue
		}
		record := map[string]any{}
		for i, name := range p.names {
			if i == 0 || name == "" {
				continue
			}
			record[name] = match[i]
		}
		if len(record) == 0 {
			continue
		}
		events = append(events, MapToEvent(source, record))
	}
	return events, nil
}

// ---------------------------------------------------------------------------
// raw
// ---------------------------------------------------------------------------

// Raw wraps each non-empty line as the message of an event, for sources whose
// payload has no structure at all.
type Raw struct{}

// Name implements Parser.
func (Raw) Name() string { return "raw" }

// Parse implements Parser.
func (Raw) Parse(source string, data []byte) ([]event.Event, error) {
	var events []event.Event
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimRight(line, "\r")
		if strings.TrimSpace(line) == "" {
			continue
		}
		ev := event.New(source)
		ev.Attrs["message"] = line
		events = append(events, ev)
	}
	return events, nil
}

// ---------------------------------------------------------------------------
// normalisation
// ---------------------------------------------------------------------------

// fieldAliases maps the many names producers use onto canonical event fields.
// Order matters: the earliest alias present wins.
var fieldAliases = map[string][]string{
	"timestamp": {"timestamp", "@timestamp", "time", "ts", "event_time", "eventtime", "created_at", "date"},
	"severity":  {"severity", "level", "loglevel", "log_level", "sev", "status", "priority"},
	"host":      {"host", "hostname", "host_name", "source_host", "instance", "node", "machine"},
	"category":  {"category", "type", "event_type", "eventtype", "log_type", "kind", "component"},
	"actor":     {"actor", "user", "username", "user_id", "userid", "principal", "identity"},
	"resource":  {"resource", "path", "url", "uri", "endpoint", "target", "object"},
	"trace":     {"trace_id", "traceid", "trace", "request_id", "requestid", "correlation_id", "span_id"},
	"message":   {"message", "msg", "event", "description", "text"},
}

// canonicalSlots gives every canonical field a fixed array index, so alias
// precedence can be tracked on the stack instead of in a map per event.
var canonicalSlots = map[string]int{
	"timestamp": 0, "severity": 1, "host": 2, "category": 3,
	"actor": 4, "resource": 5, "trace": 6, "message": 7,
}

type aliasInfo struct {
	canonical string
	slot      int
	priority  int
}

// aliasIndex is built once at startup: alias -> canonical field and precedence.
// Resolving aliases with a single map lookup per record key turns normalisation
// into one pass over the payload instead of dozens of probe lookups.
var aliasIndex = buildAliasIndex()

func buildAliasIndex() map[string]aliasInfo {
	index := make(map[string]aliasInfo, 64)
	for canonical, aliases := range fieldAliases {
		slot, ok := canonicalSlots[canonical]
		if !ok {
			continue
		}
		for priority, alias := range aliases {
			if existing, found := index[alias]; found && existing.priority <= priority {
				continue
			}
			index[alias] = aliasInfo{canonical: canonical, slot: slot, priority: priority}
		}
	}
	return index
}

// MapToEvent converts a decoded record into a canonical event. Known fields are
// promoted to typed columns; everything else is preserved under attrs, so no
// producer data is ever silently dropped.
//
// Precedence is deterministic rather than dependent on Go's randomised map
// iteration order: when a record carries several aliases for one canonical field
// ("level" and "severity", say) the earliest listed alias wins.
func MapToEvent(source string, record map[string]any) event.Event {
	ev := event.New(source)

	// priorityBySlot holds the winning precedence per canonical field; the
	// sentinel keeps the first alias seen in the lead.
	best := [8]int{1 << 30, 1 << 30, 1 << 30, 1 << 30, 1 << 30, 1 << 30, 1 << 30, 1 << 30}

	for key, value := range record {
		if value == nil {
			continue
		}
		info, isAlias := aliasIndex[key]
		if !isAlias {
			ev.Attrs[key] = value
			continue
		}
		if best[info.slot] <= info.priority {
			continue
		}
		best[info.slot] = info.priority

		switch info.canonical {
		case "timestamp":
			if ts, ok := ParseTimeValue(value); ok {
				ev.Timestamp = ts
			}
		case "severity":
			if sev, err := event.ParseSeverity(event.ValueString(value)); err == nil {
				ev.Severity = sev
			}
		case "host":
			ev.Host = event.ValueString(value)
		case "category":
			ev.Category = event.ValueString(value)
		case "actor":
			ev.Actor = event.ValueString(value)
		case "resource":
			ev.Resource = event.ValueString(value)
		case "trace":
			ev.TraceID = event.ValueString(value)
		case "message":
			ev.Attrs["message"] = event.ValueString(value)
		}
	}

	if id := record["id"]; id != nil {
		ev.ID = event.ValueString(id)
	}
	ev.Normalize()
	return ev
}

var timeFormats = []string{
	time.RFC3339Nano,
	time.RFC3339,
	"2006-01-02T15:04:05",
	"2006-01-02T15:04:05.000",
	"2006-01-02 15:04:05.000000",
	"2006-01-02 15:04:05.000",
	"2006-01-02 15:04:05",
	"2006-01-02",
	time.RFC1123Z,
	time.RFC1123,
	time.RFC822Z,
	time.ANSIC,
	time.UnixDate,
	time.RubyDate,
}

// ParseTimeValue interprets the many timestamp encodings producers emit,
// including epoch seconds, milliseconds, microseconds and nanoseconds.
func ParseTimeValue(value any) (time.Time, bool) {
	switch typed := value.(type) {
	case time.Time:
		return typed.UTC(), true
	case string:
		trimmed := strings.TrimSpace(typed)
		if trimmed == "" {
			return time.Time{}, false
		}
		for _, layout := range timeFormats {
			if parsed, err := time.Parse(layout, trimmed); err == nil {
				return parsed.UTC(), true
			}
		}
		if number, err := strconv.ParseFloat(trimmed, 64); err == nil {
			return FromEpoch(number)
		}
		return time.Time{}, false
	default:
		if number, ok := event.ValueFloat(value); ok {
			return FromEpoch(number)
		}
		return time.Time{}, false
	}
}

// FromEpoch interprets a numeric timestamp by magnitude.
func FromEpoch(value float64) (time.Time, bool) {
	switch {
	case value <= 0:
		return time.Time{}, false
	case value > 1e17:
		return time.Unix(0, int64(value)).UTC(), true
	case value > 1e14:
		return time.Unix(0, int64(value)*int64(time.Microsecond)).UTC(), true
	case value > 1e11:
		return time.Unix(0, int64(value)*int64(time.Millisecond)).UTC(), true
	default:
		seconds := int64(value)
		nanos := int64((value - float64(seconds)) * float64(time.Second))
		return time.Unix(seconds, nanos).UTC(), true
	}
}
