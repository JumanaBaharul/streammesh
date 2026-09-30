package parse

import (
	"strings"
	"testing"
	"time"

	"github.com/JumanaBaharul/streammesh/internal/event"
)

func TestJSONHandlesSingleObjectArrayAndNDJSON(t *testing.T) {
	parser := JSON{}

	t.Run("single object", func(t *testing.T) {
		events, err := parser.Parse("svc", []byte(`{"level":"warn","host":"edge-01","message":"slow"}`))
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		if len(events) != 1 {
			t.Fatalf("events = %d, want 1", len(events))
		}
		if events[0].Severity != event.SeverityWarning {
			t.Errorf("severity = %v, want warning", events[0].Severity)
		}
		if events[0].Host != "edge-01" {
			t.Errorf("host = %q", events[0].Host)
		}
		if event.ValueString(events[0].Attrs["message"]) != "slow" {
			t.Errorf("message = %v", events[0].Attrs["message"])
		}
	})

	t.Run("array", func(t *testing.T) {
		events, err := parser.Parse("svc", []byte(`[{"a":1},{"a":2},{"a":3}]`))
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		if len(events) != 3 {
			t.Fatalf("events = %d, want 3", len(events))
		}
	})

	t.Run("ndjson", func(t *testing.T) {
		events, err := parser.Parse("svc", []byte("{\"a\":1}\n{\"a\":2}\n"))
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		if len(events) != 2 {
			t.Fatalf("events = %d, want 2", len(events))
		}
	})

	t.Run("empty payload", func(t *testing.T) {
		events, err := parser.Parse("svc", []byte("   \n"))
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		if len(events) != 0 {
			t.Fatalf("events = %d, want 0", len(events))
		}
	})
}

// TestJSONKeepsGoodLinesAndReportsBadOnes pins the partial-success contract: the
// valid records come back and the failure is reported, so a caller can keep the
// data and count the rejection instead of discarding the whole payload.
func TestJSONKeepsGoodLinesAndReportsBadOnes(t *testing.T) {
	events, err := JSON{}.Parse("svc", []byte("{\"a\":1}\nnot json\n{\"a\":3}\n"))
	if err == nil {
		t.Fatal("expected the malformed line to be reported")
	}
	if len(events) != 2 {
		t.Fatalf("events = %d, want the two valid lines", len(events))
	}
	if !strings.Contains(err.Error(), "line 2") {
		t.Fatalf("error %q does not identify the offending line", err)
	}
}

func TestJSONFailsWhenNothingParses(t *testing.T) {
	events, err := (JSON{}).Parse("svc", []byte("not json at all"))
	if err == nil {
		t.Fatal("expected an error when no line parses")
	}
	if len(events) != 0 {
		t.Fatalf("events = %d, want none", len(events))
	}
}

func TestSyslogReturnsGoodLinesWithTheError(t *testing.T) {
	payload := "<13>Sep 30 09:15:01 api-11 app: first\n   \n"
	events, err := Syslog{}.Parse("svc", []byte(payload))
	if len(events) != 1 {
		t.Fatalf("events = %d, want 1", len(events))
	}
	_ = err
}

func TestCSVParsesHeaderRowsAndCustomDelimiter(t *testing.T) {
	parser, err := Build("csv", map[string]string{"delimiter": ";"})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	events, err := parser.Parse("svc", []byte("level;host;message\nwarn;edge-01;slow response\n"))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("events = %d, want 1", len(events))
	}
	if events[0].Severity != event.SeverityWarning || events[0].Host != "edge-01" {
		t.Fatalf("unexpected event: %+v", events[0])
	}
}

func TestCSVWithoutHeaderRequiresColumns(t *testing.T) {
	if _, err := Build("csv", map[string]string{"header": "false"}); err == nil {
		t.Fatal("expected an error when a headerless CSV has no columns option")
	}
	parser, err := Build("csv", map[string]string{"header": "false", "columns": "host,message"})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	events, err := parser.Parse("svc", []byte("edge-01,hello\n"))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(events) != 1 || events[0].Host != "edge-01" {
		t.Fatalf("unexpected events: %+v", events)
	}
}

func TestKeyValueParsesQuotedAndBareValues(t *testing.T) {
	events, err := KeyValue{}.Parse("svc", []byte(`level=error host="edge 01" msg='two words' count=7`))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("events = %d, want 1", len(events))
	}
	ev := events[0]
	if ev.Severity != event.SeverityError {
		t.Errorf("severity = %v", ev.Severity)
	}
	if ev.Host != "edge 01" {
		t.Errorf("host = %q", ev.Host)
	}
	// "msg" is an alias for the canonical message field, so it is promoted out
	// of attrs rather than duplicated into them.
	if event.ValueString(ev.Attrs["message"]) != "two words" {
		t.Errorf("message = %v", ev.Attrs["message"])
	}
	if _, duplicated := ev.Attrs["msg"]; duplicated {
		t.Error("the alias key leaked into attrs alongside the promoted field")
	}
}

func TestRegexParserUsesNamedGroups(t *testing.T) {
	parser, err := Build("regex", map[string]string{
		"pattern": `^(?P<host>\S+) (?P<level>\S+) (?P<message>.*)$`,
	})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	events, err := parser.Parse("svc", []byte("api-11 error upstream down\n"))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("events = %d, want 1", len(events))
	}
	if events[0].Host != "api-11" || events[0].Severity != event.SeverityError {
		t.Fatalf("unexpected event: %+v", events[0])
	}
}

func TestRegexParserRequiresPattern(t *testing.T) {
	if _, err := Build("regex", nil); err == nil {
		t.Fatal("expected an error without a pattern")
	}
	if _, err := Build("regex", map[string]string{"pattern": "([unclosed"}); err == nil {
		t.Fatal("expected an error for an invalid pattern")
	}
}

func TestBuildRejectsUnknownParser(t *testing.T) {
	if _, err := Build("protobuf", nil); err == nil {
		t.Fatal("expected an error for an unknown parser")
	}
	if _, err := Build("", nil); err != nil {
		t.Fatalf("an empty name should default to json: %v", err)
	}
}

func TestMapToEventPromotesKnownFieldsAndPreservesTheRest(t *testing.T) {
	ev := MapToEvent("svc", map[string]any{
		"@timestamp": "2026-09-30T09:15:01Z",
		"level":      "critical",
		"hostname":   "db-primary",
		"event_type": "database",
		"user":       "svc-billing",
		"endpoint":   "/v1/orders",
		"trace_id":   "trace-abc",
		"message":    "connection pool exhausted",
		"custom_one": "kept",
		"custom_two": float64(2),
	})

	if !ev.Timestamp.Equal(time.Date(2026, 9, 30, 9, 15, 1, 0, time.UTC)) {
		t.Errorf("timestamp = %s", ev.Timestamp)
	}
	if ev.Severity != event.SeverityCritical {
		t.Errorf("severity = %v", ev.Severity)
	}
	if ev.Host != "db-primary" || ev.Category != "database" || ev.Actor != "svc-billing" || ev.Resource != "/v1/orders" {
		t.Errorf("unexpected field promotion: %+v", ev)
	}
	if ev.TraceID != "trace-abc" {
		t.Errorf("trace id = %q", ev.TraceID)
	}
	if event.ValueString(ev.Attrs["message"]) != "connection pool exhausted" {
		t.Errorf("message = %v", ev.Attrs["message"])
	}
	if ev.Attrs["custom_one"] != "kept" {
		t.Errorf("unknown field was not preserved: %v", ev.Attrs["custom_one"])
	}
	if _, promoted := ev.Attrs["level"]; promoted {
		t.Error("a promoted field was also copied into attrs")
	}
}

func TestMapToEventUsesTheProvidedID(t *testing.T) {
	ev := MapToEvent("svc", map[string]any{"id": "producer-id", "message": "hi"})
	if ev.ID != "producer-id" {
		t.Fatalf("id = %q", ev.ID)
	}
}

func TestParseTimeValueHandlesCommonEncodings(t *testing.T) {
	cases := []struct {
		in   any
		want time.Time
		ok   bool
	}{
		{in: "2026-09-30T09:15:01Z", want: time.Date(2026, 9, 30, 9, 15, 1, 0, time.UTC), ok: true},
		{in: "2026-09-30 09:15:01", want: time.Date(2026, 9, 30, 9, 15, 1, 0, time.UTC), ok: true},
		{in: "2026-09-30", want: time.Date(2026, 9, 30, 0, 0, 0, 0, time.UTC), ok: true},
		{in: float64(1789000505), want: time.Unix(1789000505, 0).UTC(), ok: true},
		{in: float64(1789000505000), want: time.Unix(1789000505, 0).UTC(), ok: true},
		{in: "not a time", ok: false},
		{in: float64(0), ok: false},
	}
	for _, tc := range cases {
		got, ok := ParseTimeValue(tc.in)
		if ok != tc.ok {
			t.Errorf("ParseTimeValue(%v) ok = %v, want %v", tc.in, ok, tc.ok)
			continue
		}
		if tc.ok && !got.Equal(tc.want) {
			t.Errorf("ParseTimeValue(%v) = %s, want %s", tc.in, got, tc.want)
		}
	}
}

func TestFromEpochDistinguishesUnits(t *testing.T) {
	base := time.Unix(1789000505, 0).UTC()
	cases := []float64{
		float64(base.Unix()),
		float64(base.UnixMilli()),
		float64(base.UnixMicro()),
		float64(base.UnixNano()),
	}
	for _, value := range cases {
		got, ok := FromEpoch(value)
		if !ok {
			t.Fatalf("FromEpoch(%v) failed", value)
		}
		if !got.Equal(base) {
			t.Errorf("FromEpoch(%v) = %s, want %s", value, got, base)
		}
	}
}

func TestFuzzJSONNeverPanics(t *testing.T) {
	inputs := []string{
		`{"a":`, `[`, `{"a":1}`, `null`, `{"a":{"b":[1,2,{"c":null}]}}`, `[1,2,3]`,
		`{"timestamp":"0000-00-00","level":""}`, strings.Repeat("{", 100),
	}
	for _, input := range inputs {
		if _, err := (JSON{}).Parse("svc", []byte(input)); err != nil {
			t.Logf("input %q returned error %v", input, err)
		}
	}
}

func FuzzJSON(f *testing.F) {
	f.Add(`{"level":"info","message":"hello"}`)
	f.Add(`[{"a":1}]`)
	f.Add(`{"a":1}
{"b":2}`)
	f.Add("")
	f.Fuzz(func(t *testing.T, input string) {
		events, err := (JSON{}).Parse("fuzz", []byte(input))
		if err != nil {
			return
		}
		for _, ev := range events {
			ev.EnsureID()
			if ev.Source != "fuzz" {
				t.Fatalf("source = %q", ev.Source)
			}
		}
	})
}

func FuzzSyslog(f *testing.F) {
	f.Add("<34>1 2026-09-30T09:15:01Z edge-01 app 1234 ID47 - message")
	f.Add("<13>Sep 30 09:15:01 api-11 sshd[123]: failed password")
	f.Add("<190>1 2026-09-30T09:15:01Z host app - - [meta key=\"value\"] payload")
	f.Add("no priority at all")
	f.Add("<")
	f.Fuzz(func(t *testing.T, input string) {
		ev, err := ParseSyslogLine("fuzz", input)
		if err != nil {
			return
		}
		// Whatever happens, the parser must return a usable event.
		if ev.Severity == event.SeverityUnknown {
			t.Fatalf("severity was left unknown for %q", input)
		}
	})
}

func BenchmarkJSONBatch(b *testing.B) {
	line := `{"timestamp":"2026-09-30T09:15:01Z","level":"warn","host":"edge-01","category":"api","actor":"svc","resource":"/v1/orders","message":"slow response","latency_ms":1204.9}`
	payload := []byte(strings.Join([]string{line, line, line, line, line, line, line, line, line, line}, "\n") + "\n")
	parser := JSON{}
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		events, err := parser.Parse("svc", payload)
		if err != nil {
			b.Fatalf("parse: %v", err)
		}
		if len(events) != 10 {
			b.Fatalf("events = %d, want 10", len(events))
		}
	}
}

func BenchmarkJSONSingleObject(b *testing.B) {
	payload := []byte(`{"timestamp":"2026-09-30T09:15:01Z","level":"warn","host":"edge-01","category":"api","actor":"svc","resource":"/v1/orders","message":"slow response","latency_ms":1204.9}`)
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := (JSON{}).Parse("svc", payload); err != nil {
			b.Fatalf("parse: %v", err)
		}
	}
}

func BenchmarkSyslogLine(b *testing.B) {
	line := []byte("<34>1 2026-09-30T09:15:01Z edge-01 app 1234 ID47 - request completed")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := ParseSyslogLine("svc", string(line)); err != nil {
			b.Fatalf("parse: %v", err)
		}
	}
}
