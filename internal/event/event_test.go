package event

import (
	"encoding/json"
	"testing"
	"time"
)

func TestParseSeverityAcceptssNamesAndNumbers(t *testing.T) {
	cases := []struct {
		in    string
		want  Severity
		fails bool
	}{
		{in: "warn", want: SeverityWarning},
		{in: "WARNING", want: SeverityWarning},
		{in: "Err", want: SeverityError},
		{in: "fatal", want: SeverityCritical},
		{in: "panic", want: SeverityEmergency},
		{in: "6", want: SeverityCritical},
		{in: "", want: SeverityInfo},
		{in: "9", fails: true},
		{in: "nonsense", fails: true},
	}
	for _, tc := range cases {
		got, err := ParseSeverity(tc.in)
		if tc.fails {
			if err == nil {
				t.Errorf("ParseSeverity(%q): expected an error", tc.in)
			}
			continue
		}
		if err != nil {
			t.Errorf("ParseSeverity(%q): unexpected error %v", tc.in, err)
			continue
		}
		if got != tc.want {
			t.Errorf("ParseSeverity(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

func TestSeverityOrdersLikeAnInteger(t *testing.T) {
	if !(SeverityWarning < SeverityError) {
		t.Fatal("warning should rank below error")
	}
	if !(SeverityDebug < SeverityEmergency) {
		t.Fatal("debug should rank below emergency")
	}
}

func TestSeverityJSONRoundTrip(t *testing.T) {
	raw, err := json.Marshal(SeverityError)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(raw) != `"error"` {
		t.Fatalf("marshalled severity = %s, want \"error\"", raw)
	}

	var decoded Severity
	if err := json.Unmarshal([]byte(`"warning"`), &decoded); err != nil {
		t.Fatalf("unmarshal string: %v", err)
	}
	if decoded != SeverityWarning {
		t.Fatalf("decoded = %v, want warning", decoded)
	}
	if err := json.Unmarshal([]byte(`5`), &decoded); err != nil {
		t.Fatalf("unmarshal number: %v", err)
	}
	if decoded != SeverityError {
		t.Fatalf("decoded = %v, want error", decoded)
	}
	if err := json.Unmarshal([]byte(`99`), &decoded); err == nil {
		t.Fatal("expected an out-of-range severity to fail")
	}
}

func TestNormalizeFillsDefaults(t *testing.T) {
	var ev Event
	ev.Normalize()

	if ev.Timestamp.IsZero() {
		t.Fatal("timestamp was not defaulted")
	}
	if ev.ReceivedAt.IsZero() {
		t.Fatal("received_at was not defaulted")
	}
	if ev.Attrs == nil {
		t.Fatal("attrs was not initialised")
	}
	if ev.Severity != SeverityInfo {
		t.Fatalf("severity = %v, want info", ev.Severity)
	}
	if ev.Timestamp.Location() != time.UTC {
		t.Fatalf("timestamp location = %v, want UTC", ev.Timestamp.Location())
	}
}

func TestNormalizeRepairsOutOfRangeSeverity(t *testing.T) {
	ev := New("test")
	ev.Severity = Severity(42)
	ev.Normalize()
	if ev.Severity != SeverityInfo {
		t.Fatalf("severity = %v, want info", ev.Severity)
	}
}

func TestUnknownSeverityIsNotTheSameAsDebug(t *testing.T) {
	var ev Event
	if ev.Severity != SeverityUnknown {
		t.Fatalf("zero value = %v, want unknown", ev.Severity)
	}
	if SeverityUnknown == SeverityDebug {
		t.Fatal("unknown and debug must be distinct levels")
	}
	ev.Normalize()
	if ev.Severity != SeverityInfo {
		t.Fatalf("severity = %v, want info after normalisation", ev.Severity)
	}
}

func TestFieldReadsNestedAttributes(t *testing.T) {
	ev := New("svc")
	ev.Host = "edge-01"
	ev.Attrs["http"] = map[string]any{"status": float64(503), "path": "/v1/orders"}
	ev.Attrs["tags"] = []any{"a", "b"}

	cases := []struct {
		path  string
		want  string
		found bool
	}{
		{path: "host", want: "edge-01", found: true},
		{path: "source", want: "svc", found: true},
		{path: "severity", want: "info", found: true},
		{path: "attrs.http.status", want: "503", found: true},
		{path: "attrs.http.path", want: "/v1/orders", found: true},
		{path: "attrs.tags.1", want: "b", found: true},
		{path: "attrs.http.missing", found: false},
		{path: "nope", found: false},
		{path: "attrs.tags.9", found: false},
	}
	for _, tc := range cases {
		got, ok := ev.Field(tc.path)
		if ok != tc.found {
			t.Errorf("Field(%q) found = %v, want %v", tc.path, ok, tc.found)
			continue
		}
		if tc.found && ValueString(got) != tc.want {
			t.Errorf("Field(%q) = %q, want %q", tc.path, ValueString(got), tc.want)
		}
	}
}

func TestSetFieldCreatesIntermediateMaps(t *testing.T) {
	ev := New("svc")
	if !ev.SetField("attrs.dlp.match", "email") {
		t.Fatal("SetField reported failure")
	}
	got, ok := ev.Field("attrs.dlp.match")
	if !ok || ValueString(got) != "email" {
		t.Fatalf("round trip failed, got %v (found=%v)", got, ok)
	}
	if !ev.SetField("host", "edge-02") {
		t.Fatal("SetField on a top-level field failed")
	}
	if ev.Host != "edge-02" {
		t.Fatalf("host = %q, want edge-02", ev.Host)
	}
}

func TestCloneIsDeep(t *testing.T) {
	ev := New("svc")
	ev.Attrs["nested"] = map[string]any{"key": "original"}
	ev.Tags = []string{"one"}

	clone := ev.Clone()
	clone.Attrs["nested"].(map[string]any)["key"] = "mutated"
	clone.Attrs["added"] = true
	clone.Tags = append(clone.Tags, "two")

	if ev.Attrs["nested"].(map[string]any)["key"] != "original" {
		t.Fatal("clone shared nested maps with the original")
	}
	if _, found := ev.Attrs["added"]; found {
		t.Fatal("clone shared the top-level map with the original")
	}
	if len(ev.Tags) != 1 {
		t.Fatal("clone shared the tag slice with the original")
	}
}

func TestDigestIsStableAndContentSensitive(t *testing.T) {
	base := New("svc")
	base.Timestamp = time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)
	base.Attrs["a"] = "b"
	base.EnsureID()

	same := base.Clone()
	if same.Digest() != base.Digest() {
		t.Fatal("identical events produced different digests")
	}

	different := base.Clone()
	different.Attrs["a"] = "c"
	if different.Digest() == base.Digest() {
		t.Fatal("changed content produced the same digest")
	}

	// Receipt time must not affect the digest, or retries would dedup differently.
	later := base.Clone()
	later.ReceivedAt = base.ReceivedAt.Add(time.Hour)
	if later.Digest() != base.Digest() {
		t.Fatal("receipt time changed the digest")
	}
}

func TestEnsureIDKeepsProvidedID(t *testing.T) {
	ev := New("svc")
	ev.ID = "from-producer"
	ev.EnsureID()
	if ev.ID != "from-producer" {
		t.Fatalf("id = %q, want from-producer", ev.ID)
	}
}

func TestValueFloat(t *testing.T) {
	cases := []struct {
		in   any
		want float64
		ok   bool
	}{
		{in: float64(12.5), want: 12.5, ok: true},
		{in: 7, want: 7, ok: true},
		{in: "3.5", want: 3.5, ok: true},
		{in: "abc", ok: false},
		{in: nil, ok: false},
	}
	for _, tc := range cases {
		got, ok := ValueFloat(tc.in)
		if ok != tc.ok {
			t.Errorf("ValueFloat(%v) ok = %v, want %v", tc.in, ok, tc.ok)
			continue
		}
		if tc.ok && got != tc.want {
			t.Errorf("ValueFloat(%v) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

func TestValueStringFormatsWholeFloatsWithoutDecimals(t *testing.T) {
	if got := ValueString(float64(200)); got != "200" {
		t.Fatalf("ValueString(200.0) = %q, want 200", got)
	}
	if got := ValueString(float64(1.5)); got != "1.5" {
		t.Fatalf("ValueString(1.5) = %q, want 1.5", got)
	}
}
