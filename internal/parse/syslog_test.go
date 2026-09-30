package parse

import (
	"testing"

	"github.com/JumanaBaharul/streammesh/internal/event"
)

func TestParseSyslogRFC5424(t *testing.T) {
	line := `<34>1 2026-09-30T09:15:01.000Z edge-01 api 1234 ID47 [meta key="value"] request completed`
	ev, err := ParseSyslogLine("syslog", line)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	// PRI 34: facility 4 (auth), severity 2 (critical).
	if ev.Severity != event.SeverityCritical {
		t.Errorf("severity = %v, want critical", ev.Severity)
	}
	if ev.Attrs["facility"] != "4" {
		t.Errorf("facility = %v, want 4", ev.Attrs["facility"])
	}
	if ev.Attrs["facility_name"] != "auth" {
		t.Errorf("facility_name = %v, want auth", ev.Attrs["facility_name"])
	}
	if ev.Host != "edge-01" {
		t.Errorf("host = %q", ev.Host)
	}
	if ev.Category != "api" {
		t.Errorf("category = %q", ev.Category)
	}
	if ev.Attrs["procid"] != "1234" {
		t.Errorf("procid = %v", ev.Attrs["procid"])
	}
	if ev.Attrs["msgid"] != "ID47" {
		t.Errorf("msgid = %v", ev.Attrs["msgid"])
	}
	if event.ValueString(ev.Attrs["message"]) != "request completed" {
		t.Errorf("message = %v", ev.Attrs["message"])
	}
	if ev.Timestamp.Year() != 2026 {
		t.Errorf("timestamp = %s", ev.Timestamp)
	}
}

func TestParseSyslogRFC3164(t *testing.T) {
	ev, err := ParseSyslogLine("syslog", `<13>Sep 30 09:15:01 api-11 sshd[123]: failed password for root`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	// PRI 13: facility 1 (user), severity 5 (notice).
	if ev.Severity != event.SeverityNotice {
		t.Errorf("severity = %v, want notice", ev.Severity)
	}
	if ev.Host != "api-11" {
		t.Errorf("host = %q", ev.Host)
	}
	if ev.Category != "sshd" {
		t.Errorf("category = %q", ev.Category)
	}
	if ev.Attrs["procid"] != "123" {
		t.Errorf("procid = %v", ev.Attrs["procid"])
	}
	if event.ValueString(ev.Attrs["message"]) != "failed password for root" {
		t.Errorf("message = %v", ev.Attrs["message"])
	}
}

func TestParseSyslogWithoutPriorityFallsBackToInfo(t *testing.T) {
	ev, err := ParseSyslogLine("syslog", "just a bare message")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if ev.Severity != event.SeverityInfo {
		t.Errorf("severity = %v, want info", ev.Severity)
	}
	if event.ValueString(ev.Attrs["message"]) == "" {
		t.Error("the bare message was lost")
	}
}

func TestStructuredDataWithNestedBracketsIsNotTruncated(t *testing.T) {
	line := `<190>1 2026-09-30T09:15:01Z host app - - [meta keys="[a,b]" other="x"] payload after`
	ev, err := ParseSyslogLine("syslog", line)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	structured := event.ValueString(ev.Attrs["structured_data"])
	if structured != `[meta keys="[a,b]" other="x"]` {
		t.Errorf("structured_data = %q", structured)
	}
	if event.ValueString(ev.Attrs["message"]) != "payload after" {
		t.Errorf("message = %v", ev.Attrs["message"])
	}
}

func TestSyslogBatchSkipsUnparseableLines(t *testing.T) {
	payload := `<13>Sep 30 09:15:01 api-11 app: first

<13>Sep 30 09:15:02 api-11 app: second
`
	events, err := Syslog{}.Parse("syslog", []byte(payload))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("events = %d, want 2", len(events))
	}
}

func TestEmptySyslogLineIsAnError(t *testing.T) {
	if _, err := ParseSyslogLine("syslog", "   "); err == nil {
		t.Fatal("expected an error for an empty line")
	}
}

func TestSyslogSeverityMappingCoversAllLevels(t *testing.T) {
	cases := map[int]event.Severity{
		0: event.SeverityEmergency,
		1: event.SeverityAlert,
		2: event.SeverityCritical,
		3: event.SeverityError,
		4: event.SeverityWarning,
		5: event.SeverityNotice,
		6: event.SeverityInfo,
		7: event.SeverityDebug,
	}
	for code, want := range cases {
		if got := syslogSeverity(code); got != want {
			t.Errorf("syslogSeverity(%d) = %v, want %v", code, got, want)
		}
	}
}
