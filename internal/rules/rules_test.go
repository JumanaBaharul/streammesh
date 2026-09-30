package rules

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/JumanaBaharul/streammesh/internal/dlp"
	"github.com/JumanaBaharul/streammesh/internal/event"
)

func mustLibrary(t *testing.T, sets ...dlp.RuleSet) *dlp.Library {
	t.Helper()
	library, err := dlp.NewLibrary(sets)
	if err != nil {
		t.Fatalf("NewLibrary: %v", err)
	}
	return library
}

func buildEngine(t *testing.T, configs []Rule, enricher Enricher) *Engine {
	t.Helper()
	engine, err := New(configs, mustLibrary(t), enricher, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return engine
}

func sampleEvent() event.Event {
	ev := event.New("svc-api")
	ev.Host = "edge-01"
	ev.Category = "api"
	ev.Actor = "svc-billing"
	ev.Severity = event.SeverityWarning
	ev.Attrs["status"] = float64(503)
	ev.Attrs["latency_ms"] = 1204.9
	return ev
}

func boolPtr(value bool) *bool        { return &value }
func floatPtr(value float64) *float64 { return &value }

func TestMatcherConditions(t *testing.T) {
	cases := []struct {
		name    string
		matcher *Matcher
		want    bool
	}{
		{name: "equals string", matcher: &Matcher{Field: "host", Equals: "edge-01"}, want: true},
		{name: "equals mismatch", matcher: &Matcher{Field: "host", Equals: "edge-02"}, want: false},
		{name: "equals number", matcher: &Matcher{Field: "attrs.status", Equals: 503}, want: true},
		{name: "prefix", matcher: &Matcher{Field: "resource", Prefix: "/v1"}, want: false},
		{name: "contains", matcher: &Matcher{Field: "host", Contains: "edge"}, want: true},
		{name: "suffix", matcher: &Matcher{Field: "host", Suffix: "-01"}, want: true},
		{name: "regex", matcher: &Matcher{Field: "host", Regex: `^edge-\d+$`}, want: true},
		{name: "in", matcher: &Matcher{Field: "category", In: []string{"auth", "api"}}, want: true},
		{name: "not in", matcher: &Matcher{Field: "category", In: []string{"auth"}}, want: false},
		{name: "exists", matcher: &Matcher{Field: "attrs.status", Exists: boolPtr(true)}, want: true},
		{name: "absent", matcher: &Matcher{Field: "attrs.missing", Absent: true}, want: true},
		{name: "absent on present field", matcher: &Matcher{Field: "attrs.status", Absent: true}, want: false},
		{name: "gt false", matcher: &Matcher{Field: "attrs.latency_ms", Gt: floatPtr(2000)}, want: false},
		{name: "gte true", matcher: &Matcher{Field: "attrs.latency_ms", Gte: floatPtr(1204.9)}, want: true},
		{name: "lt true", matcher: &Matcher{Field: "attrs.status", Lt: floatPtr(600)}, want: true},
		{name: "severity floor met", matcher: &Matcher{Severity: "warning"}, want: true},
		{name: "severity floor not met", matcher: &Matcher{Severity: "error"}, want: false},
		{name: "source", matcher: &Matcher{Source: "svc-api"}, want: true},
		{name: "source mismatch", matcher: &Matcher{Source: "other"}, want: false},
		{name: "always false", matcher: &Matcher{Always: boolPtr(false)}, want: false},
		{name: "nil matcher matches", matcher: nil, want: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.matcher.Compile(); err != nil {
				t.Fatalf("compile: %v", err)
			}
			ev := sampleEvent()
			if got := tc.matcher.Matches(&ev); got != tc.want {
				t.Fatalf("matches = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestMatcherComposition(t *testing.T) {
	ev := sampleEvent()

	all := &Matcher{All: []*Matcher{{Field: "host", Equals: "edge-01"}, {Field: "category", Equals: "api"}}}
	if err := all.Compile(); err != nil {
		t.Fatalf("compile: %v", err)
	}
	if !all.Matches(&ev) {
		t.Error("all-block should match")
	}

	failing := &Matcher{All: []*Matcher{{Field: "host", Equals: "edge-01"}, {Field: "category", Equals: "auth"}}}
	if err := failing.Compile(); err != nil {
		t.Fatalf("compile: %v", err)
	}
	if failing.Matches(&ev) {
		t.Error("all-block with one false child should not match")
	}

	any := &Matcher{Any: []*Matcher{{Field: "category", Equals: "auth"}, {Field: "category", Equals: "api"}}}
	if err := any.Compile(); err != nil {
		t.Fatalf("compile: %v", err)
	}
	if !any.Matches(&ev) {
		t.Error("any-block should match")
	}

	not := &Matcher{Not: &Matcher{Field: "category", Equals: "auth"}}
	if err := not.Compile(); err != nil {
		t.Fatalf("compile: %v", err)
	}
	if !not.Matches(&ev) {
		t.Error("not-block should match")
	}
}

func TestMatcherCompileRejectsInvalidRules(t *testing.T) {
	if err := (&Matcher{Regex: "([unclosed"}).Compile(); err == nil {
		t.Error("expected an invalid regex to fail")
	}
	if err := (&Matcher{Severity: "louder"}).Compile(); err == nil {
		t.Error("expected an invalid severity to fail")
	}
}

func TestEngineDropsAndStopsEvaluating(t *testing.T) {
	engine := buildEngine(t, []Rule{
		{
			Name:    "drop-noise",
			Match:   &Matcher{Field: "category", Equals: "api"},
			Actions: []Action{{Drop: true}},
		},
		{
			Name:    "would-route",
			Match:   &Matcher{Always: boolPtr(true)},
			Actions: []Action{{Route: []string{"archive"}}},
		},
	}, nil)

	ev := sampleEvent()
	decision := engine.Evaluate(context.Background(), &ev)
	if !decision.Drop {
		t.Fatal("expected the event to be dropped")
	}
	if decision.DropRule != "drop-noise" {
		t.Fatalf("drop rule = %q, want drop-noise", decision.DropRule)
	}
	if len(decision.Sinks) != 0 {
		t.Fatalf("a dropped event should not be routed anywhere, got %v", decision.Sinks)
	}
}

func TestEngineRoutesFanOutAndUnions(t *testing.T) {
	engine := buildEngine(t, []Rule{
		{
			Name:    "errors-to-alerts",
			Match:   &Matcher{Severity: "warning"},
			Actions: []Action{{Route: []string{"alerts"}}, {Tag: "alert"}},
		},
		{
			Name:    "everything-to-archive",
			Match:   &Matcher{Always: boolPtr(true)},
			Actions: []Action{{Route: []string{"archive", "alerts"}}},
		},
	}, nil)

	ev := sampleEvent()
	decision := engine.Evaluate(context.Background(), &ev)

	want := map[string]bool{"alerts": true, "archive": true}
	if len(decision.Sinks) != len(want) {
		t.Fatalf("sinks = %v, want exactly %v", decision.Sinks, want)
	}
	for _, id := range decision.Sinks {
		if !want[id] {
			t.Fatalf("unexpected sink %q in %v", id, decision.Sinks)
		}
	}
	if len(ev.Tags) != 1 || ev.Tags[0] != "alert" {
		t.Fatalf("tags = %v, want [alert]", ev.Tags)
	}
}

func TestSamplingIsDeterministicPerEventID(t *testing.T) {
	engine := buildEngine(t, []Rule{{
		Name:    "sample-ten",
		Match:   &Matcher{Always: boolPtr(true)},
		Actions: []Action{{Sample: &SampleSpec{Percent: 10}}},
	}}, nil)

	kept := 0
	for i := 0; i < 2000; i++ {
		ev := sampleEvent()
		ev.ID = fmt.Sprintf("event-%d", i)
		if !engine.Evaluate(context.Background(), &ev).Drop {
			kept++
		}
	}
	// A ten percent deterministic sample of 2000 events lands near 200.
	if kept < 120 || kept > 300 {
		t.Fatalf("kept %d of 2000 events, expected roughly 10%%", kept)
	}

	// The same id must always make the same decision, or a retried event could
	// be delivered twice or dropped.
	for i := 0; i < 50; i++ {
		first := sampleEvent()
		first.ID = "stable-id"
		second := sampleEvent()
		second.ID = "stable-id"
		if engine.Evaluate(context.Background(), &first).Drop != engine.Evaluate(context.Background(), &second).Drop {
			t.Fatal("sampling was not deterministic for a repeated event id")
		}
	}
}

func TestSampleEveryKeepsOneInN(t *testing.T) {
	engine := buildEngine(t, []Rule{{
		Name:    "every-ten",
		Match:   &Matcher{Always: boolPtr(true)},
		Actions: []Action{{Sample: &SampleSpec{Every: 10}}},
	}}, nil)

	kept := 0
	for i := 0; i < 1000; i++ {
		ev := sampleEvent()
		ev.ID = fmt.Sprintf("event-%d", i)
		if !engine.Evaluate(context.Background(), &ev).Drop {
			kept++
		}
	}
	if kept < 70 || kept > 130 {
		t.Fatalf("kept %d of 1000, expected roughly 100", kept)
	}
}

func TestRedactActionUsesTheNamedRuleSet(t *testing.T) {
	library := mustLibrary(t, dlp.RuleSet{
		Name: "pii",
		Fields: []dlp.FieldRule{
			{Path: "attrs.secret", Replace: "[redacted:secret]"},
		},
	})
	engine, err := New([]Rule{{
		Name:    "scrub",
		Match:   &Matcher{Always: boolPtr(true)},
		Actions: []Action{{Redact: "pii"}},
	}}, library, nil, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ev := sampleEvent()
	ev.Attrs["secret"] = "hunter2"
	decision := engine.Evaluate(context.Background(), &ev)

	if ev.Attrs["secret"] != "[redacted:secret]" {
		t.Fatalf("secret = %v", ev.Attrs["secret"])
	}
	if decision.Redacted != 1 {
		t.Fatalf("redacted = %d, want 1", decision.Redacted)
	}
}

func TestDefaultRuleSetNameIsAvailableWithoutConfiguration(t *testing.T) {
	engine := buildEngine(t, []Rule{{
		Name:    "scrub",
		Match:   &Matcher{Always: boolPtr(true)},
		Actions: []Action{{Redact: "default"}},
	}}, nil)

	ev := sampleEvent()
	ev.Attrs["message"] = "contact alice@example.com"
	engine.Evaluate(context.Background(), &ev)
	if got := event.ValueString(ev.Attrs["message"]); got == "contact alice@example.com" {
		t.Fatalf("message was not scrubbed: %q", got)
	}
}

func TestSetActionCopiesAndAssigns(t *testing.T) {
	engine := buildEngine(t, []Rule{{
		Name:  "annotate",
		Match: &Matcher{Always: boolPtr(true)},
		Actions: []Action{{Set: []SetSpec{
			{Path: "attrs.team", Value: "platform"},
			{Path: "attrs.host_copy", From: "host"},
		}}},
	}}, nil)

	ev := sampleEvent()
	engine.Evaluate(context.Background(), &ev)

	if ev.Attrs["team"] != "platform" {
		t.Errorf("team = %v", ev.Attrs["team"])
	}
	if ev.Attrs["host_copy"] != "edge-01" {
		t.Errorf("host_copy = %v", ev.Attrs["host_copy"])
	}
}

type fakeEnricher struct {
	mu     sync.Mutex
	output string
	err    error
	calls  int
	seen   EnrichRequest
	block  time.Duration
}

func (f *fakeEnricher) Enrich(ctx context.Context, req EnrichRequest) (string, error) {
	f.mu.Lock()
	f.calls++
	f.seen = req
	f.mu.Unlock()
	if f.block > 0 {
		select {
		case <-time.After(f.block):
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	return f.output, f.err
}

func TestEnrichmentWritesTheProviderAnswer(t *testing.T) {
	enricher := &fakeEnricher{output: "credential-stuffing"}
	engine := buildEngine(t, []Rule{{
		Name:  "classify",
		Match: &Matcher{Severity: "warning"},
		Actions: []Action{{Enrich: &EnrichSpec{
			Provider:    "ollama",
			PromptField: "attrs.message",
			OutputField: "attrs.ai_category",
		}}},
	}}, enricher)

	ev := sampleEvent()
	ev.Attrs["message"] = "many failed logins"
	decision := engine.Evaluate(context.Background(), &ev)

	if decision.Enriched != 1 {
		t.Fatalf("enriched = %d, want 1", decision.Enriched)
	}
	if ev.Attrs["ai_category"] != "credential-stuffing" {
		t.Fatalf("ai_category = %v", ev.Attrs["ai_category"])
	}
	if ev.Attrs["enrichment_source"] != "llm" {
		t.Fatalf("enrichment_source = %v", ev.Attrs["enrichment_source"])
	}
	if enricher.calls != 1 {
		t.Fatalf("provider calls = %d, want 1", enricher.calls)
	}
	if enricher.seen.Input != "many failed logins" {
		t.Fatalf("provider input = %q", enricher.seen.Input)
	}
}

func TestEnrichmentFallsBackWhenTheProviderFails(t *testing.T) {
	enricher := &fakeEnricher{err: errors.New("model unavailable")}
	engine := buildEngine(t, []Rule{{
		Name:  "classify",
		Match: &Matcher{Always: boolPtr(true)},
		Actions: []Action{{Enrich: &EnrichSpec{
			PromptField: "attrs.message",
			OutputField: "attrs.ai_category",
			Fallback:    "unclassified",
		}}},
	}}, enricher)

	ev := sampleEvent()
	ev.Attrs["message"] = "something happened"
	engine.Evaluate(context.Background(), &ev)

	if ev.Attrs["ai_category"] != "unclassified" {
		t.Fatalf("ai_category = %v, want the fallback", ev.Attrs["ai_category"])
	}
	if ev.Attrs["enrichment_source"] != "fallback" {
		t.Fatalf("enrichment_source = %v", ev.Attrs["enrichment_source"])
	}
}

func TestEnrichmentIsSkippedWithoutInput(t *testing.T) {
	enricher := &fakeEnricher{output: "x"}
	engine := buildEngine(t, []Rule{{
		Name:  "classify",
		Match: &Matcher{Always: boolPtr(true)},
		Actions: []Action{{Enrich: &EnrichSpec{
			PromptField: "attrs.absent",
			OutputField: "attrs.ai_category",
		}}},
	}}, enricher)

	ev := sampleEvent()
	engine.Evaluate(context.Background(), &ev)
	if enricher.calls != 0 {
		t.Fatalf("provider calls = %d, want 0 when there is no input", enricher.calls)
	}
}

func TestReloadRejectsInvalidRuleSetsAndKeepsTheOldOnes(t *testing.T) {
	engine := buildEngine(t, []Rule{{
		Name:    "original",
		Match:   &Matcher{Always: boolPtr(true)},
		Actions: []Action{{Route: []string{"archive"}}},
	}}, nil)

	cases := []struct {
		name  string
		rules []Rule
	}{
		{name: "duplicate names", rules: []Rule{
			{Name: "same", Actions: []Action{{Route: []string{"a"}}}},
			{Name: "same", Actions: []Action{{Route: []string{"b"}}}},
		}},
		{name: "unknown dlp set", rules: []Rule{
			{Name: "bad", Actions: []Action{{Redact: "nope"}}},
		}},
		{name: "sample without a rate", rules: []Rule{
			{Name: "bad", Actions: []Action{{Sample: &SampleSpec{}}}},
		}},
		{name: "sample above one hundred", rules: []Rule{
			{Name: "bad", Actions: []Action{{Sample: &SampleSpec{Percent: 150}}}},
		}},
		{name: "enrichment without an enricher", rules: []Rule{
			{Name: "bad", Actions: []Action{{Enrich: &EnrichSpec{OutputField: "attrs.x"}}}},
		}},
		{name: "broken matcher", rules: []Rule{
			{Name: "bad", Match: &Matcher{Regex: "(["}, Actions: []Action{{Route: []string{"a"}}}},
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := engine.Reload(tc.rules); err == nil {
				t.Fatal("Reload accepted an invalid rule set")
			}
			active := engine.Rules()
			if len(active) != 1 || active[0].Name != "original" {
				t.Fatalf("the running rules changed after a failed reload: %+v", active)
			}
		})
	}
}

func TestReloadSwapsRulesForNewTraffic(t *testing.T) {
	engine := buildEngine(t, []Rule{{
		Name:    "first",
		Match:   &Matcher{Always: boolPtr(true)},
		Actions: []Action{{Route: []string{"archive"}}},
	}}, nil)

	if err := engine.Reload([]Rule{{
		Name:    "second",
		Match:   &Matcher{Always: boolPtr(true)},
		Actions: []Action{{Route: []string{"elsewhere"}}},
	}}); err != nil {
		t.Fatalf("Reload: %v", err)
	}

	ev := sampleEvent()
	decision := engine.Evaluate(context.Background(), &ev)
	if len(decision.Sinks) != 1 || decision.Sinks[0] != "elsewhere" {
		t.Fatalf("sinks = %v, want [elsewhere]", decision.Sinks)
	}
}

func TestEngineAssignsNamesToUnnamedRules(t *testing.T) {
	engine := buildEngine(t, []Rule{{Match: &Matcher{Always: boolPtr(true)}}}, nil)
	rules := engine.Rules()
	if len(rules) != 1 || rules[0].Name != "rule-1" {
		t.Fatalf("rules = %+v", rules)
	}
}

func TestConcurrentEvaluationIsRaceFree(t *testing.T) {
	enricher := &fakeEnricher{output: "label"}
	engine := buildEngine(t, []Rule{
		{Name: "scrub", Match: &Matcher{Always: boolPtr(true)}, Actions: []Action{{Redact: "default"}}},
		{Name: "route", Match: &Matcher{Always: boolPtr(true)}, Actions: []Action{{Route: []string{"archive"}}}},
		{Name: "enrich", Match: &Matcher{Severity: "warning"}, Actions: []Action{{Enrich: &EnrichSpec{
			PromptField: "attrs.message", OutputField: "attrs.ai_category",
		}}}},
	}, enricher)

	done := make(chan struct{})
	for worker := 0; worker < 8; worker++ {
		go func() {
			defer func() { done <- struct{}{} }()
			for i := 0; i < 200; i++ {
				ev := sampleEvent()
				ev.ID = fmt.Sprintf("event-%d", i)
				ev.Attrs["message"] = "contact alice@example.com"
				engine.Evaluate(context.Background(), &ev)
			}
		}()
	}
	for worker := 0; worker < 8; worker++ {
		<-done
	}
}
