package dlp

import (
	"strings"
	"testing"

	"github.com/JumanaBaharul/streammesh/internal/event"
)

func compile(t *testing.T, set RuleSet) *RuleSet {
	t.Helper()
	if err := set.Compile(); err != nil {
		t.Fatalf("compile ruleset %q: %v", set.Name, err)
	}
	return &set
}

func TestPatternsRedactEveryStringField(t *testing.T) {
	set := compile(t, RuleSet{
		Name: "pii",
		Patterns: []Pattern{
			{Name: "email", Regex: `[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}`, Replace: "[redacted:email]"},
			{Name: "bearer", Regex: `(?i)\bbearer\s+[A-Za-z0-9._\-]{8,}`, Replace: "[redacted:token]"},
		},
	})

	ev := event.New("svc")
	ev.Actor = "alice.k@example.com"
	ev.Host = "edge-01"
	ev.Attrs["message"] = "login failed for bob@example.com"
	ev.Attrs["header"] = "Authorization: Bearer abcdefghijklmnop"
	ev.Attrs["nested"] = map[string]any{"contact": "carol@example.com"}
	ev.Attrs["list"] = []any{"dave@example.com", "safe"}

	changes := set.Apply(&ev)
	if changes == 0 {
		t.Fatal("no redactions were reported")
	}

	if ev.Actor != "[redacted:email]" {
		t.Errorf("actor = %q", ev.Actor)
	}
	if ev.Host != "edge-01" {
		t.Errorf("host should be untouched, got %q", ev.Host)
	}
	if message := event.ValueString(ev.Attrs["message"]); strings.Contains(message, "bob@example.com") {
		t.Errorf("message still contains the email: %q", message)
	}
	if header := event.ValueString(ev.Attrs["header"]); !strings.Contains(header, "[redacted:token]") {
		t.Errorf("bearer token was not redacted: %q", header)
	}
	nested := ev.Attrs["nested"].(map[string]any)
	if nested["contact"] != "[redacted:email]" {
		t.Errorf("nested contact = %v", nested["contact"])
	}
	list := ev.Attrs["list"].([]any)
	if list[0] != "[redacted:email]" {
		t.Errorf("list entry = %v", list[0])
	}
	if list[1] != "safe" {
		t.Errorf("list entry should be untouched, got %v", list[1])
	}
}

func TestFieldRulesReplaceWholeValues(t *testing.T) {
	set := compile(t, RuleSet{
		Name: "fields",
		Fields: []FieldRule{
			{Path: "attrs.password", Replace: "[redacted:password]"},
			{Path: "attrs.authorization", Replace: "[redacted:auth]"},
		},
	})

	ev := event.New("svc")
	ev.Attrs["password"] = "hunter2"
	ev.Attrs["authorization"] = "Basic Zm9vOmJhcg=="
	ev.Attrs["untouched"] = "keep me"

	set.Apply(&ev)

	if ev.Attrs["password"] != "[redacted:password]" {
		t.Errorf("password = %v", ev.Attrs["password"])
	}
	if ev.Attrs["authorization"] != "[redacted:auth]" {
		t.Errorf("authorization = %v", ev.Attrs["authorization"])
	}
	if ev.Attrs["untouched"] != "keep me" {
		t.Errorf("unrelated field changed: %v", ev.Attrs["untouched"])
	}
}

func TestMissingFieldsAreSkippedQuietly(t *testing.T) {
	set := compile(t, RuleSet{
		Name:   "fields",
		Fields: []FieldRule{{Path: "attrs.absent", Replace: "[redacted]"}},
	})
	ev := event.New("svc")
	if changes := set.Apply(&ev); changes != 0 {
		t.Fatalf("changes = %d, want 0 for a missing field", changes)
	}
}

func TestHashFieldsProduceStablePseudonyms(t *testing.T) {
	set := compile(t, RuleSet{
		Name:       "hash",
		HashFields: []string{"attrs.user_id"},
		Salt:       "deployment-a",
	})

	first := event.New("svc")
	first.Attrs["user_id"] = "user-42"
	set.Apply(&first)

	second := event.New("svc")
	second.Attrs["user_id"] = "user-42"
	set.Apply(&second)

	third := event.New("svc")
	third.Attrs["user_id"] = "user-43"
	set.Apply(&third)

	hashedFirst := event.ValueString(first.Attrs["user_id"])
	hashedSecond := event.ValueString(second.Attrs["user_id"])
	hashedThird := event.ValueString(third.Attrs["user_id"])

	if hashedFirst != hashedSecond {
		t.Fatalf("the same value hashed differently: %q vs %q", hashedFirst, hashedSecond)
	}
	if hashedFirst == hashedThird {
		t.Fatal("different values produced the same hash")
	}
	if !strings.HasPrefix(hashedFirst, "sha256:") {
		t.Fatalf("hash %q does not carry its algorithm prefix", hashedFirst)
	}
	if strings.Contains(hashedFirst, "user-42") {
		t.Fatal("the original value leaked into the hash")
	}

	// A different salt must produce a different pseudonym, or the same person
	// would be correlatable across deployments.
	other := compile(t, RuleSet{Name: "hash", HashFields: []string{"attrs.user_id"}, Salt: "deployment-b"})
	ev := event.New("svc")
	ev.Attrs["user_id"] = "user-42"
	other.Apply(&ev)
	if event.ValueString(ev.Attrs["user_id"]) == hashedFirst {
		t.Fatal("a different salt produced the same pseudonym")
	}
}

func TestCompileReportsBrokenRules(t *testing.T) {
	cases := []RuleSet{
		{Name: "bad-regex", Patterns: []Pattern{{Name: "broken", Regex: "([unclosed"}}},
		{Name: "empty-regex", Patterns: []Pattern{{Name: "empty"}}},
		{Name: "empty-replacement", Fields: []FieldRule{{Path: "attrs.x"}}},
	}
	for _, set := range cases {
		if err := set.Compile(); err == nil {
			t.Errorf("ruleset %q compiled without error", set.Name)
		}
	}
}

func TestLibraryResolvesNamesAndRejectsBadSets(t *testing.T) {
	library, err := NewLibrary([]RuleSet{
		{Name: "pii", Patterns: []Pattern{{Regex: `a`, Replace: "b"}}},
	})
	if err != nil {
		t.Fatalf("NewLibrary: %v", err)
	}
	if _, ok := library.Lookup("pii"); !ok {
		t.Fatal("named ruleset was not found")
	}
	if _, ok := library.Lookup("missing"); ok {
		t.Fatal("unknown ruleset was found")
	}

	if _, err := NewLibrary([]RuleSet{{Name: "broken", Patterns: []Pattern{{Regex: "(["}}}}); err == nil {
		t.Fatal("NewLibrary accepted an invalid ruleset")
	}
}

func TestDefaultRuleSetCatchesCommonSecrets(t *testing.T) {
	set := DefaultRuleSet()
	if err := set.Compile(); err != nil {
		t.Fatalf("the built-in ruleset does not compile: %v", err)
	}

	ev := event.New("svc")
	ev.Attrs["message"] = "contact alice@example.com key AKIAIOSFODNN7EXAMPLE token eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.dozjgNryP4J3jVmNHl0w5N_XgL0n3I9PlFUP0THsR8U"
	ev.Attrs["password"] = "hunter2"

	set.Apply(&ev)

	message := event.ValueString(ev.Attrs["message"])
	for _, leak := range []string{"alice@example.com", "AKIAIOSFODNN7EXAMPLE", "eyJhbGciOiJIUzI1NiJ9"} {
		if strings.Contains(message, leak) {
			t.Errorf("message still leaks %q: %s", leak, message)
		}
	}
	if ev.Attrs["password"] != "[redacted:password]" {
		t.Errorf("password = %v", ev.Attrs["password"])
	}
}

func TestApplyOnNilIsSafe(t *testing.T) {
	var set *RuleSet
	ev := event.New("svc")
	if changes := set.Apply(&ev); changes != 0 {
		t.Fatalf("changes = %d, want 0", changes)
	}
}

func BenchmarkApplyCleanEvent(b *testing.B) {
	set := DefaultRuleSet()
	if err := set.Compile(); err != nil {
		b.Fatalf("compile: %v", err)
	}
	ev := event.New("svc")
	ev.Attrs["message"] = "request completed in 42ms"
	ev.Attrs["trace_id"] = "trace-12345678"

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		candidate := ev
		set.Apply(&candidate)
	}
}
