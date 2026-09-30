// Package rules implements the declarative matching and routing engine.
//
// All rules are evaluated top to bottom for every event. Every matching rule's
// actions are applied in order, with two exceptions that short-circuit:
// "drop" discards the event immediately, and "route" accumulates destinations
// across all matching rules. An event that ends up routed nowhere goes to the
// pipeline's default sinks, or is counted as unrouted and dropped.
package rules

import (
	"context"
	"fmt"
	"hash/fnv"
	"math"
	"math/rand"
	"regexp"
	"strings"
	"sync/atomic"
	"time"

	"github.com/JumanaBaharul/streammesh/internal/dlp"
	"github.com/JumanaBaharul/streammesh/internal/duration"
	"github.com/JumanaBaharul/streammesh/internal/event"
	"github.com/JumanaBaharul/streammesh/internal/metrics"
)

// Matcher decides whether a rule applies to an event. Conditions inside one
// matcher are ANDed; use the all/any/not blocks for boolean composition.
type Matcher struct {
	Always *bool      `yaml:"always"`
	All    []*Matcher `yaml:"all"`
	Any    []*Matcher `yaml:"any"`
	Not    *Matcher   `yaml:"not"`

	Field    string   `yaml:"field"`
	Equals   any      `yaml:"equals"`
	Prefix   string   `yaml:"prefix"`
	Suffix   string   `yaml:"suffix"`
	Contains string   `yaml:"contains"`
	Regex    string   `yaml:"regex"`
	In       []string `yaml:"in"`
	Exists   *bool    `yaml:"exists"`
	Absent   bool     `yaml:"absent"`
	Gt       *float64 `yaml:"gt"`
	Gte      *float64 `yaml:"gte"`
	Lt       *float64 `yaml:"lt"`
	Lte      *float64 `yaml:"lte"`

	// Shorthands.
	Severity string `yaml:"severity"` // severity >= level
	Source   string `yaml:"source"`   // source_id == value
	HasTag   string `yaml:"has_tag"`

	compiledRegex *regexp.Regexp
	severityFloor *event.Severity
}

// Compile validates the matcher and prepares its regex and severity checks.
func (m *Matcher) Compile() error {
	if m == nil {
		return nil
	}
	if m.Regex != "" {
		re, err := regexp.Compile(m.Regex)
		if err != nil {
			return fmt.Errorf("invalid regex %q: %w", m.Regex, err)
		}
		m.compiledRegex = re
	}
	if m.Severity != "" {
		sev, err := event.ParseSeverity(m.Severity)
		if err != nil {
			return fmt.Errorf("invalid severity %q: %w", m.Severity, err)
		}
		m.severityFloor = &sev
	}
	for _, child := range m.All {
		if err := child.Compile(); err != nil {
			return err
		}
	}
	for _, child := range m.Any {
		if err := child.Compile(); err != nil {
			return err
		}
	}
	return m.Not.Compile()
}

// Matches reports whether the event satisfies the matcher.
func (m *Matcher) Matches(ev *event.Event) bool {
	if m == nil {
		return true
	}
	if m.Always != nil && !*m.Always {
		return false
	}
	for _, child := range m.All {
		if !child.Matches(ev) {
			return false
		}
	}
	if len(m.Any) > 0 {
		matchedSomething := false
		for _, child := range m.Any {
			if child.Matches(ev) {
				matchedSomething = true
				break
			}
		}
		if !matchedSomething {
			return false
		}
	}
	if m.Not != nil && m.Not.Matches(ev) {
		return false
	}

	if m.severityFloor != nil && ev.Severity < *m.severityFloor {
		return false
	}
	if m.Source != "" && ev.Source != m.Source {
		return false
	}
	if m.HasTag != "" {
		found := false
		for _, tag := range ev.Tags {
			if tag == m.HasTag {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	if m.Absent {
		if m.Field == "" {
			return false
		}
		if _, found := ev.Field(m.Field); found {
			return false
		}
		return true
	}
	if m.Field == "" {
		return true
	}

	value, found := ev.Field(m.Field)
	if m.Exists != nil {
		if *m.Exists != found {
			return false
		}
	}
	if !found {
		return false
	}

	if m.Equals != nil {
		if !equalValues(value, m.Equals) {
			return false
		}
	}
	text := event.ValueString(value)
	if m.Prefix != "" && !strings.HasPrefix(text, m.Prefix) {
		return false
	}
	if m.Suffix != "" && !strings.HasSuffix(text, m.Suffix) {
		return false
	}
	if m.Contains != "" && !strings.Contains(text, m.Contains) {
		return false
	}
	if m.compiledRegex != nil && !m.compiledRegex.MatchString(text) {
		return false
	}
	if len(m.In) > 0 {
		matched := false
		for _, candidate := range m.In {
			if text == candidate {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	if m.Gt != nil || m.Gte != nil || m.Lt != nil || m.Lte != nil {
		number, ok := event.ValueFloat(value)
		if !ok {
			return false
		}
		if m.Gt != nil && !(number > *m.Gt) {
			return false
		}
		if m.Gte != nil && !(number >= *m.Gte) {
			return false
		}
		if m.Lt != nil && !(number < *m.Lt) {
			return false
		}
		if m.Lte != nil && !(number <= *m.Lte) {
			return false
		}
	}
	return true
}

func equalValues(value any, want any) bool {
	if wantNumber, ok := event.ValueFloat(want); ok {
		if valueNumber, ok2 := event.ValueFloat(value); ok2 {
			return math.Abs(wantNumber-valueNumber) < 1e-9
		}
	}
	return event.ValueString(value) == event.ValueString(want)
}

// SampleSpec keeps a deterministic fraction of events.
type SampleSpec struct {
	// Percent is the share to keep, 0-100.
	Percent float64 `yaml:"percent"`
	// Every keeps one event out of N when set.
	Every int `yaml:"every"`
}

// SetSpec writes a constant or copied value onto a field.
type SetSpec struct {
	Path  string `yaml:"path"`
	Value any    `yaml:"value"`
	From  string `yaml:"from"`
}

// EnrichSpec configures the optional LLM enrichment action.
type EnrichSpec struct {
	Provider    string            `yaml:"provider"`
	URL         string            `yaml:"url"`
	Model       string            `yaml:"model"`
	Prompt      string            `yaml:"prompt"`
	PromptField string            `yaml:"prompt_field"`
	OutputField string            `yaml:"output_field"`
	Timeout     duration.Duration `yaml:"timeout"`
	CacheTTL    duration.Duration `yaml:"cache_ttl"`
	Fallback    string            `yaml:"fallback"`
}

// Action is a single mutation or routing decision.
type Action struct {
	Drop   bool        `yaml:"drop"`
	Tag    string      `yaml:"tag"`
	Redact string      `yaml:"redact"`
	Route  []string    `yaml:"route"`
	Sample *SampleSpec `yaml:"sample"`
	Set    []SetSpec   `yaml:"set"`
	Enrich *EnrichSpec `yaml:"enrich"`
}

// Rule ties a matcher to its actions.
type Rule struct {
	Name    string   `yaml:"name"`
	Match   *Matcher `yaml:"match"`
	Actions []Action `yaml:"actions"`
	Comment string   `yaml:"comment"`
}

// Enricher is implemented by the enrichment client and faked in tests.
type Enricher interface {
	Enrich(ctx context.Context, req EnrichRequest) (string, error)
}

// EnrichRequest is a single enrichment call.
type EnrichRequest struct {
	Provider    string
	URL         string
	Model       string
	Prompt      string
	Input       string
	OutputField string
	Timeout     time.Duration
	CacheTTL    time.Duration
}

// Decision is the outcome of evaluating the rule set for one event.
type Decision struct {
	Drop       bool
	DropReason string
	DropRule   string
	Sinks      []string
	Tags       []string
	Redacted   int
	Enriched   int
}

type ruleSet struct {
	rules []Rule
}

// Engine evaluates rule sets and can be hot-reloaded while traffic flows.
type Engine struct {
	current  atomic.Pointer[ruleSet]
	library  *dlp.Library
	enricher Enricher
	rand     *rand.Rand

	rulesEvaluated *metrics.Counter
	dropped        *metrics.Counter
	sampled        *metrics.Counter
	redactions     *metrics.Counter
	enrichCalls    *metrics.Counter
	enrichErrors   *metrics.Counter
	routed         *metrics.Counter
}

// New compiles the configured rules. It fails if any rule is invalid, so a bad
// ruleset is rejected at startup instead of at 3am.
func New(configs []Rule, library *dlp.Library, enricher Enricher, registry *metrics.Registry) (*Engine, error) {
	engine := &Engine{
		library:  library,
		enricher: enricher,
		rand:     rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	if registry != nil {
		engine.rulesEvaluated = registry.Counter("streammesh_rule_evaluations_total", "Events evaluated against the rule set.", nil)
		engine.dropped = registry.Counter("streammesh_dropped_total", "Events dropped before routing, by rule.", nil)
		engine.sampled = registry.Counter("streammesh_sampled_out_total", "Events discarded by a sampling action.", nil)
		engine.redactions = registry.Counter("streammesh_redactions_total", "Field and pattern redactions applied.", nil)
		engine.enrichCalls = registry.Counter("streammesh_enrichment_calls_total", "Enrichment calls attempted.", nil)
		engine.enrichErrors = registry.Counter("streammesh_enrichment_errors_total", "Enrichment calls that failed and used a fallback.", nil)
		engine.routed = registry.Counter("streammesh_routed_total", "Sink routing decisions taken.", nil)
	}
	if err := engine.Reload(configs); err != nil {
		return nil, err
	}
	return engine, nil
}

// Reload swaps in a new rule set atomically. Events in flight keep using the
// previous set, which is why rules must never be mutated in place.
func (e *Engine) Reload(configs []Rule) error {
	compiled := make([]Rule, 0, len(configs))
	seen := map[string]bool{}
	for i, cfg := range configs {
		rule := cfg
		if rule.Name == "" {
			rule.Name = fmt.Sprintf("rule-%d", i+1)
		}
		if seen[rule.Name] {
			return fmt.Errorf("rules: duplicate rule name %q", rule.Name)
		}
		seen[rule.Name] = true
		if err := rule.Match.Compile(); err != nil {
			return fmt.Errorf("rules: %s: %w", rule.Name, err)
		}
		for j := range rule.Actions {
			action := &rule.Actions[j]
			if action.Redact != "" {
				if _, ok := e.library.Lookup(action.Redact); !ok && action.Redact != "default" {
					return fmt.Errorf("rules: %s: unknown dlp ruleset %q", rule.Name, action.Redact)
				}
			}
			if action.Sample != nil {
				if action.Sample.Percent <= 0 && action.Sample.Every <= 0 {
					return fmt.Errorf("rules: %s: sample action needs percent or every", rule.Name)
				}
				if action.Sample.Percent > 100 {
					return fmt.Errorf("rules: %s: sample percent must be 0-100", rule.Name)
				}
			}
			if action.Enrich != nil && e.enricher == nil {
				return fmt.Errorf("rules: %s: enrichment is configured but no enricher is available", rule.Name)
			}
		}
		compiled = append(compiled, rule)
	}
	e.current.Store(&ruleSet{rules: compiled})
	return nil
}

// Rules returns the active rules, for the control-plane API.
func (e *Engine) Rules() []Rule {
	set := e.current.Load()
	if set == nil {
		return nil
	}
	return set.rules
}

// Evaluate applies every matching rule to the event.
func (e *Engine) Evaluate(ctx context.Context, ev *event.Event) Decision {
	decision := Decision{}
	set := e.current.Load()
	if set == nil {
		return decision
	}

	if e.rulesEvaluated != nil {
		e.rulesEvaluated.Inc()
	}

	for i := range set.rules {
		rule := &set.rules[i]
		if !rule.Match.Matches(ev) {
			continue
		}
		for j := range rule.Actions {
			action := &rule.Actions[j]

			if action.Drop {
				decision.Drop = true
				decision.DropReason = "rule"
				decision.DropRule = rule.Name
				if e.dropped != nil {
					e.dropped.Inc()
				}
				return decision
			}

			if action.Sample != nil && !e.keepSample(ev, action.Sample) {
				decision.Drop = true
				decision.DropReason = "sampled"
				decision.DropRule = rule.Name
				if e.sampled != nil {
					e.sampled.Inc()
				}
				return decision
			}

			if action.Redact != "" {
				set := e.library.Lookup
				ruleSet, ok := set(action.Redact)
				if !ok && action.Redact == "default" {
					builtin := dlp.DefaultRuleSet()
					if err := builtin.Compile(); err == nil {
						ruleSet, ok = &builtin, true
					}
				}
				if ok {
					changes := ruleSet.Apply(ev)
					decision.Redacted += changes
					if changes > 0 && e.redactions != nil {
						e.redactions.Add(float64(changes))
					}
				}
			}

			if action.Tag != "" && !containsString(ev.Tags, action.Tag) {
				ev.Tags = append(ev.Tags, action.Tag)
				decision.Tags = append(decision.Tags, action.Tag)
			}

			for _, assignment := range action.Set {
				if assignment.Path == "" {
					continue
				}
				value := assignment.Value
				if assignment.From != "" {
					if sourced, found := ev.Field(assignment.From); found {
						value = sourced
					}
				}
				ev.SetField(assignment.Path, value)
			}

			if len(action.Route) > 0 {
				for _, sink := range action.Route {
					if !containsString(decision.Sinks, sink) {
						decision.Sinks = append(decision.Sinks, sink)
						if e.routed != nil {
							e.routed.Inc()
						}
					}
				}
			}

			if action.Enrich != nil {
				e.applyEnrichment(ctx, ev, action.Enrich, &decision)
			}
		}
	}
	return decision
}

func (e *Engine) applyEnrichment(ctx context.Context, ev *event.Event, spec *EnrichSpec, decision *Decision) {
	if e.enricher == nil {
		return
	}
	input := ""
	if spec.PromptField != "" {
		if value, found := ev.Field(spec.PromptField); found {
			input = event.ValueString(value)
		}
	} else {
		input = event.ValueString(ev.Attrs["message"])
	}
	if strings.TrimSpace(input) == "" {
		return
	}

	if e.enrichCalls != nil {
		e.enrichCalls.Inc()
	}
	output, err := e.enricher.Enrich(ctx, EnrichRequest{
		Provider:    spec.Provider,
		URL:         spec.URL,
		Model:       spec.Model,
		Prompt:      spec.Prompt,
		Input:       input,
		OutputField: spec.OutputField,
		Timeout:     spec.Timeout.Or(2 * time.Second),
		CacheTTL:    spec.CacheTTL.Or(10 * time.Minute),
	})
	if err != nil || strings.TrimSpace(output) == "" {
		if e.enrichErrors != nil {
			e.enrichErrors.Inc()
		}
		if spec.Fallback != "" && spec.OutputField != "" {
			ev.SetField(spec.OutputField, spec.Fallback)
			ev.Attrs["enrichment_source"] = "fallback"
		}
		return
	}
	if spec.OutputField != "" {
		ev.SetField(spec.OutputField, strings.TrimSpace(output))
	}
	ev.Attrs["enrichment_source"] = "llm"
	decision.Enriched++
}

// keepSample decides deterministically from the event ID, so a retried event is
// sampled out (or kept) exactly like its first attempt.
//
// The decision uses integer arithmetic on a mixed hash rather than a float
// threshold. FNV-1a alone does not avalanche well in the bits a threshold
// comparison would read, which made the first implementation of this function
// keep none of the traffic; the SplitMix64 finaliser fixes that, and the bucket
// granularity is exact in tenths of a percent.
func (e *Engine) keepSample(ev *event.Event, spec *SampleSpec) bool {
	key := ev.ID
	if key == "" {
		key = ev.Digest()
	}
	hasher := fnv.New64a()
	_, _ = hasher.Write([]byte(key))
	roll := mix64(hasher.Sum64())

	if spec.Every > 1 {
		return roll%uint64(spec.Every) == 0
	}
	percent := spec.Percent
	if percent <= 0 {
		return false
	}
	if percent >= 100 {
		return true
	}
	const buckets = 1000
	threshold := uint64(percent / 100 * buckets)
	return roll%buckets < threshold
}

// mix64 is the SplitMix64 finaliser: it spreads the entropy of its input across
// every output bit, which is what makes modulo-based bucketing uniform.
func mix64(value uint64) uint64 {
	value ^= value >> 30
	value *= 0xbf58476d1ce4e5b9
	value ^= value >> 27
	value *= 0x94d049bb133111eb
	value ^= value >> 31
	return value
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
