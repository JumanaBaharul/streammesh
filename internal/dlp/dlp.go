// Package dlp applies redaction and masking before an event reaches any sink.
//
// Because it runs as a fixed pipeline stage rather than inside each connector,
// a newly added sink cannot bypass it - the property you want from a data-loss
// prevention layer.
package dlp

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/JumanaBaharul/streammesh/internal/event"
)

// Pattern is a regular expression replacement applied to every string value.
type Pattern struct {
	Name       string `yaml:"name" json:"name"`
	Regex      string `yaml:"regex" json:"regex"`
	Replace    string `yaml:"replace" json:"replace"`
	compiledRe *regexp.Regexp
}

// FieldRule replaces a whole field value by dotted path.
type FieldRule struct {
	Path    string `yaml:"path" json:"path"`
	Replace string `yaml:"replace" json:"replace"`
}

// RuleSet is a named bundle of redaction rules.
type RuleSet struct {
	Name       string      `yaml:"name" json:"name"`
	Patterns   []Pattern   `yaml:"patterns" json:"patterns"`
	Fields     []FieldRule `yaml:"fields" json:"fields"`
	HashFields []string    `yaml:"hash_fields" json:"hash_fields"`
	// Salt is mixed into hashed values so the same value hashes differently
	// across deployments.
	Salt string `yaml:"salt" json:"salt"`

	fieldPaths []string
}

// Compile validates and prepares every rule, returning all problems at once.
func (rs *RuleSet) Compile() error {
	var problems []string
	for i := range rs.Patterns {
		p := &rs.Patterns[i]
		if p.Replace == "" {
			p.Replace = "[redacted]"
		}
		if p.Regex == "" {
			problems = append(problems, fmt.Sprintf("pattern %d (%s): empty regex", i, p.Name))
			continue
		}
		re, err := regexp.Compile(p.Regex)
		if err != nil {
			problems = append(problems, fmt.Sprintf("pattern %s: %v", p.Name, err))
			continue
		}
		p.compiledRe = re
	}
	for _, f := range rs.Fields {
		if f.Replace == "" {
			problems = append(problems, fmt.Sprintf("field %s: empty replacement", f.Path))
		}
		rs.fieldPaths = append(rs.fieldPaths, f.Path)
	}
	if len(problems) > 0 {
		return fmt.Errorf("dlp ruleset %q: %s", rs.Name, strings.Join(problems, "; "))
	}
	return nil
}

// Apply redacts the event in place and reports how many changes were made.
func (rs *RuleSet) Apply(ev *event.Event) int {
	if rs == nil || ev == nil {
		return 0
	}
	changes := 0

	if len(rs.Patterns) > 0 {
		changes += rs.redactValue(&ev.Attrs)

		if replacement, ok := rs.applyPatterns(ev.Host); ok {
			ev.Host = replacement
			changes++
		}
		if replacement, ok := rs.applyPatterns(ev.Actor); ok {
			ev.Actor = replacement
			changes++
		}
		if replacement, ok := rs.applyPatterns(ev.Resource); ok {
			ev.Resource = replacement
			changes++
		}
		if replacement, ok := rs.applyPatterns(ev.Category); ok {
			ev.Category = replacement
			changes++
		}
	}

	for _, rule := range rs.Fields {
		if _, found := ev.Field(rule.Path); !found {
			continue
		}
		if ev.SetField(rule.Path, rule.Replace) {
			changes++
		}
	}

	for _, path := range rs.HashFields {
		value, found := ev.Field(path)
		if !found {
			continue
		}
		raw := event.ValueString(value)
		if raw == "" {
			continue
		}
		if ev.SetField(path, rs.Hash(raw)) {
			changes++
		}
	}
	return changes
}

// Hash produces a stable pseudonym for a value, so records can still be
// correlated without storing the original.
func (rs *RuleSet) Hash(value string) string {
	sum := sha256.Sum256([]byte(rs.Salt + "\x00" + value))
	return "sha256:" + hex.EncodeToString(sum[:8])
}

func (rs *RuleSet) applyPatterns(s string) (string, bool) {
	if s == "" {
		return s, false
	}
	out := s
	changed := false
	for i := range rs.Patterns {
		p := &rs.Patterns[i]
		if p.compiledRe == nil {
			continue
		}
		if p.compiledRe.MatchString(out) {
			out = p.compiledRe.ReplaceAllString(out, p.Replace)
			changed = true
		}
	}
	return out, changed
}

// redactValue walks nested maps and slices, rewriting strings in place.
func (rs *RuleSet) redactValue(value *map[string]any) int {
	if value == nil || *value == nil {
		return 0
	}
	changes := 0
	for k, v := range *value {
		switch typed := v.(type) {
		case string:
			if replacement, changed := rs.applyPatterns(typed); changed {
				(*value)[k] = replacement
				changes++
			}
		case map[string]any:
			nested := typed
			changes += rs.redactValue(&nested)
		case []any:
			changes += rs.redactSlice(typed)
		}
	}
	return changes
}

func (rs *RuleSet) redactSlice(values []any) int {
	changes := 0
	for i, v := range values {
		switch typed := v.(type) {
		case string:
			if replacement, changed := rs.applyPatterns(typed); changed {
				values[i] = replacement
				changes++
			}
		case map[string]any:
			nested := typed
			changes += rs.redactValue(&nested)
		case []any:
			changes += rs.redactSlice(typed)
		}
	}
	return changes
}

// Library is the set of named rulesets available to rules.
type Library struct {
	sets map[string]*RuleSet
}

// NewLibrary compiles the provided rule sets, failing loudly on a bad rule.
func NewLibrary(sets []RuleSet) (*Library, error) {
	lib := &Library{sets: make(map[string]*RuleSet, len(sets))}
	for i := range sets {
		set := sets[i]
		if set.Name == "" {
			set.Name = fmt.Sprintf("set-%d", i)
		}
		if err := set.Compile(); err != nil {
			return nil, err
		}
		lib.sets[set.Name] = &set
	}
	return lib, nil
}

// Lookup returns the named ruleset.
func (l *Library) Lookup(name string) (*RuleSet, bool) {
	if l == nil {
		return nil, false
	}
	set, ok := l.sets[name]
	return set, ok
}

// Names lists the compiled ruleset names, sorted.
func (l *Library) Names() []string {
	if l == nil {
		return nil
	}
	names := make([]string, 0, len(l.sets))
	for name := range l.sets {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// DefaultRuleSet is the built-in PII ruleset used when a config asks for
// "default".
func DefaultRuleSet() RuleSet {
	return RuleSet{
		Name: "default",
		Patterns: []Pattern{
			{Name: "email", Regex: `[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}`, Replace: "[redacted:email]"},
			{Name: "card", Regex: `\b(?:\d[ \-]?){13,19}\b`, Replace: "[redacted:card]"},
			{Name: "bearer", Regex: `(?i)\bbearer\s+[A-Za-z0-9._\-]{8,}`, Replace: "[redacted:token]"},
			{Name: "aws_key", Regex: `\bAKIA[0-9A-Z]{16}\b`, Replace: "[redacted:aws-key]"},
			{Name: "private_key", Regex: `-----BEGIN [A-Z ]*PRIVATE KEY-----[\s\S]*?-----END [A-Z ]*PRIVATE KEY-----`, Replace: "[redacted:private-key]"},
			{Name: "jwt", Regex: `\beyJ[A-Za-z0-9_\-]{10,}\.[A-Za-z0-9_\-]{10,}\.[A-Za-z0-9_\-]{5,}\b`, Replace: "[redacted:jwt]"},
		},
		Fields: []FieldRule{
			{Path: "attrs.password", Replace: "[redacted:password]"},
			{Path: "attrs.secret", Replace: "[redacted:secret]"},
			{Path: "attrs.token", Replace: "[redacted:token]"},
			{Path: "attrs.authorization", Replace: "[redacted:auth]"},
			{Path: "attrs.api_key", Replace: "[redacted:api-key]"},
		},
	}
}
