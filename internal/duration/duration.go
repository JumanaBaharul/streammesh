// Package duration provides a YAML-friendly wrapper around time.Duration so
// config files can say "250ms" instead of 250000000.
package duration

import (
	"fmt"
	"time"

	"gopkg.in/yaml.v3"
)

// Duration is a time.Duration that unmarshals from strings like "1s" or "250ms"
// and from bare integers, which are read as seconds.
type Duration time.Duration

// UnmarshalYAML accepts "250ms", "1.5s", or a number of seconds.
func (d *Duration) UnmarshalYAML(node *yaml.Node) error {
	var raw string
	if err := node.Decode(&raw); err == nil {
		trimmed := raw
		if trimmed == "" {
			*d = 0
			return nil
		}
		parsed, err := time.ParseDuration(trimmed)
		if err != nil {
			return fmt.Errorf("invalid duration %q: %w", raw, err)
		}
		*d = Duration(parsed)
		return nil
	}
	var seconds float64
	if err := node.Decode(&seconds); err != nil {
		return fmt.Errorf("invalid duration %v: %w", node.Value, err)
	}
	*d = Duration(time.Duration(seconds * float64(time.Second)))
	return nil
}

// MarshalYAML renders the duration back as a string.
func (d Duration) MarshalYAML() (any, error) {
	return time.Duration(d).String(), nil
}

// Duration returns the standard-library value.
func (d Duration) Duration() time.Duration { return time.Duration(d) }

// Or returns the duration, or fallback when unset.
func (d Duration) Or(fallback time.Duration) time.Duration {
	if d <= 0 {
		return fallback
	}
	return time.Duration(d)
}
