// Package bytesize provides a YAML-friendly byte count so configuration can say
// "32MB" instead of 33554432.
package bytesize

import (
	"fmt"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// Size is a byte count that unmarshals from "8MB", "512KB", "1GiB" or a plain
// number of bytes.
type Size int64

var suffixes = []struct {
	suffix string
	scale  int64
}{
	{"kib", 1 << 10}, {"mib", 1 << 20}, {"gib", 1 << 30}, {"tib", 1 << 40},
	{"kb", 1 << 10}, {"mb", 1 << 20}, {"gb", 1 << 30}, {"tb", 1 << 40},
	{"b", 1},
}

// UnmarshalYAML accepts a string with a unit suffix or a plain integer.
func (s *Size) UnmarshalYAML(node *yaml.Node) error {
	var numeric int64
	if err := node.Decode(&numeric); err == nil {
		*s = Size(numeric)
		return nil
	}

	var raw string
	if err := node.Decode(&raw); err != nil {
		return fmt.Errorf("invalid size %v", node.Value)
	}
	parsed, err := Parse(raw)
	if err != nil {
		return err
	}
	*s = parsed
	return nil
}

// MarshalYAML renders the size back as a number of bytes.
func (s Size) MarshalYAML() (any, error) { return int64(s), nil }

// Parse reads a human-readable byte size such as "32MB".
func Parse(value string) (Size, error) {
	trimmed := strings.ToLower(strings.TrimSpace(value))
	if trimmed == "" {
		return 0, nil
	}
	for _, entry := range suffixes {
		if strings.HasSuffix(trimmed, entry.suffix) {
			number := strings.TrimSpace(strings.TrimSuffix(trimmed, entry.suffix))
			parsed, err := strconv.ParseFloat(number, 64)
			if err != nil {
				return 0, fmt.Errorf("invalid size %q", value)
			}
			return Size(int64(parsed * float64(entry.scale))), nil
		}
	}
	parsed, err := strconv.ParseInt(trimmed, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid size %q (use a number of bytes or a suffix such as MB)", value)
	}
	return Size(parsed), nil
}

// Int64 returns the size in bytes.
func (s Size) Int64() int64 { return int64(s) }

// Or returns the size, or fallback when unset.
func (s Size) Or(fallback int64) int64 {
	if s <= 0 {
		return fallback
	}
	return int64(s)
}
