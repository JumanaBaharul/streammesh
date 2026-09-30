// Package config loads and validates the pipeline definition.
//
// Loading is strict: an unknown key is an error rather than a silent no-op, so
// a typo in a sink name or a misspelled option fails at startup instead of
// quietly dropping data.
package config

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/JumanaBaharul/streammesh/internal/bytesize"
	"github.com/JumanaBaharul/streammesh/internal/dlp"
	"github.com/JumanaBaharul/streammesh/internal/duration"
	"github.com/JumanaBaharul/streammesh/internal/parse"
	"github.com/JumanaBaharul/streammesh/internal/rules"
)

// Config is the whole pipeline definition.
type Config struct {
	Server   Server       `yaml:"server"`
	Pipeline Pipeline     `yaml:"pipeline"`
	DLP      DLP          `yaml:"dlp"`
	Sources  []Source     `yaml:"sources"`
	Sinks    []Sink       `yaml:"sinks"`
	Rules    []rules.Rule `yaml:"rules"`
}

// Server configures the HTTP surface: ingestion endpoints, the control plane
// and the metrics endpoint.
type Server struct {
	Addr          string            `yaml:"addr"`
	APIToken      string            `yaml:"api_token"`
	APITokenEnv   string            `yaml:"api_token_env"`
	ReadTimeout   duration.Duration `yaml:"read_timeout"`
	WriteTimeout  duration.Duration `yaml:"write_timeout"`
	MaxHeaderByte int               `yaml:"max_header_bytes"`
}

// Pipeline holds engine-wide settings.
type Pipeline struct {
	Workers       int               `yaml:"workers"`
	QueueSize     int               `yaml:"queue_size"`
	AcceptTimeout duration.Duration `yaml:"accept_timeout"`
	SinkQueueSize int               `yaml:"sink_queue_size"`
	DefaultSinks  []string          `yaml:"default_sinks"`
	ShutdownGrace duration.Duration `yaml:"shutdown_grace"`
	WAL           WALConfig         `yaml:"wal"`
	Dedup         DedupConfig       `yaml:"dedup"`
	Retry         RetryConfig       `yaml:"retry"`
}

// WALConfig configures the write-ahead log.
type WALConfig struct {
	Enabled       *bool             `yaml:"enabled"`
	Dir           string            `yaml:"dir"`
	SegmentBytes  bytesize.Size     `yaml:"segment_bytes"`
	FsyncInterval duration.Duration `yaml:"fsync_interval"`
	SyncEach      bool              `yaml:"sync_each"`
	MaxSegments   int               `yaml:"max_segments"`
}

// DedupConfig configures duplicate suppression.
type DedupConfig struct {
	Enabled *bool             `yaml:"enabled"`
	TTL     duration.Duration `yaml:"ttl"`
	Bits    int               `yaml:"bits"`
	Hashes  int               `yaml:"hashes"`
	// DropProbable makes a bloom-only hit discard the event. The default keeps
	// the event, because a saturated filter must never cause silent data loss.
	DropProbable bool `yaml:"drop_probable"`
}

// RetryConfig is the default delivery policy for sinks.
type RetryConfig struct {
	MaxAttempts int               `yaml:"max_attempts"`
	Base        duration.Duration `yaml:"base"`
	Max         duration.Duration `yaml:"max"`
	Jitter      float64           `yaml:"jitter"`
	MaxElapsed  duration.Duration `yaml:"max_elapsed"`
}

// DLP holds the named redaction rule sets.
type DLP struct {
	RuleSets []dlp.RuleSet `yaml:"rule_sets"`
}

// Source describes one ingest endpoint.
type Source struct {
	ID            string            `yaml:"id"`
	Type          string            `yaml:"type"`
	Parser        string            `yaml:"parser"`
	ParserOptions map[string]string `yaml:"parser_options"`
	Enabled       *bool             `yaml:"enabled"`

	// HTTP, webhook and syslog listeners.
	Listen  string `yaml:"listen"`
	Path    string `yaml:"path"`
	Network string `yaml:"network"`

	// File tailer.
	File         string            `yaml:"file"`
	PollInterval duration.Duration `yaml:"poll_interval"`
	StartAt      string            `yaml:"start_at"`
	OffsetsFile  string            `yaml:"offsets_file"`

	// Webhook verification.
	Secret string `yaml:"secret"`

	// Limits.
	MaxBodyBytes bytesize.Size `yaml:"max_body_bytes"`
	MaxLineBytes int           `yaml:"max_line_bytes"`

	DefaultTags []string `yaml:"default_tags"`
	Category    string   `yaml:"category"`
}

// Sink describes one destination.
type Sink struct {
	ID      string `yaml:"id"`
	Type    string `yaml:"type"`
	Enabled *bool  `yaml:"enabled"`

	// File sink.
	File        string        `yaml:"file"`
	RotateBytes bytesize.Size `yaml:"rotate_bytes"`
	RotateKeep  int           `yaml:"rotate_keep"`

	// HTTP sink.
	URL           string            `yaml:"url"`
	Headers       map[string]string `yaml:"headers"`
	Timeout       duration.Duration `yaml:"timeout"`
	BatchSize     int               `yaml:"batch_size"`
	FlushInterval duration.Duration `yaml:"flush_interval"`

	// Kafka sink.
	Brokers []string `yaml:"brokers"`
	Topic   string   `yaml:"topic"`

	// S3-compatible object store.
	Endpoint     string `yaml:"endpoint"`
	Bucket       string `yaml:"bucket"`
	Region       string `yaml:"region"`
	Prefix       string `yaml:"prefix"`
	AccessKeyEnv string `yaml:"access_key_env"`
	SecretKeyEnv string `yaml:"secret_key_env"`
	UsePathStyle bool   `yaml:"use_path_style"`

	// Shared behaviour.
	BufferSize int         `yaml:"buffer_size"`
	Retry      RetryConfig `yaml:"retry"`
}

// EnabledOrDefault reports whether a source or sink is active.
func enabled(flag *bool) bool { return flag == nil || *flag }

// IsEnabled reports whether the source is active.
func (s Source) IsEnabled() bool { return enabled(s.Enabled) }

// IsEnabled reports whether the sink is active.
func (s Sink) IsEnabled() bool { return enabled(s.Enabled) }

// IsEnabled reports whether the write-ahead log is active.
func (w WALConfig) IsEnabled() bool { return enabled(w.Enabled) }

// IsEnabled reports whether deduplication is active.
func (d DedupConfig) IsEnabled() bool { return enabled(d.Enabled) }

// Load reads, parses and validates a config file.
func Load(path string) (*Config, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("config: open %s: %w", path, err)
	}
	defer func() { _ = file.Close() }()

	cfg := &Config{}
	decoder := yaml.NewDecoder(file)
	decoder.KnownFields(true)
	if err := decoder.Decode(cfg); err != nil {
		return nil, fmt.Errorf("config: parse %s: %w", path, err)
	}
	cfg.applyDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// Parse turns raw YAML into a validated config, used by tests and the API.
func Parse(data []byte) (*Config, error) {
	cfg := &Config{}
	decoder := yaml.NewDecoder(strings.NewReader(string(data)))
	decoder.KnownFields(true)
	if err := decoder.Decode(cfg); err != nil {
		return nil, fmt.Errorf("config: parse: %w", err)
	}
	cfg.applyDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

func (c *Config) applyDefaults() {
	if c.Server.Addr == "" {
		c.Server.Addr = ":8080"
	}
	if c.Server.APIToken == "" && c.Server.APITokenEnv != "" {
		c.Server.APIToken = os.Getenv(c.Server.APITokenEnv)
	}
	if c.Pipeline.Workers <= 0 {
		c.Pipeline.Workers = 4
	}
	if c.Pipeline.QueueSize <= 0 {
		c.Pipeline.QueueSize = 4096
	}
	if c.Pipeline.AcceptTimeout <= 0 {
		c.Pipeline.AcceptTimeout = duration.Duration(5 * 1e9)
	}
	if c.Pipeline.SinkQueueSize <= 0 {
		c.Pipeline.SinkQueueSize = 8192
	}
	if c.Pipeline.ShutdownGrace <= 0 {
		c.Pipeline.ShutdownGrace = duration.Duration(10 * 1e9)
	}
	if c.Pipeline.WAL.Dir == "" {
		c.Pipeline.WAL.Dir = ".streammesh/wal"
	}
	if c.Pipeline.Dedup.TTL <= 0 {
		c.Pipeline.Dedup.TTL = duration.Duration(10 * 60 * 1e9)
	}
	if c.Pipeline.Dedup.Bits <= 0 {
		// 4 Mi bits is 512 KiB and holds roughly ten minutes of traffic at
		// hundreds of thousands of events per second without saturating.
		c.Pipeline.Dedup.Bits = 1 << 22
	}
	if c.Pipeline.Dedup.Hashes <= 0 {
		c.Pipeline.Dedup.Hashes = 3
	}
	if c.Pipeline.Retry.MaxAttempts <= 0 {
		c.Pipeline.Retry.MaxAttempts = 5
	}
	if c.Pipeline.Retry.Base <= 0 {
		c.Pipeline.Retry.Base = duration.Duration(25 * 1e6)
	}
	if c.Pipeline.Retry.Max <= 0 {
		c.Pipeline.Retry.Max = duration.Duration(2 * 1e9)
	}
	for i := range c.Sources {
		src := &c.Sources[i]
		if src.Type == "" {
			src.Type = "http"
		}
		if src.Parser == "" {
			src.Parser = "json"
		}
		if src.PollInterval <= 0 {
			src.PollInterval = duration.Duration(1e9)
		}
		if src.Path == "" {
			switch src.Type {
			case "http":
				src.Path = "/ingest"
			case "webhook":
				src.Path = "/webhook"
			}
		}
		if src.Network == "" {
			src.Network = "udp"
		}
		if src.MaxBodyBytes <= 0 {
			src.MaxBodyBytes = bytesize.Size(8 << 20)
		}
		if src.MaxLineBytes <= 0 {
			src.MaxLineBytes = 1 << 20
		}
	}
	for i := range c.Sinks {
		sink := &c.Sinks[i]
		if sink.Timeout <= 0 {
			sink.Timeout = duration.Duration(5 * 1e9)
		}
		if sink.BatchSize <= 0 {
			sink.BatchSize = 100
		}
		if sink.FlushInterval <= 0 {
			sink.FlushInterval = duration.Duration(1e9)
		}
		if sink.BufferSize <= 0 {
			sink.BufferSize = 8192
		}
		if sink.RotateBytes <= 0 {
			sink.RotateBytes = bytesize.Size(128 << 20)
		}
		if sink.AccessKeyEnv == "" {
			sink.AccessKeyEnv = "AWS_ACCESS_KEY_ID"
		}
		if sink.SecretKeyEnv == "" {
			sink.SecretKeyEnv = "AWS_SECRET_ACCESS_KEY"
		}
		if sink.Type == "s3" && sink.Region == "" {
			sink.Region = "us-east-1"
		}
	}
}

// Validate checks the config for mistakes that would otherwise surface as data
// loss at runtime. It reports every problem it finds, not just the first.
func (c *Config) Validate() error {
	var problems []string

	if len(c.Sources) == 0 {
		problems = append(problems, "at least one source is required")
	}

	sourceIDs := map[string]bool{}
	for i, src := range c.Sources {
		label := fmt.Sprintf("sources[%d]", i)
		if src.ID == "" {
			problems = append(problems, label+": id is required")
		} else {
			label = fmt.Sprintf("source %q", src.ID)
			if sourceIDs[src.ID] {
				problems = append(problems, label+": duplicate id")
			}
			sourceIDs[src.ID] = true
		}
		switch src.Type {
		case "http", "webhook":
			if src.Listen == "" {
				problems = append(problems, label+": listen address is required")
			}
		case "syslog":
			if src.Listen == "" {
				problems = append(problems, label+": listen address is required")
			}
			if src.Network != "udp" && src.Network != "tcp" {
				problems = append(problems, label+`: network must be "udp" or "tcp"`)
			}
		case "file":
			if src.File == "" {
				problems = append(problems, label+": file path is required")
			}
			if src.StartAt != "" && src.StartAt != "start" && src.StartAt != "end" {
				problems = append(problems, label+`: start_at must be "start" or "end"`)
			}
		default:
			problems = append(problems, label+fmt.Sprintf(": unknown type %q", src.Type))
		}
		if _, err := parse.Build(src.Parser, src.ParserOptions); err != nil {
			problems = append(problems, label+": "+err.Error())
		}
	}

	sinkIDs := map[string]bool{}
	for i, sink := range c.Sinks {
		label := fmt.Sprintf("sinks[%d]", i)
		if sink.ID == "" {
			problems = append(problems, label+": id is required")
		} else {
			label = fmt.Sprintf("sink %q", sink.ID)
			if sinkIDs[sink.ID] {
				problems = append(problems, label+": duplicate id")
			}
			sinkIDs[sink.ID] = true
		}
		switch sink.Type {
		case "file":
			if sink.File == "" {
				problems = append(problems, label+": file path is required")
			}
		case "http":
			if sink.URL == "" {
				problems = append(problems, label+": url is required")
			}
		case "kafka":
			if len(sink.Brokers) == 0 {
				problems = append(problems, label+": at least one broker is required")
			}
			if sink.Topic == "" {
				problems = append(problems, label+": topic is required")
			}
		case "s3":
			if sink.Bucket == "" {
				problems = append(problems, label+": bucket is required")
			}
		case "memory":
			// Test-only sink, nothing to validate.
		default:
			problems = append(problems, label+fmt.Sprintf(": unknown type %q", sink.Type))
		}
	}

	for _, id := range c.Pipeline.DefaultSinks {
		if !sinkIDs[id] {
			problems = append(problems, fmt.Sprintf("pipeline.default_sinks: unknown sink %q", id))
		}
	}

	for i, rule := range c.Rules {
		name := rule.Name
		if name == "" {
			name = fmt.Sprintf("rules[%d]", i)
		}
		for _, action := range rule.Actions {
			for _, id := range action.Route {
				if !sinkIDs[id] {
					problems = append(problems, fmt.Sprintf("rule %q: routes to unknown sink %q", name, id))
				}
			}
		}
	}

	if len(problems) > 0 {
		return fmt.Errorf("config: invalid configuration:\n  - %s", strings.Join(problems, "\n  - "))
	}
	return nil
}

// SourceIDs lists the configured source IDs.
func (c *Config) SourceIDs() []string {
	ids := make([]string, 0, len(c.Sources))
	for _, src := range c.Sources {
		ids = append(ids, src.ID)
	}
	return ids
}

// SinkIDs lists the configured sink IDs.
func (c *Config) SinkIDs() []string {
	ids := make([]string, 0, len(c.Sinks))
	for _, sink := range c.Sinks {
		ids = append(ids, sink.ID)
	}
	return ids
}
