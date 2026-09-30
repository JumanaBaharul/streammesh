package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const minimalConfig = `
server:
  addr: ":9999"
sources:
  - id: ingest
    type: http
    listen: ":18081"
sinks:
  - id: archive
    type: file
    file: ./out/events.ndjson
rules:
  - name: everything
    match:
      always: true
    actions:
      - route: [archive]
`

func TestParseAppliesDefaults(t *testing.T) {
	cfg, err := Parse([]byte(minimalConfig))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if cfg.Pipeline.Workers != 4 {
		t.Errorf("workers = %d, want the default 4", cfg.Pipeline.Workers)
	}
	if cfg.Pipeline.QueueSize != 4096 {
		t.Errorf("queue size = %d, want the default 4096", cfg.Pipeline.QueueSize)
	}
	if cfg.Pipeline.AcceptTimeout.Duration() != 5*time.Second {
		t.Errorf("accept timeout = %s", cfg.Pipeline.AcceptTimeout.Duration())
	}
	if cfg.Pipeline.Dedup.Bits != 1<<22 {
		t.Errorf("dedup bits = %d, want the default 4 Mi bits", cfg.Pipeline.Dedup.Bits)
	}
	if cfg.Pipeline.Dedup.Hashes != 3 {
		t.Errorf("dedup hashes = %d, want 3", cfg.Pipeline.Dedup.Hashes)
	}
	if cfg.Sources[0].Path != "/ingest" {
		t.Errorf("source path = %q, want /ingest", cfg.Sources[0].Path)
	}
	if cfg.Sources[0].Parser != "json" {
		t.Errorf("source parser = %q, want json", cfg.Sources[0].Parser)
	}
	if cfg.Sources[0].MaxBodyBytes.Int64() != 8<<20 {
		t.Errorf("source max body = %d, want 8 MiB", cfg.Sources[0].MaxBodyBytes.Int64())
	}
	if cfg.Sinks[0].BatchSize != 100 {
		t.Errorf("sink batch size = %d, want 100", cfg.Sinks[0].BatchSize)
	}
	if cfg.Sinks[0].Timeout.Duration() != 5*time.Second {
		t.Errorf("sink timeout = %s", cfg.Sinks[0].Timeout.Duration())
	}
	// The write-ahead log defaults to enabled, which is the durability promise.
	if !cfg.Pipeline.WAL.IsEnabled() {
		t.Error("the write-ahead log should be enabled by default")
	}
}

func TestParseRejectsUnknownKeys(t *testing.T) {
	_, err := Parse([]byte(`
sources:
  - id: ingest
    type: http
    listen: ":18081"
    listne: typo
`))
	if err == nil {
		t.Fatal("expected an unknown key to be rejected")
	}
	if !strings.Contains(err.Error(), "listne") {
		t.Errorf("error %q does not name the offending key", err)
	}
}

func TestValidateReportsEveryProblemAtOnce(t *testing.T) {
	_, err := Parse([]byte(`
sources:
  - id: ""
    type: carrier-pigeon
    parser: nonsense
  - id: dup
    type: http
    listen: ":1"
  - id: dup
    type: http
sinks:
  - id: archive
    type: file
  - id: archive
    type: http
rules:
  - name: broken-route
    match:
      always: true
    actions:
      - route: [does-not-exist]
`))
	if err == nil {
		t.Fatal("expected validation to fail")
	}

	message := err.Error()
	for _, want := range []string{
		"id is required",
		"unknown type",
		"unknown parser",
		"duplicate id",
		"listen address is required",
		"file path is required",
		"url is required",
		`unknown sink "does-not-exist"`,
	} {
		if !strings.Contains(message, want) {
			t.Errorf("validation output is missing %q\n%s", want, message)
		}
	}
}

func TestValidateCatchesRoutingAndParserMistakes(t *testing.T) {
	cases := []struct {
		name   string
		config string
		want   string
	}{
		{
			name: "unknown default sink",
			config: `
pipeline:
  default_sinks: [ghost]
sources:
  - id: s
    type: http
    listen: ":1"
sinks:
  - id: real
    type: memory
`,
			want: `unknown sink "ghost"`,
		},
		{
			name: "regex parser without a pattern",
			config: `
sources:
  - id: s
    type: http
    listen: ":1"
    parser: regex
sinks:
  - id: real
    type: memory
`,
			want: "requires a pattern",
		},
		{
			name: "file source without a path",
			config: `
sources:
  - id: s
    type: file
sinks:
  - id: real
    type: memory
`,
			want: "file path is required",
		},
		{
			name: "syslog with an unsupported network",
			config: `
sources:
  - id: s
    type: syslog
    listen: ":5514"
    network: carrier-pigeon
sinks:
  - id: real
    type: memory
`,
			want: "network must be",
		},
		{
			name: "bad start_at",
			config: `
sources:
  - id: s
    type: file
    file: ./app.log
    start_at: middle
sinks:
  - id: real
    type: memory
`,
			want: "start_at must be",
		},
		{
			name: "kafka without a topic",
			config: `
sources:
  - id: s
    type: http
    listen: ":1"
sinks:
  - id: k
    type: kafka
    brokers: ["localhost:9092"]
`,
			want: "topic is required",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse([]byte(tc.config))
			if err == nil {
				t.Fatal("expected validation to fail")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q does not mention %q", err, tc.want)
			}
		})
	}
}

func TestValidateRequiresAtLeastOneSource(t *testing.T) {
	_, err := Parse([]byte("sinks:\n  - id: a\n    type: memory\n"))
	if err == nil || !strings.Contains(err.Error(), "at least one source") {
		t.Fatalf("err = %v, want a missing-source error", err)
	}
}

func TestDisabledConnectorsAreSkippedButStillValidated(t *testing.T) {
	cfg, err := Parse([]byte(`
sources:
  - id: ingest
    type: http
    listen: ":18081"
  - id: parked
    type: http
    listen: ":18082"
    enabled: false
sinks:
  - id: recent
    type: memory
  - id: future
    type: kafka
    enabled: false
    brokers: ["localhost:9092"]
    topic: events
`))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if !cfg.Sources[0].IsEnabled() {
		t.Error("the first source should be enabled")
	}
	if cfg.Sources[1].IsEnabled() {
		t.Error("an explicitly disabled source should not be enabled")
	}
	if cfg.Sinks[1].IsEnabled() {
		t.Error("an explicitly disabled sink should not be enabled")
	}
}

func TestByteSizesAndDurationsParseFromReadableStrings(t *testing.T) {
	cfg, err := Parse([]byte(`
sources:
  - id: ingest
    type: http
    listen: ":18081"
    max_body_bytes: 2MB
sinks:
  - id: archive
    type: file
    file: ./out/events.ndjson
    rotate_bytes: 3GiB
    flush_interval: 250ms
pipeline:
  wal:
    segment_bytes: 16MB
    fsync_interval: 1.5s
`))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if got := cfg.Sources[0].MaxBodyBytes.Int64(); got != 2<<20 {
		t.Errorf("max body = %d, want %d", got, 2<<20)
	}
	if got := cfg.Sinks[0].RotateBytes.Int64(); got != 3<<30 {
		t.Errorf("rotate bytes = %d, want %d", got, 3<<30)
	}
	if got := cfg.Sinks[0].FlushInterval.Duration(); got != 250*time.Millisecond {
		t.Errorf("flush interval = %s", got)
	}
	if got := cfg.Pipeline.WAL.SegmentBytes.Int64(); got != 16<<20 {
		t.Errorf("segment bytes = %d", got)
	}
	if got := cfg.Pipeline.WAL.FsyncInterval.Duration(); got != 1500*time.Millisecond {
		t.Errorf("fsync interval = %s", got)
	}
}

func TestInvalidByteSizeIsRejected(t *testing.T) {
	_, err := Parse([]byte(`
sources:
  - id: ingest
    type: http
    listen: ":18081"
    max_body_bytes: plenty
sinks:
  - id: recent
    type: memory
`))
	if err == nil {
		t.Fatal("expected an unparseable size to fail")
	}
	if !strings.Contains(err.Error(), "invalid size") {
		t.Errorf("error %q does not explain the size problem", err)
	}
}

func TestLoadReadsFromDiskAndReportsMissingfiles(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "pipelines.yaml")
	if err := os.WriteFile(path, []byte(minimalConfig), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Server.Addr != ":9999" {
		t.Errorf("addr = %q", cfg.Server.Addr)
	}

	if _, err := Load(filepath.Join(dir, "missing.yaml")); err == nil {
		t.Fatal("expected loading a missing file to fail")
	}
}

func TestAPITokenCanComeFromTheEnvironment(t *testing.T) {
	t.Setenv("STREAMMESH_TEST_TOKEN", "s3cret")
	cfg, err := Parse([]byte(`
server:
  api_token_env: STREAMMESH_TEST_TOKEN
sources:
  - id: ingest
    type: http
    listen: ":18081"
sinks:
  - id: recent
    type: memory
`))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if cfg.Server.APIToken != "s3cret" {
		t.Fatalf("api token = %q, want the environment value", cfg.Server.APIToken)
	}
}

func TestTheShippedExamplesAreValid(t *testing.T) {
	for _, name := range []string{"pipelines.yaml", "demo.yaml"} {
		path := filepath.Join("..", "..", "examples", name)
		cfg, err := Load(path)
		if err != nil {
			t.Errorf("%s is not valid: %v", name, err)
			continue
		}
		if len(cfg.Sources) == 0 || len(cfg.Sinks) == 0 || len(cfg.Rules) == 0 {
			t.Errorf("%s is missing sources, sinks or rules", name)
		}
	}
}
