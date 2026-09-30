package sink

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/JumanaBaharul/streammesh/internal/event"
)

// s3Sink archives batches as gzipped newline-delimited JSON in an
// S3-compatible bucket, partitioned by date and hour so an operator can find a
// ten-minute window without scanning the whole bucket.
//
// The AWS SigV4 signature is implemented here rather than pulled in as an SDK:
// it is about eighty lines of HMAC chaining, and it keeps a network archive
// destination from dragging a multi-megabyte dependency tree into the build.
type s3Sink struct {
	*Base
	client    *http.Client
	endpoint  string
	bucket    string
	region    string
	prefix    string
	accessKey string
	secretKey string
	pathStyle bool
	sequence  atomic.Uint64
}

func newS3Sink(base *Base) (Sink, error) {
	if base.cfg.Bucket == "" {
		return nil, fmt.Errorf("sink %q: bucket is required", base.cfg.ID)
	}
	region := base.cfg.Region
	if region == "" {
		region = "us-east-1"
	}

	// Fall back to the conventional names so the sink works regardless of
	// whether it was configured from a file or constructed directly.
	accessKeyEnv := base.cfg.AccessKeyEnv
	if accessKeyEnv == "" {
		accessKeyEnv = "AWS_ACCESS_KEY_ID"
	}
	secretKeyEnv := base.cfg.SecretKeyEnv
	if secretKeyEnv == "" {
		secretKeyEnv = "AWS_SECRET_ACCESS_KEY"
	}

	accessKey := os.Getenv(accessKeyEnv)
	secretKey := os.Getenv(secretKeyEnv)
	if accessKey == "" || secretKey == "" {
		return nil, fmt.Errorf("sink %q: credentials are required in %s and %s",
			base.cfg.ID, accessKeyEnv, secretKeyEnv)
	}

	endpoint := strings.TrimRight(base.cfg.Endpoint, "/")
	if endpoint == "" {
		endpoint = fmt.Sprintf("https://s3.%s.amazonaws.com", region)
	}
	// S3-compatible servers such as MinIO need path-style addressing; AWS
	// itself prefers virtual-hosted style.
	pathStyle := base.cfg.UsePathStyle || !strings.Contains(endpoint, "amazonaws.com")

	timeout := base.cfg.Timeout.Duration()
	if timeout <= 0 {
		timeout = 15 * time.Second
	}

	return &s3Sink{
		Base:      base,
		client:    &http.Client{Timeout: timeout},
		endpoint:  endpoint,
		bucket:    base.cfg.Bucket,
		region:    region,
		prefix:    strings.Trim(base.cfg.Prefix, "/"),
		accessKey: accessKey,
		secretKey: secretKey,
		pathStyle: pathStyle,
	}, nil
}

func (s *s3Sink) Write(ctx context.Context, events []event.Event) error {
	if len(events) == 0 {
		return nil
	}
	payload, err := encodeBatch(events)
	if err != nil {
		s.noteFailure(err)
		return err
	}

	var compressed bytes.Buffer
	gzipWriter := gzip.NewWriter(&compressed)
	if _, err := gzipWriter.Write(payload); err != nil {
		s.noteFailure(err)
		return fmt.Errorf("sink %q: compress: %w", s.cfg.ID, err)
	}
	if err := gzipWriter.Close(); err != nil {
		s.noteFailure(err)
		return fmt.Errorf("sink %q: compress: %w", s.cfg.ID, err)
	}

	now := time.Now().UTC()
	key := s.objectKey(now)

	var target string
	if s.pathStyle {
		target = fmt.Sprintf("%s/%s/%s", s.endpoint, s.bucket, key)
	} else {
		parsed, err := url.Parse(s.endpoint)
		if err != nil {
			s.noteFailure(err)
			return fmt.Errorf("sink %q: parse endpoint: %w", s.cfg.ID, err)
		}
		parsed.Host = s.bucket + "." + parsed.Host
		parsed.Path = "/" + key
		target = parsed.String()
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodPut, target, bytes.NewReader(compressed.Bytes()))
	if err != nil {
		s.noteFailure(err)
		return fmt.Errorf("sink %q: build request: %w", s.cfg.ID, err)
	}
	request.Header.Set("Content-Encoding", "gzip")
	request.Header.Set("Content-Type", "application/x-ndjson")
	request.ContentLength = int64(compressed.Len())

	signV4(request, compressed.Bytes(), s.region, "s3", s.accessKey, s.secretKey, now)

	response, err := s.client.Do(request)
	if err != nil {
		s.noteFailure(err)
		return &RetryableError{Err: fmt.Errorf("sink %q: put: %w", s.cfg.ID, err)}
	}
	defer func() { _ = response.Body.Close() }()
	body, _ := io.ReadAll(io.LimitReader(response.Body, 4096))

	if response.StatusCode >= 200 && response.StatusCode < 300 {
		s.noteSuccess(events, compressed.Len())
		return nil
	}
	httpErr := fmt.Errorf("sink %q: status %d: %s", s.cfg.ID, response.StatusCode, truncate(string(body), 200))
	s.noteFailure(httpErr)
	if response.StatusCode >= 500 || response.StatusCode == http.StatusTooManyRequests {
		return &RetryableError{Err: httpErr}
	}
	return httpErr
}

func (s *s3Sink) objectKey(now time.Time) string {
	segments := []string{}
	if s.prefix != "" {
		segments = append(segments, s.prefix)
	}
	segments = append(segments,
		fmt.Sprintf("dt=%s", now.Format("2006-01-02")),
		fmt.Sprintf("hour=%s", now.Format("15")),
		fmt.Sprintf("batch-%d-%04d.ndjson.gz", now.UnixNano(), s.sequence.Add(1)),
	)
	return path.Join(segments...)
}

func (s *s3Sink) Close(context.Context) error {
	s.client.CloseIdleConnections()
	return nil
}

// canonicalRequest builds the SigV4 canonical request string.
func canonicalRequest(req *http.Request, payloadHash, signedHeaders, canonicalHeaders string) string {
	query := ""
	if req.URL.RawQuery != "" {
		query = canonicalQueryString(req.URL)
	}
	return strings.Join([]string{
		req.Method,
		req.URL.EscapedPath(),
		query,
		canonicalHeaders,
		signedHeaders,
		payloadHash,
	}, "\n")
}

func canonicalQueryString(u *url.URL) string {
	values := u.Query()
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var parts []string
	for _, key := range keys {
		items := values[key]
		sort.Strings(items)
		for _, item := range items {
			parts = append(parts, url.QueryEscape(key)+"="+url.QueryEscape(item))
		}
	}
	return strings.Join(parts, "&")
}

// signV4 signs a request in place using AWS Signature Version 4.
func signV4(req *http.Request, payload []byte, region, service, accessKey, secretKey string, now time.Time) {
	amzDate := now.UTC().Format("20060102T150405Z")
	dateStamp := now.UTC().Format("20060102")
	payloadHash := sha256Hex(payload)

	req.Header.Set("x-amz-date", amzDate)
	req.Header.Set("x-amz-content-sha256", payloadHash)

	host := req.Host
	if host == "" {
		host = req.URL.Host
	}
	canonicalHeaders := "host:" + host + "\n" +
		"x-amz-content-sha256:" + payloadHash + "\n" +
		"x-amz-date:" + amzDate + "\n"
	signedHeaders := "host;x-amz-content-sha256;x-amz-date"

	canonical := canonicalRequest(req, payloadHash, signedHeaders, canonicalHeaders)
	scope := strings.Join([]string{dateStamp, region, service, "aws4_request"}, "/")
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256",
		amzDate,
		scope,
		sha256Hex([]byte(canonical)),
	}, "\n")

	signingKey := deriveSigningKey(secretKey, dateStamp, region, service)
	signature := hex.EncodeToString(hmacSHA256(signingKey, []byte(stringToSign)))

	req.Header.Set("Authorization", fmt.Sprintf(
		"AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		accessKey, scope, signedHeaders, signature,
	))
}

func deriveSigningKey(secretKey, dateStamp, region, service string) []byte {
	dateKey := hmacSHA256([]byte("AWS4"+secretKey), []byte(dateStamp))
	regionKey := hmacSHA256(dateKey, []byte(region))
	serviceKey := hmacSHA256(regionKey, []byte(service))
	return hmacSHA256(serviceKey, []byte("aws4_request"))
}

func hmacSHA256(key, data []byte) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write(data)
	return mac.Sum(nil)
}

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
