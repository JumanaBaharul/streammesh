package parse

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/JumanaBaharul/streammesh/internal/event"
)

// Syslog parses RFC5424 and RFC3164 lines.
type Syslog struct{}

// Name implements Parser.
func (Syslog) Name() string { return "syslog" }

// Parse implements Parser.
func (Syslog) Parse(source string, data []byte) ([]event.Event, error) {
	var events []event.Event
	var firstErr error
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimRight(line, "\r")
		if strings.TrimSpace(line) == "" {
			continue
		}
		ev, err := ParseSyslogLine(source, line)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		events = append(events, ev)
	}
	// Good lines are returned alongside the error so a caller keeps the data and
	// counts the rejection separately.
	return events, firstErr
}

// ParseSyslogLine parses one syslog message. It never panics on malformed
// input: unparseable lines degrade into a raw message event rather than an
// error, because a syslog stream is inherently best-effort.
func ParseSyslogLine(source, line string) (event.Event, error) {
	ev := event.New(source)
	rest := strings.TrimSpace(line)
	if rest == "" {
		return ev, fmt.Errorf("parse: empty syslog line")
	}

	// Priority: <NNN>, where severity is pri%8 and facility is pri/8.
	if strings.HasPrefix(rest, "<") {
		if end := strings.IndexByte(rest, '>'); end > 1 && end <= 4 {
			if pri, err := strconv.Atoi(rest[1:end]); err == nil && pri >= 0 && pri <= 191 {
				ev.Severity = syslogSeverity(pri % 8)
				ev.Attrs["facility"] = strconv.Itoa(pri / 8)
				if facility, ok := facilityNames[pri/8]; ok {
					ev.Attrs["facility_name"] = facility
				}
				rest = rest[end+1:]
			}
		}
	}

	// RFC5424 starts with a version number followed by a space.
	if len(rest) > 1 && rest[0] >= '1' && rest[0] <= '9' && rest[1] == ' ' {
		return parseRFC5424(ev, rest[2:]), nil
	}
	return parseRFC3164(ev, rest), nil
}

func parseRFC5424(ev event.Event, rest string) event.Event {
	fields := strings.SplitN(rest, " ", 5)
	if len(fields) > 0 && fields[0] != "-" {
		if ts, ok := ParseTimeValue(fields[0]); ok {
			ev.Timestamp = ts
		}
	}
	if len(fields) > 1 && fields[1] != "-" {
		ev.Host = fields[1]
	}
	if len(fields) > 2 && fields[2] != "-" {
		ev.Category = fields[2]
	}
	if len(fields) > 3 && fields[3] != "-" {
		ev.Attrs["procid"] = fields[3]
	}
	if len(fields) > 4 {
		msgid, structured, message := splitStructuredData(fields[4])
		if msgid != "" && msgid != "-" {
			ev.Attrs["msgid"] = msgid
		}
		if structured != "" {
			ev.Attrs["structured_data"] = structured
		}
		if message != "" {
			ev.Attrs["message"] = message
		}
	}
	ev.Normalize()
	return ev
}

// splitStructuredData separates MSGID, the bracketed structured-data block and
// the free-form message. Brackets nest, so a naive split would corrupt JSON
// payloads that contain "]".
func splitStructuredData(rest string) (string, string, string) {
	msgid := ""
	rest = strings.TrimLeft(rest, " ")
	if next := strings.IndexByte(rest, ' '); next >= 0 {
		msgid = rest[:next]
		rest = strings.TrimLeft(rest[next+1:], " ")
	} else {
		return rest, "", ""
	}

	var structured strings.Builder
	if strings.HasPrefix(rest, "[") {
		depth := 0
		for i := 0; i < len(rest); i++ {
			switch rest[i] {
			case '[':
				depth++
			case ']':
				depth--
			}
			if i < len(rest) {
				structured.WriteByte(rest[i])
			}
			if depth == 0 && i > 0 {
				rest = rest[i+1:]
				goto done
			}
		}
		return msgid, structured.String(), ""
	} else if strings.HasPrefix(rest, "-") {
		rest = rest[1:]
	}
done:
	return msgid, structured.String(), strings.TrimLeft(rest, " ")
}

func parseRFC3164(ev event.Event, rest string) event.Event {
	fields := strings.SplitN(rest, " ", 4)
	// "Jan _2 15:04:05" is the classic timestamp; it contains a space, so the
	// first three tokens belong together.
	if len(fields) >= 3 {
		candidate := strings.Join(fields[:3], " ")
		if ts, ok := parseSyslogTime(candidate); ok {
			ev.Timestamp = ts
			rest = strings.TrimLeft(strings.Join(fields[3:], " "), " ")
			fields = strings.SplitN(rest, " ", 2)
		}
	}
	if len(fields) >= 2 {
		ev.Host = fields[0]
		rest = fields[1]
	} else if len(fields) == 1 {
		rest = fields[0]
	}

	// "app[1234]: message" or "app: message", or plain "message".
	if tag, message, ok := strings.Cut(rest, ": "); ok && !strings.Contains(tag, " ") {
		ev.Category = tag
		if open := strings.IndexByte(tag, '['); open > 0 && strings.HasSuffix(tag, "]") {
			ev.Category = tag[:open]
			ev.Attrs["procid"] = strings.TrimSuffix(tag[open+1:], "]")
		}
		rest = message
	}
	if rest != "" {
		ev.Attrs["message"] = rest
	}
	ev.Normalize()
	return ev
}

func parseSyslogTime(value string) (time.Time, bool) {
	layouts := []string{
		"Jan _2 15:04:05",
		"Jan 2 15:04:05",
		"Jan _2 15:04:05.000",
		"2006-01-02T15:04:05Z07:00",
	}
	for _, layout := range layouts {
		if parsed, err := time.Parse(layout, strings.TrimSpace(value)); err == nil {
			// Classic syslog omits the year; assume the current one.
			now := time.Now().UTC()
			parsed = parsed.AddDate(now.Year(), 0, 0)
			if parsed.After(now.Add(24 * time.Hour)) {
				parsed = parsed.AddDate(-1, 0, 0)
			}
			return parsed.UTC(), true
		}
	}
	return time.Time{}, false
}

// syslogSeverity converts a syslog severity (0=emergency .. 7=debug) into the
// canonical, higher-is-worse scale.
func syslogSeverity(code int) event.Severity {
	switch code {
	case 0:
		return event.SeverityEmergency
	case 1:
		return event.SeverityAlert
	case 2:
		return event.SeverityCritical
	case 3:
		return event.SeverityError
	case 4:
		return event.SeverityWarning
	case 5:
		return event.SeverityNotice
	case 6:
		return event.SeverityInfo
	default:
		return event.SeverityDebug
	}
}

var facilityNames = map[int]string{
	0: "kern", 1: "user", 2: "mail", 3: "daemon", 4: "auth", 5: "syslog",
	6: "lpr", 7: "news", 8: "uucp", 9: "cron", 10: "authpriv", 11: "ftp",
	16: "local0", 17: "local1", 18: "local2", 19: "local3",
	20: "local4", 21: "local5", 22: "local6", 23: "local7",
}
