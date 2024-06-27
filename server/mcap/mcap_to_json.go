package mcap

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	fmcap "github.com/foxglove/mcap/go/mcap"
	"github.com/wkalt/dp3/server/util/ros1msg"
	"github.com/wkalt/dp3/server/util/schema"
)

func digits(n uint64) int {
	if n == 0 {
		return 1
	}
	count := 0
	for n != 0 {
		n /= 10
		count++
	}
	return count
}

func formatDecimalTime(buf []byte, t uint64) []byte {
	buf = buf[:0]
	seconds := t / 1e9
	nanoseconds := t % 1e9
	buf = strconv.AppendInt(buf, int64(seconds), 10)
	buf = append(buf, '.')
	for i := 0; i < 9-digits(nanoseconds); i++ {
		buf = append(buf, '0')
	}
	buf = strconv.AppendInt(buf, int64(nanoseconds), 10)
	return buf
}

func writeMessage(
	w io.Writer,
	topic string,
	sequence uint32,
	logTime uint64,
	publishTime uint64,
	data []byte,
	buf []byte,
) error {
	if _, err := w.Write([]byte(`{"topic":"` + topic + `","sequence":`)); err != nil {
		return fmt.Errorf("failed to write topic: %w", err)
	}
	if _, err := w.Write([]byte(strconv.FormatUint(uint64(sequence), 10))); err != nil {
		return fmt.Errorf("failed to write sequence: %w", err)
	}
	if _, err := w.Write([]byte(`,"log_time":`)); err != nil {
		return fmt.Errorf("failed to write log time: %w", err)
	}
	if _, err := w.Write(formatDecimalTime(buf, logTime)); err != nil {
		return fmt.Errorf("failed to write log time: %w", err)
	}
	if _, err := w.Write([]byte(`,"publish_time":`)); err != nil {
		return fmt.Errorf("failed to write publish time: %w", err)
	}
	if _, err := w.Write(formatDecimalTime(buf, publishTime)); err != nil {
		return fmt.Errorf("failed to write publish time: %w", err)
	}
	if _, err := w.Write([]byte(`,"data":`)); err != nil {
		return fmt.Errorf("failed to write data: %w", err)
	}
	if _, err := w.Write(data); err != nil {
		return fmt.Errorf("failed to write data: %w", err)
	}
	if _, err := w.Write([]byte("}\n")); err != nil {
		return fmt.Errorf("failed to write closing brace: %w", err)
	}
	return nil
}

func MCAPToJSON(
	w io.Writer,
	r io.Reader,
) error {
	reader, err := NewReader(bufio.NewReader(r))
	if err != nil {
		return fmt.Errorf("failed to create reader: %w", err)
	}
	it, err := reader.Messages(fmcap.UsingIndex(false), fmcap.InOrder(fmcap.FileOrder))
	if err != nil {
		return fmt.Errorf("failed to read messages: %w", err)
	}
	msg := &bytes.Buffer{}
	decoder := ros1msg.NewDecoder(nil)
	transcoders := make(map[uint16]*schema.JSONTranscoder)
	buf := make([]byte, 30)
	m := &fmcap.Message{}
	for {
		s, c, m, err := it.NextInto(m)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("failed to read next message: %w", err)
		}
		switch s.Encoding {
		case "ros1msg":
			transcoder, ok := transcoders[c.SchemaID]
			if !ok {
				packageName := strings.Split(s.Name, "/")[0]
				parsed, err := ros1msg.ParseROS1MessageDefinition(packageName, s.Name, s.Data)
				if err != nil {
					return fmt.Errorf("failed to parse %s: %w", s.Name, err)
				}
				transcoder, err = schema.NewJSONTranscoder(parsed, decoder)
				if err != nil {
					return fmt.Errorf("failed to create transcoder for %s: %w", s.Name, err)
				}
				transcoders[c.SchemaID] = transcoder
			}
			if err := transcoder.Transcode(msg, m.Data); err != nil {
				return fmt.Errorf("failed to transcode %s record on %s: %w", s.Name, c.Topic, err)
			}
		default:
			return fmt.Errorf("JSON output only supported for ros1msg, protobuf, and jsonschema schemas. Found: %s",
				s.Encoding,
			)
		}
		if err := writeMessage(w,
			c.Topic,
			m.Sequence,
			m.LogTime,
			m.PublishTime,
			msg.Bytes(),
			buf,
		); err != nil {
			return fmt.Errorf("failed to write encoded message: %w", err)
		}
		msg.Reset()
	}
	return nil
}
