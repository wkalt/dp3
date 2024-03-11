/*
 MIT License

 Copyright (c) Foxglove Technologies Inc

 Permission is hereby granted, free of charge, to any person obtaining a copy
 of this software and associated documentation files (the "Software"), to deal
 in the Software without restriction, including without limitation the rights
 to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 copies of the Software, and to permit persons to whom the Software is
 furnished to do so, subject to the following conditions:

 The above copyright notice and this permission notice shall be included in all
 copies or substantial portions of the Software.

 THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 SOFTWARE.

Lightly modified from https://github.com/foxglove/mcap/blob/main/go/cli/mcap/cmd/cat.go.
*/

package util

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	ros "github.com/foxglove/go-rosbag/ros1msg"
	fmcap "github.com/foxglove/mcap/go/mcap"
	"github.com/wkalt/dp3/mcap"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
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

func formatDecimalTime(t uint64) []byte {
	seconds := t / 1e9
	nanoseconds := t % 1e9
	requiredLength := digits(seconds) + 1 + 9
	buf := make([]byte, 0, requiredLength)
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
) error {
	if _, err := w.Write([]byte(`{"topic":"` + topic + `","sequence":`)); err != nil {
		return err
	}
	if _, err := w.Write([]byte(strconv.FormatUint(uint64(sequence), 10))); err != nil {
		return err
	}
	if _, err := w.Write([]byte(`,"log_time":`)); err != nil {
		return err
	}
	if _, err := w.Write(formatDecimalTime(logTime)); err != nil {
		return err
	}
	if _, err := w.Write([]byte(`,"publish_time":`)); err != nil {
		return err
	}
	if _, err := w.Write(formatDecimalTime(publishTime)); err != nil {
		return err
	}
	if _, err := w.Write([]byte(`,"data":`)); err != nil {
		return err
	}
	if _, err := w.Write(data); err != nil {
		return err
	}
	if _, err := w.Write([]byte("}\n")); err != nil {
		return err
	}
	return nil
}

func MCAPToJSON(
	w io.Writer,
	r io.Reader,
) error {
	bw := bufio.NewWriter(w)
	defer bw.Flush()
	reader, err := mcap.NewReader(r)
	if err != nil {
		return fmt.Errorf("failed to create reader: %w", err)
	}
	it, err := reader.Messages(fmcap.UsingIndex(false), fmcap.InOrder(fmcap.FileOrder))
	if err != nil {
		return fmt.Errorf("failed to read messages: %w", err)
	}
	msg := &bytes.Buffer{}
	msgReader := &bytes.Reader{}
	buf := make([]byte, 1024*1024)
	transcoders := make(map[uint16]*ros.JSONTranscoder)
	descriptors := make(map[uint16]protoreflect.MessageDescriptor)
	for {
		schema, channel, message, err := it.Next(buf)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("failed to read next message: %w", err)
		}
		if schema == nil || schema.Encoding == "" {
			switch channel.MessageEncoding {
			case "json":
				if _, err = msg.Write(message.Data); err != nil {
					return fmt.Errorf("failed to write message bytes: %w", err)
				}
			default:
				return fmt.Errorf(
					"for schema-less channels, JSON output is only supported with 'json' message encoding. found: %s",
					channel.MessageEncoding,
				)
			}
		} else {
			switch schema.Encoding {
			case "ros1msg":
				transcoder, ok := transcoders[channel.SchemaID]
				if !ok {
					packageName := strings.Split(schema.Name, "/")[0]
					transcoder, err = ros.NewJSONTranscoder(packageName, schema.Data)
					if err != nil {
						return fmt.Errorf("failed to build transcoder for %s: %w", channel.Topic, err)
					}
					transcoders[channel.SchemaID] = transcoder
				}
				msgReader.Reset(message.Data)
				err = transcoder.Transcode(msg, msgReader)
				if err != nil {
					return fmt.Errorf("failed to transcode %s record on %s: %w", schema.Name, channel.Topic, err)
				}
			case "protobuf":
				messageDescriptor, ok := descriptors[channel.SchemaID]
				if !ok {
					fileDescriptorSet := &descriptorpb.FileDescriptorSet{}
					if err := proto.Unmarshal(schema.Data, fileDescriptorSet); err != nil {
						return fmt.Errorf("failed to build file descriptor set: %w", err)
					}
					files, err := protodesc.FileOptions{}.NewFiles(fileDescriptorSet)
					if err != nil {
						return fmt.Errorf("failed to create file descriptor: %w", err)
					}
					descriptor, err := files.FindDescriptorByName(protoreflect.FullName(schema.Name))
					if err != nil {
						return fmt.Errorf("failed to find descriptor: %w", err)
					}
					messageDescriptor = descriptor.(protoreflect.MessageDescriptor)
					descriptors[channel.SchemaID] = messageDescriptor
				}
				protoMsg := dynamicpb.NewMessage(messageDescriptor)
				if err := proto.Unmarshal(message.Data, protoMsg); err != nil {
					return fmt.Errorf("failed to parse message: %w", err)
				}
				bytes, err := protojson.Marshal(protoMsg)
				if err != nil {
					return fmt.Errorf("failed to marshal message: %w", err)
				}
				if _, err = msg.Write(bytes); err != nil {
					return fmt.Errorf("failed to write message bytes: %w", err)
				}
			case "jsonschema":
				if _, err = msg.Write(message.Data); err != nil {
					return fmt.Errorf("failed to write message bytes: %w", err)
				}
			default:
				return fmt.Errorf(
					"JSON output only supported for ros1msg, protobuf, and jsonschema schemas. Found: %s",
					schema.Encoding,
				)
			}
		}
		if err := writeMessage(
			bw,
			channel.Topic,
			message.Sequence,
			message.LogTime,
			message.PublishTime,
			msg.Bytes(),
		); err != nil {
			return fmt.Errorf("failed to write encoded message: %w", err)
		}
		msg.Reset()
	}
	return nil
}
