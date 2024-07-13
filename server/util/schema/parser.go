package schema

import (
	"encoding/binary"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/wkalt/dp3/server/util"
)

/*
This file implements a generic parser for our supported binary encoding formats,
using a stack-based bytecode VM. During parsing, a reader will create one Parser
per message schema encountered. During construction, the target schema and the
requested set of fields are compiled into a stack of bytecodes, which are then
repeatedly passed to the parser. The bytecodes express parse and skip operations
on fields and encode a sequence of operations to extract just the requested
fields/subfields from the message, leaving the rest unparsed.

Extension of the parser is handled through the "Decoder" interface, an instance
of which is passed to the constructor. The decoder interface expresses all the
necessary operations for parsing and skipping fields. Each new encoding type
must implement a Decoder, as well as a parser for the associated schema format
to transform it to a schema.Schema.
*/

////////////////////////////////////////////////////////////////////////////////

// Bytecode instructions for the parser.
const (
	// Parse instructions.
	opBool     byte = iota // 0
	opInt8                 // 1
	opUint8                // 2
	opInt16                // 3
	opUint16               // 4
	opInt32                // 5
	opUint32               // 6
	opInt64                // 7
	opUint64               // 8
	opFloat32              // 9
	opFloat64              // 10
	opString               // 11
	opTime                 // 12
	opDuration             // 13

	// Skip instructions. Must be in same order.
	opSkipBool     // 14
	opSkipInt8     // 15
	opSkipUint8    // 16
	opSkipInt16    // 17
	opSkipUint16   // 18
	opSkipInt32    // 19
	opSkipUint32   // 20
	opSkipInt64    // 21
	opSkipUint64   // 22
	opSkipFloat32  // 23
	opSkipFloat64  // 24
	opSkipString   // 25
	opSkipTime     // 26
	opSkipDuration // 27

	// Other instructions.
	opVarlenArrayStart   // 28
	opFixedlenArrayStart // 29
	opArrayEnd           // 30
)

var (
	// Mapping of schema primitive types to the associated parse bytecode.
	primitivesToInclude = map[PrimitiveType]byte{ // nolint: gochecknoglobals
		BOOL:     opBool,
		INT8:     opInt8,
		UINT8:    opUint8,
		INT16:    opInt16,
		UINT16:   opUint16,
		INT32:    opInt32,
		UINT32:   opUint32,
		INT64:    opInt64,
		UINT64:   opUint64,
		FLOAT32:  opFloat32,
		FLOAT64:  opFloat64,
		STRING:   opString,
		TIME:     opTime,
		DURATION: opDuration,
		CHAR:     opUint8,
		BYTE:     opUint8,
	}

	// Mapping of schema primitive types to the associated skip bytecode.
	primitivesToSkip = map[PrimitiveType]byte{ // nolint: gochecknoglobals
		BOOL:     opSkipBool,
		INT8:     opSkipInt8,
		UINT8:    opSkipUint8,
		INT16:    opSkipInt16,
		UINT16:   opSkipUint16,
		INT32:    opSkipInt32,
		UINT32:   opSkipUint32,
		INT64:    opSkipInt64,
		UINT64:   opSkipUint64,
		FLOAT32:  opSkipFloat32,
		FLOAT64:  opSkipFloat64,
		STRING:   opSkipString,
		TIME:     opSkipTime,
		DURATION: opSkipDuration,
		CHAR:     opSkipUint8,
		BYTE:     opSkipUint8,
	}
)

type Parser struct {
	stack    []byte
	opcodes  []byte
	breaks   []int
	values   []any
	permuted []any
	decoder  Decoder
	handlers []func() error

	selectionCount int
	permutation    []int

	buf []byte
}

func NewParser(schema *Schema, fieldSelections []string, decoder Decoder) (*Parser, error) {
	handlers := make([]func() error, 100)
	permutation := inferPermutation(schema, fieldSelections)
	opcodes, err := compileSchemaByteCode(schema, fieldSelections)
	if err != nil {
		return nil, err
	}
	p := &Parser{
		opcodes:        opcodes,
		decoder:        decoder,
		values:         []any{},
		permuted:       make([]any, len(permutation)),
		buf:            make([]byte, binary.MaxVarintLen64),
		selectionCount: len(fieldSelections),
		permutation:    permutation,
	}
	handlers[opBool] = p.handleBool
	handlers[opInt8] = p.handleInt8
	handlers[opUint8] = p.handleUint8
	handlers[opInt16] = p.handleInt16
	handlers[opUint16] = p.handleUint16
	handlers[opInt32] = p.handleInt32
	handlers[opUint32] = p.handleUint32
	handlers[opInt64] = p.handleInt64
	handlers[opUint64] = p.handleUint64
	handlers[opFloat32] = p.handleFloat32
	handlers[opFloat64] = p.handleFloat64
	handlers[opString] = p.handleString
	handlers[opTime] = p.handleTime
	handlers[opDuration] = p.handleDuration

	handlers[opSkipBool] = p.decoder.SkipBool
	handlers[opSkipInt8] = p.decoder.SkipInt8
	handlers[opSkipUint8] = p.decoder.SkipUint8
	handlers[opSkipInt16] = p.decoder.SkipInt16
	handlers[opSkipUint16] = p.decoder.SkipUint16
	handlers[opSkipInt32] = p.decoder.SkipInt32
	handlers[opSkipUint32] = p.decoder.SkipUint32
	handlers[opSkipInt64] = p.decoder.SkipInt64
	handlers[opSkipUint64] = p.decoder.SkipUint64
	handlers[opSkipFloat32] = p.decoder.SkipFloat32
	handlers[opSkipFloat64] = p.decoder.SkipFloat64
	handlers[opSkipString] = p.decoder.SkipString
	handlers[opSkipTime] = p.decoder.SkipTime
	handlers[opSkipDuration] = p.decoder.SkipDuration

	handlers[opVarlenArrayStart] = p.handleArrayStart(false)
	handlers[opFixedlenArrayStart] = p.handleArrayStart(true)
	p.handlers = handlers
	return p, nil
}

func inferPermutation(schema *Schema, selections []string) []int {
	var permutation []int
	if len(selections) > 0 {
		analyzed := AnalyzeSchema(*schema)
		fieldNames := util.Map(func(n util.Named[PrimitiveType]) string {
			return n.Name
		}, analyzed)
		relevant := util.Filter(func(s string) bool {
			return slices.Index(selections, s) != -1
		}, fieldNames)
		permutation = make([]int, len(selections))
		for i, selection := range selections {
			permutation[i] = slices.Index(relevant, selection)
		}
	}
	if !needsPermutation(permutation) {
		return nil
	}
	return permutation
}

// Parse the given buffer and return the set of parsed array lengths, the list
// of parsed values, and any error encountered. These can be used to reconstruct
// the full message structure by walking the original schema. The parsed values
// may be unsafely backed by the input buffer, thus the caller must be cautious
// about reuse of the buffer or values across calls.
func (p *Parser) Parse(buf []byte) ([]int, []any, error) {
	p.breaks = p.breaks[:0]
	p.values = p.values[:0]
	p.decoder.Set(buf)
	if cap(p.stack) < len(p.opcodes) {
		p.stack = make([]byte, len(p.opcodes))
	}
	p.stack = p.stack[:len(p.opcodes)]
	copy(p.stack, p.opcodes)
	for len(p.stack) > 0 {
		op := p.stack[len(p.stack)-1]
		p.stack = p.stack[:len(p.stack)-1]
		if err := p.handlers[op](); err != nil {
			return nil, nil, err
		}
	}
	p.permute()
	if p.selectionCount > 0 && len(p.values) != p.selectionCount {
		return nil, nil, fmt.Errorf("expected %d values, got %d", p.selectionCount, len(p.values))
	}
	return p.breaks, p.values, nil
}

func (p *Parser) permute() {
	if len(p.permutation) == 0 {
		return
	}
	for i, j := range p.permutation {
		p.permuted[i] = p.values[j]
	}
	p.values = append(p.values[:0], p.permuted...)
}

func needsPermutation(permutation []int) bool {
	last := -1
	for i := 0; i < len(permutation); i++ {
		if permutation[i] <= last {
			return true
		}
		last = permutation[i]
	}
	return false
}

// ReadByte implements the io.ByteReader interface.
func (p *Parser) ReadByte() (byte, error) {
	return p.popByte(), nil
}

func (p *Parser) handleBool() error {
	x, err := p.decoder.Bool()
	if err != nil {
		return fmt.Errorf("failed to parse bool: %w", err)
	}
	p.values = append(p.values, x)
	return nil
}

func (p *Parser) handleInt8() error {
	x, err := p.decoder.Int8()
	if err != nil {
		return fmt.Errorf("failed to parse int8: %w", err)
	}
	p.values = append(p.values, x)
	return nil
}

func (p *Parser) handleInt16() error {
	x, err := p.decoder.Int16()
	if err != nil {
		return fmt.Errorf("failed to parse int16: %w", err)
	}
	p.values = append(p.values, x)
	return nil
}

func (p *Parser) handleInt32() error {
	x, err := p.decoder.Int32()
	if err != nil {
		return fmt.Errorf("failed to parse int32: %w", err)
	}
	p.values = append(p.values, x)
	return nil
}

func (p *Parser) handleInt64() error {
	x, err := p.decoder.Int64()
	if err != nil {
		return fmt.Errorf("failed to parse int64: %w", err)
	}
	p.values = append(p.values, x)
	return nil
}

func (p *Parser) handleUint8() error {
	x, err := p.decoder.Uint8()
	if err != nil {
		return fmt.Errorf("failed to parse uint8: %w", err)
	}
	p.values = append(p.values, x)
	return nil
}

func (p *Parser) handleUint16() error {
	x, err := p.decoder.Uint16()
	if err != nil {
		return fmt.Errorf("failed to parse uint16: %w", err)
	}
	p.values = append(p.values, x)
	return nil
}

func (p *Parser) handleUint32() error {
	x, err := p.decoder.Uint32()
	if err != nil {
		return fmt.Errorf("failed to parse uint32: %w", err)
	}
	p.values = append(p.values, x)
	return nil
}

func (p *Parser) handleUint64() error {
	x, err := p.decoder.Uint64()
	if err != nil {
		return fmt.Errorf("failed to parse uint64: %w", err)
	}
	p.values = append(p.values, x)
	return nil
}

func (p *Parser) handleFloat32() error {
	x, err := p.decoder.Float32()
	if err != nil {
		return fmt.Errorf("failed to parse float32: %w", err)
	}
	p.values = append(p.values, x)
	return nil
}

func (p *Parser) handleFloat64() error {
	x, err := p.decoder.Float64()
	if err != nil {
		return fmt.Errorf("failed to parse float64: %w", err)
	}
	p.values = append(p.values, x)
	return nil
}

func (p *Parser) handleTime() error {
	x, err := p.decoder.Time()
	if err != nil {
		return fmt.Errorf("failed to parse time: %w", err)
	}
	p.values = append(p.values, x)
	return nil
}

func (p *Parser) handleDuration() error {
	x, err := p.decoder.Duration()
	if err != nil {
		return fmt.Errorf("failed to parse duration: %w", err)
	}
	p.values = append(p.values, x)
	return nil
}

func (p *Parser) handleString() error {
	s, err := p.decoder.String()
	if err != nil {
		return fmt.Errorf("failed to parse string: %w", err)
	}
	p.values = append(p.values, s)
	return nil
}

func (p *Parser) popByte() byte {
	b := p.stack[len(p.stack)-1]
	p.stack = p.stack[:len(p.stack)-1]
	return b
}

func (p *Parser) parseArrayHeader() (bool, [][]int, error) {
	isComplex := p.popByte() == 0x01
	projected := p.popByte() == 0x01
	if !projected {
		return isComplex, nil, nil
	}
	ranges := [][]int{}
	length, err := p.readVarint()
	if err != nil {
		return isComplex, nil, fmt.Errorf("failed to parse range length: %w", err)
	}
	for i := 0; i < int(length); i++ {
		start, err := p.readVarint()
		if err != nil {
			return isComplex, nil, fmt.Errorf("failed to parse range start: %w", err)
		}
		end, err := p.readVarint()
		if err != nil {
			return isComplex, nil, fmt.Errorf("failed to parse range end: %w", err)
		}
		ranges = append(ranges, []int{int(start), int(end)})
	}
	return isComplex, ranges, nil
}

func (p *Parser) readVarint() (int64, error) {
	x, err := binary.ReadVarint(p)
	if err != nil {
		return 0, fmt.Errorf("failed to read varint: %w", err)
	}
	return x, nil
}

func buildArrayHeader(isComplex bool, ranges [][]int) []byte {
	complexByte := util.When(isComplex, byte(0x01), byte(0x00))
	if len(ranges) == 0 {
		return []byte{complexByte, 0x00}
	}
	tmp := make([]byte, binary.MaxVarintLen64)
	buf := []byte{complexByte, 0x01}
	n := binary.PutVarint(tmp, int64(len(ranges)))
	buf = append(buf, tmp[:n]...)
	for _, r := range ranges {
		n := binary.PutVarint(tmp, int64(r[0]))
		buf = append(buf, tmp[:n]...)
		n = binary.PutVarint(tmp, int64(r[1]))
		buf = append(buf, tmp[:n]...)
	}
	return buf
}

func overlaps(x int, ranges [][]int) bool {
	for _, r := range ranges {
		if x >= r[0] && x < r[1] {
			return true
		}
	}
	return false
}

func (p *Parser) handleArrayStart(fixedSize bool) func() error {
	return func() (err error) {
		var length int64
		if fixedSize {
			length, err = p.readVarint()
			if err != nil {
				return fmt.Errorf("failed to read array size: %w", err)
			}
		} else {
			length, err = p.decoder.ArrayLength()
			if err != nil {
				return fmt.Errorf("failed to parse array length: %w", err)
			}
		}
		return p.handleArray(length)
	}
}

func (p *Parser) handleByteArray(length int) error {
	buf, err := p.decoder.Bytes(length)
	if err != nil {
		return fmt.Errorf("failed to read byte array: %w", err)
	}
	p.values = append(p.values, buf)
	return nil
}

func (p *Parser) handleArray(length int64) error {
	isComplex, ranges, err := p.parseArrayHeader()
	if err != nil {
		return fmt.Errorf("failed to parse array projection: %w", err)
	}
	tmp := []byte{}
	tmpskip := []byte{}
	counter := 1
	for {
		s := p.stack[len(p.stack)-1]
		p.stack = p.stack[:len(p.stack)-1]
		switch s {
		case opFixedlenArrayStart:
			size, err := p.readVarint()
			if err != nil {
				return fmt.Errorf("failed to read array size: %w", err)
			}
			tmp = append(tmp, s)
			tmp = binary.AppendVarint(tmp, size)
			tmpskip = append(tmpskip, s)
			tmpskip = binary.AppendVarint(tmpskip, size)
			counter++
			continue
		case opVarlenArrayStart:
			counter++
		case opArrayEnd:
			counter--
			if counter == 0 { // nolint: nestif
				hasRanges := len(ranges) > 0

				if !isComplex && !hasRanges && len(tmp) == 1 && tmp[0] == opUint8 {
					p.breaks = append(p.breaks, int(length))
					return p.handleByteArray(int(length))
				}

				slices.Reverse(tmp)
				slices.Reverse(tmpskip)
				p.breaks = append(p.breaks, int(length))
				for i := 0; i < int(length); i++ {
					if !hasRanges || overlaps(i, ranges) {
						if err := p.exec(tmp); err != nil {
							return err
						}
					} else {
						if err := p.exec(tmpskip); err != nil {
							return err
						}
					}
				}
				return nil
			}
		}
		tmp = append(tmp, s)
		tmpskip = append(tmpskip, includeToSkip(s))
	}
}

func includeToSkip(s byte) byte {
	if s < 14 {
		return s + 14
	}
	return s
}

func allZeros(m map[string]int) bool {
	for _, v := range m {
		if v != 0 {
			return false
		}
	}
	return true
}

// AnalyzeSchema returns a list of Named[schema.PrimitiveType] that represent
// interesting values in a message. The length and ordering of this list match
// the response of SkipMessage.
func AnalyzeSchema(s Schema) []util.Named[PrimitiveType] {
	fields := []util.Named[PrimitiveType]{}
	for _, f := range s.Fields {
		types := []Type{f.Type}
		names := []string{f.Name}
		for len(types) > 0 {
			t := types[0]
			types = types[1:]
			name := names[0]
			names = names[1:]
			if t.Primitive > 0 {
				fields = append(fields, util.NewNamed(name, t.Primitive))
				continue
			}
			if t.Array {
				if t.FixedSize > 0 && t.FixedSize < 10 {
					elementtypes := make([]Type, 0, t.FixedSize+len(types))
					elementnames := make([]string, 0, t.FixedSize+len(names))
					for i := 0; i < t.FixedSize; i++ {
						elementtypes = append(elementtypes, *t.Items)
						elementnames = append(elementnames, fmt.Sprintf("%s[%d]", name, i))
					}
					// straight to the front
					types = append(elementtypes, types...)
					names = append(elementnames, names...)
				}
				continue
			}
			if t.Record {
				for _, f := range t.Fields {
					types = append(types, f.Type)
					names = append(names, name+"."+f.Name)
				}
				continue
			}
		}
	}
	return fields
}

// compileSchemaByteCode compiles a schema to a stack of bytecodes that can be
// copied and repeatedly used for parsing. The stack is created by enqueuing
// opcodes in a depth-first ordering of the schema. Then the queue is reversed
// to form the stack.
func compileSchemaByteCode(schema *Schema, fieldSelections []string) ([]byte, error) { // nolint: funlen
	// parse the field ordering of the schema to diff and correct against
	// selections.
	codes := []byte{}

	multiplicities := make(map[string]int)
	unselected := make(map[string]int)
	for _, selection := range fieldSelections {
		multiplicities[selection]++
		unselected[selection]++
	}
	for _, field := range schema.Fields {
		// if we were requested specific fields but they are all selected, we
		// can stop early.
		if fieldSelections != nil && allZeros(unselected) {
			break
		}
		stack := []*util.Pair[*Type, string]{util.Pointer(util.NewPair(&field.Type, field.Name))}
		for len(stack) > 0 {
			pair := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			if pair == nil { // sentinal
				codes = append(codes, opArrayEnd)
				continue
			}
			typ := pair.First
			prefix := pair.Second
			unselected[prefix]--
			include := fieldSelections == nil
			if !include {
				for _, selection := range fieldSelections {
					re, err := regexp.Compile(`^` + prefix)
					if err != nil {
						return nil, fmt.Errorf("failed to compile prefix regex: %w", err)
					}
					if re.MatchString(selection) {
						include = true
						break
					}
				}
			}
			if typ.IsPrimitive() {
				// how to figure out right here what the relevant field index is?
				code := util.When(include, primitivesToInclude[typ.Primitive], primitivesToSkip[typ.Primitive])
				codes = append(codes, code)
				continue
			}
			if typ.Array {
				isComplex := !typ.Items.IsPrimitive()
				ranges, err := parseArraySelections(fieldSelections, prefix)
				if err != nil {
					return nil, fmt.Errorf("failed to parse array selections: %w", err)
				}
				projection := buildArrayHeader(isComplex, ranges)
				if typ.FixedSize > 0 {
					codes = append(codes, opFixedlenArrayStart)
					codes = binary.AppendVarint(codes, int64(typ.FixedSize))
				} else {
					codes = append(codes, opVarlenArrayStart)
				}
				codes = append(codes, projection...)
				stack = append(stack, nil)
				stack = append(stack, util.Pointer(util.NewPair(typ.Items, prefix+`\[\d*\]`)))
				continue
			}
			if typ.Record {
				for i := len(typ.Fields) - 1; i >= 0; i-- {
					childType := &typ.Fields[i].Type
					stack = append(stack, util.Pointer(util.NewPair(childType, prefix+`\.`+typ.Fields[i].Name)))
				}
				continue
			}
		}
	}
	slices.Reverse(codes)
	return codes, nil
}

func buildDiscreteRanges(input []int) [][]int {
	if len(input) == 0 {
		return nil
	}
	ranges := [][]int{}
	start := input[0]
	end := input[0] + 1
	for i := 1; i < len(input); i++ {
		if input[i] == end {
			end++
			continue
		}
		ranges = append(ranges, []int{start, end})
		start = input[i]
		end = input[i]
	}
	ranges = append(ranges, []int{start, end})
	return ranges
}

func parseArraySelections(selectionFields []string, prefix string) ([][]int, error) {
	indexes := []int{}
	re, err := regexp.Compile(`^` + prefix)
	if err != nil {
		return nil, fmt.Errorf("failed to compile prefix regex: %w", err)
	}
	for _, field := range selectionFields {
		if !re.MatchString(field) {
			continue
		}
		trimmed := re.ReplaceAllString(field, "")
		bopen := strings.Index(trimmed, "[")
		if bopen == -1 {
			continue
		}
		bclose := strings.Index(trimmed, "]")
		if bclose == -1 {
			continue
		}
		number := trimmed[bopen+1 : bclose]
		index, err := strconv.Atoi(number)
		if err != nil {
			continue
		}
		indexes = append(indexes, index)
	}
	if len(indexes) == 0 {
		return [][]int{}, nil
	}
	slices.Sort(indexes)
	return buildDiscreteRanges(indexes), nil
}

func (p *Parser) exec(s []byte) error {
	stack := p.stack
	p.stack = s
	for len(p.stack) > 0 {
		op := p.popByte()
		if err := p.handlers[op](); err != nil {
			return err
		}
	}
	p.stack = stack
	return nil
}
