package mcap_test

import (
	"bytes"
	"testing"

	fmcap "github.com/foxglove/mcap/go/mcap"
	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/mcap"
	"github.com/wkalt/dp3/util/testutils"
)

func TestMCAPToJSON(t *testing.T) {
	// ros1 format message
	msgdef := `
	uint8 u8
	uint16 u16
	uint32 u32
	uint64 u64
	int8 i8
	int16 i16
	int32 i32
	int64 i64
	float32 f32
	float64 f64
	string s
	bool b
	time t
	duration d
	char c
	byte y
	MyType flat_record
	MyType[] varlen_array
	MyType[1] fixed_array
	===
	MSG: test/MyType
	uint8 u8
	`

	data := testutils.Flatten(
		testutils.U8b(1),
		testutils.U16b(2),
		testutils.U32b(3),
		testutils.U64b(4),
		testutils.I8b(5),
		testutils.I16b(6),
		testutils.I32b(7),
		testutils.I64b(8),
		testutils.F32b(9),
		testutils.F64b(10),
		testutils.PrefixedString("hello"),
		testutils.Boolb(true),
		testutils.U64b(11),
		testutils.U64b(12),
		testutils.U8b(13),
		testutils.U8b(14),
		testutils.U8b(15),

		testutils.U32b(2), // varlen
		testutils.U8b(16),
		testutils.U8b(17),

		testutils.U8b(18), // fixed
	)

	buf := &bytes.Buffer{}
	writer, err := fmcap.NewWriter(buf, &fmcap.WriterOptions{})
	require.NoError(t, err)

	require.NoError(t, writer.WriteHeader(&fmcap.Header{}))
	require.NoError(t, writer.WriteSchema(
		mcap.NewSchema(1, "test/test", "ros1msg", []byte(msgdef)),
	))
	require.NoError(t, writer.WriteChannel(
		mcap.NewChannel(0, 1, "/foo", "ros1msg", nil),
	))
	require.NoError(t, writer.WriteMessage(&fmcap.Message{
		ChannelID: 0,
		Data:      data,
		LogTime:   100,
	}))
	require.NoError(t, writer.Close())

	output := &bytes.Buffer{}
	require.NoError(t, mcap.MCAPToJSON(output, buf))
	expected := `{"topic":"/foo","sequence":0,"log_time":0.000000100,"publish_time":0.000000000,` +
		`"data":{"u8":1,"u16":2,"u32":3,"u64":4,"i8":5,"i16":6,"i32":7,"i64":8,"f32":9,"f64":10,"s":"hello",` +
		`"b":true,"t":11000000000,"d":12000000000,"c":13,"y":14,"flat_record":{"u8":15},` +
		`"varlen_array":[{"u8":16},{"u8":17}],"fixed_array":[{"u8":18}]}}` + "\n"
	require.Equal(t, expected, output.String())
}
