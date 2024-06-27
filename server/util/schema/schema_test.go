package schema_test

import (
	"testing"

	"github.com/wkalt/dp3/server/util/schema"
)

func TestPrimitiveTypeString(t *testing.T) {
	cases := []struct {
		assertion string
		pt        schema.PrimitiveType
		expected  string
	}{
		{"int8", schema.INT8, "int8"},
		{"int16", schema.INT16, "int16"},
		{"int32", schema.INT32, "int32"},
		{"int64", schema.INT64, "int64"},
		{"uint8", schema.UINT8, "uint8"},
		{"uint16", schema.UINT16, "uint16"},
		{"uint32", schema.UINT32, "uint32"},
		{"uint64", schema.UINT64, "uint64"},
		{"float32", schema.FLOAT32, "float32"},
		{"float64", schema.FLOAT64, "float64"},
		{"string", schema.STRING, "string"},
		{"bool", schema.BOOL, "bool"},
		{"time", schema.TIME, "time"},
		{"duration", schema.DURATION, "duration"},
		{"char", schema.CHAR, "char"},
		{"byte", schema.BYTE, "byte"},
		{"unknown", schema.PrimitiveType(0), "unknown"},
	}
	for _, c := range cases {
		if c.pt.String() != c.expected {
			t.Errorf("expected %s, got %s", c.expected, c.pt.String())
		}
	}
}

func TestPrimitiveTypeJSONRoundTrip(t *testing.T) {
	cases := []struct {
		assertion string
		pt        schema.PrimitiveType
	}{
		{"int8", schema.INT8},
		{"int16", schema.INT16},
		{"int32", schema.INT32},
		{"int64", schema.INT64},
		{"uint8", schema.UINT8},
		{"uint16", schema.UINT16},
		{"uint32", schema.UINT32},
		{"uint64", schema.UINT64},
		{"float32", schema.FLOAT32},
		{"float64", schema.FLOAT64},
		{"string", schema.STRING},
		{"bool", schema.BOOL},
		{"time", schema.TIME},
		{"duration", schema.DURATION},
		{"char", schema.CHAR},
		{"byte", schema.BYTE},
	}
	for _, c := range cases {
		data, err := c.pt.MarshalJSON()
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		var pt schema.PrimitiveType
		if err := pt.UnmarshalJSON(data); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if pt != c.pt {
			t.Errorf("expected %v, got %v", c.pt, pt)
		}
	}
}

func TestPrimitiveTypeInvalidJSON(t *testing.T) {
	var pt schema.PrimitiveType
	if err := pt.UnmarshalJSON([]byte(`"invalid"`)); err == nil {
		t.Error("expected error, got nil")
	}
}
