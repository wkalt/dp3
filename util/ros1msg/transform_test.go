package ros1msg_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/util/ros1msg"
	"github.com/wkalt/dp3/util/schema"
)

func primitiveType(t schema.PrimitiveType) *schema.Type {
	return &schema.Type{
		Primitive: t,
	}
}

func TestTransform(t *testing.T) {
	cases := []struct {
		assertion string
		msgdef    string
		output    *schema.Schema
	}{
		{
			"primitive",
			"string foo",
			&schema.Schema{
				Name: "test/Test",
				Fields: []schema.Field{
					{
						Name: "foo",
						Type: *primitiveType(schema.STRING),
					},
				},
			},
		},
		{
			"primitive array",
			"string[10] foo",
			&schema.Schema{
				Name: "test/Test",
				Fields: []schema.Field{
					{
						Name: "foo",
						Type: schema.Type{
							Array:     true,
							Items:     primitiveType(schema.STRING),
							FixedSize: 10,
						},
					},
				},
			},
		},
		{
			"primitive array",
			"string[10] foo",
			&schema.Schema{
				Name: "test/Test",
				Fields: []schema.Field{
					{
						Name: "foo",
						Type: schema.Type{
							Array:     true,
							Items:     primitiveType(schema.STRING),
							FixedSize: 10,
						},
					},
				},
			},
		},
		{
			"subdependencies",
			strings.TrimSpace(`
Header header #for timestamp                                                               
===
MSG: std_msgs/Header                                                                       
uint32 seq                                                                                 
time stamp                                                                                 
string frame_id
`),
			&schema.Schema{
				Name: "test/Test",
				Fields: []schema.Field{
					{
						Name: "header",
						Type: schema.Type{
							Record: true,
							Fields: []schema.Field{
								{
									Name: "seq",
									Type: *primitiveType(schema.UINT32),
								},
								{
									Name: "stamp",
									Type: *primitiveType(schema.TIME),
								},
								{
									Name: "frame_id",
									Type: *primitiveType(schema.STRING),
								},
							},
						},
					},
				},
			},
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			schema, err := ros1msg.ParseROS1MessageDefinition("test", "Test", []byte(c.msgdef))
			require.NoError(t, err)
			require.Equal(t, c.output, schema)
		})
	}
}

func TestParseROS1MessageDefinition(t *testing.T) {
	msgdef := []byte(strings.TrimSpace(`
Header header #for timestamp                                                               
DiagnosticStatus[] status # an array of components being reported on                       
================================================================================           
MSG: std_msgs/Header                                                                       
# Standard metadata for higher-level stamped data types.                                   
# This is generally used to communicate timestamped data                                   
# in a particular coordinate frame.                                                        
#                                                                                          
# sequence ID: consecutively increasing ID                                                 
uint32 seq                                                                                 
#Two-integer timestamp that is expressed as:                                               
# * stamp.sec: seconds (stamp_secs) since epoch (in Python the variable is called 'secs')  
# * stamp.nsec: nanoseconds since stamp_secs (in Python the variable is called 'nsecs')    
# time-handling sugar is provided by the client library                                    
time stamp                                                                                 
#Frame this data is associated with                                                        
# 0: no frame                                                                              
# 1: global frame                                                                          
string frame_id                                                                            

================================================================================           
MSG: diagnostic_msgs/DiagnosticStatus                                                      
# This message holds the status of an individual component of the robot.                   
#                                                                                          

# Possible levels of operations                                                            
byte OK=0                                                                                  
byte WARN=1                                                                                
byte ERROR=2                                                                               
byte STALE=3                                                                               

byte level # level of operation enumerated above                                           
string name # a description of the test/component reporting                                
string message # a description of the status                                               
string hardware_id # a hardware unique string                                              
KeyValue[] values # an array of values associated with the status                          


================================================================================           
MSG: diagnostic_msgs/KeyValue                                                              
string key # what to label this value when viewing                                         
string value # a value to track over time 
	`))
	schema, err := ros1msg.ParseROS1MessageDefinition("diagnostic_msgs", "DiagnosticArray", msgdef)
	require.NoError(t, err)

	require.Equal(t, "diagnostic_msgs/DiagnosticArray", schema.Name)
	require.Len(t, schema.Fields, 2)
}
