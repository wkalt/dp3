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

func TestParseROS1MessageDefinition2(t *testing.T) {
	msgdef := []byte(strings.TrimSpace(`
#                                                                                          
# Specified using the WGS 84 reference ellipsoid                                           

# header.stamp specifies the ROS time for this measurement (the                            
#        corresponding satellite time may be reported using the                            
#        sensor_msgs/TimeReference message).                                               
#                                                                                          
# header.frame_id is the frame of reference reported by the satellite                      
#        receiver, usually the location of the antenna.  This is a                         
#        Euclidean frame relative to the vehicle, not a reference                          
#        ellipsoid.                                                                        
Header header                                                                              

# satellite fix status information                                                         
NavSatStatus status                                                                        

# Latitude [degrees]. Positive is north of equator; negative is south.                     
float64 latitude                                                                           

# Longitude [degrees]. Positive is east of prime meridian; negative is west.               
float64 longitude                                                                          

# Altitude [m]. Positive is above the WGS 84 ellipsoid                                     
# (quiet NaN if no altitude is available).                                                 
float64 altitude                                                                           

# Position covariance [m^2] defined relative to a tangential plane                         
# through the reported position. The components are East, North, and                       
# Up (ENU), in row-major order.                                                            
#                                                                                          
# Beware: this coordinate system exhibits singularities at the poles.                      

float64[9] position_covariance                                                             

# If the covariance of the fix is known, fill it in completely. If the                     
# GPS receiver provides the variance of each measurement, put them                         
# along the diagonal. If only Dilution of Precision is available,                          
# estimate an approximate covariance from that.                                            

uint8 COVARIANCE_TYPE_UNKNOWN = 0                                                          
uint8 COVARIANCE_TYPE_APPROXIMATED = 1                                                     
uint8 COVARIANCE_TYPE_DIAGONAL_KNOWN = 2                                                   
uint8 COVARIANCE_TYPE_KNOWN = 3                                                            

uint8 position_covariance_type                                                             

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
MSG: sensor_msgs/NavSatStatus                                                              
# Navigation Satellite fix status for any Global Navigation Satellite System               

# Whether to output an augmented fix is determined by both the fix                         
# type and the last time differential corrections were received.  A                        
# fix is valid when status >= STATUS_FIX.                                                  

int8 STATUS_NO_FIX =  -1        # unable to fix position                                   
int8 STATUS_FIX =      0        # unaugmented fix                                          
int8 STATUS_SBAS_FIX = 1        # with satellite-based augmentation                        
int8 STATUS_GBAS_FIX = 2        # with ground-based augmentation                           

int8 status                                                                                

# Bits defining which Global Navigation Satellite System signals were                      
# used by the receiver.                                                                    

uint16 SERVICE_GPS =     1                                                                 
uint16 SERVICE_GLONASS = 2                                                                 
uint16 SERVICE_COMPASS = 4      # includes BeiDou.                                         
uint16 SERVICE_GALILEO = 8                                                                 

uint16 service
	`))
	schema, err := ros1msg.ParseROS1MessageDefinition("sensor_msgs", "NavSatFix", msgdef)
	require.NoError(t, err)

	require.Equal(t, "sensor_msgs/NavSatFix", schema.Name)
	require.Len(t, schema.Fields, 7)
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
