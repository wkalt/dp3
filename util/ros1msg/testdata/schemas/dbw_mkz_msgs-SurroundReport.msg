Header header

# Cross Traffic Alert (CTA)
bool cta_left_alert
bool cta_right_alert
bool cta_left_enabled
bool cta_right_enabled

# Blind Spot Information System (BLIS)
bool blis_left_alert
bool blis_right_alert
bool blis_left_enabled
bool blis_right_enabled

# Sonar Sensors
bool sonar_enabled
bool sonar_fault

# Sonar ranges in meters, zero is no-detection
float32[12] sonar

# Sonar index enumeration
uint8 FRONT_LEFT_SIDE=0
uint8 FRONT_LEFT_CORNER=1
uint8 FRONT_LEFT_CENTER=2
uint8 FRONT_RIGHT_CENTER=3
uint8 FRONT_RIGHT_CORNER=4
uint8 FRONT_RIGHT_SIDE=5
uint8 REAR_LEFT_SIDE=6
uint8 REAR_LEFT_CORNER=7
uint8 REAR_LEFT_CENTER=8
uint8 REAR_RIGHT_CENTER=9
uint8 REAR_RIGHT_CORNER=10
uint8 REAR_RIGHT_SIDE=11

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
