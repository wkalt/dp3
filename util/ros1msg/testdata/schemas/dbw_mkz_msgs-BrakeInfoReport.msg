Header header

# Wheel torques (Nm)
float32 brake_torque_request
float32 brake_torque_actual
float32 wheel_torque_actual

# Vehicle Acceleration (m/s^2)
float32 accel_over_ground

# Hill Start Assist
HillStartAssist hsa

# Anti-lock Brakes
bool abs_active
bool abs_enabled

# Stability Control
bool stab_active
bool stab_enabled

# Traction Control
bool trac_active
bool trac_enabled

# Parking Brake
ParkingBrake parking_brake

# Misc
bool stationary

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
MSG: dbw_mkz_msgs/HillStartAssist
uint8 status
uint8 mode

uint8 STAT_INACTIVE=0
uint8 STAT_FINDING_GRADIENT=1
uint8 STAT_ACTIVE_PRESSED=2
uint8 STAT_ACTIVE_RELEASED=3
uint8 STAT_FAST_RELEASE=4
uint8 STAT_SLOW_RELEASE=5
uint8 STAT_FAILED=6
uint8 STAT_UNDEFINED=7

uint8 MODE_OFF=0
uint8 MODE_AUTO=1
uint8 MODE_MANUAL=2
uint8 MODE_UNDEFINED=3

================================================================================
MSG: dbw_mkz_msgs/ParkingBrake
uint8 status

uint8 OFF=0
uint8 TRANS=1
uint8 ON=2
uint8 FAULT=3
