Header header                   # Header w timestamp
float64 voltage                 # Raw voltage from MCU
float64 current                 # Raw current from MCU
float64 power                   # Calculated Power from MCU
float64 energy                  # Calculated cumulative energy from MCU
float64 energy_Wh               # Calculated cumulative energy from MCU (in Wh)
duration energy_uptime          # Duration of energy measurement
time since_time                 # Time of first energy measurement
# Legacy messages
duration mcu_uptime             # Time since MCU power-on
float64 total_power_consumed    # Integration of all power consumption since MCU power-on, in watt-hours
float32 mcu_temperature         # Temperature of MCU in Celsius.
float32 current_battery_peak    # Highest total system current peak as measured in a 1ms window.


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
