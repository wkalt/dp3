# Represents a command for the 4 RGB body lights on Warthog.

uint8 LIGHTS_FRONT_LEFT=0
uint8 LIGHTS_FRONT_RIGHT=1
uint8 LIGHTS_REAR_LEFT=2
uint8 LIGHTS_REAR_RIGHT=3

warthog_msgs/RGB[4] lights

================================================================================
MSG: warthog_msgs/RGB
# Represents the intensity of a single RGB LED, either reported or commanded.

float32 red
float32 green
float32 blue
