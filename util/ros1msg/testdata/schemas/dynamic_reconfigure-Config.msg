BoolParameter[] bools
IntParameter[] ints
StrParameter[] strs
DoubleParameter[] doubles
GroupState[] groups

================================================================================
MSG: dynamic_reconfigure/BoolParameter
string name
bool value

================================================================================
MSG: dynamic_reconfigure/IntParameter
string name
int32 value

================================================================================
MSG: dynamic_reconfigure/StrParameter
string name
string value

================================================================================
MSG: dynamic_reconfigure/DoubleParameter
string name
float64 value

================================================================================
MSG: dynamic_reconfigure/GroupState
string name
bool state
int32 id
int32 parent
