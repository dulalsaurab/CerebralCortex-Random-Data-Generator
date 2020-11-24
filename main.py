from ccrdg.battery_data import gen_battery_data
from ccrdg.accel_gyro_data import gen_accel_gyro_data
from ccrdg.location_data import gen_location_datastream, gen_semantic_location_datastream

from cerebralcortex.kernel import Kernel

study_name = "mguard"
CC = Kernel(cc_configs="default", study_name=study_name, new_study=True)

user_id = "00000000-e19c-3956-9db2-5459ccadd40c"
battery_stream_name = "battery--org.md2k.phonesensor--phone"
location_stream_name = "location--org.md2k.phonesensor--phone"
semantic_location_stream_name = "org.md2k.data_analysis.gps_episodes_and_semantic_location"
accel_stream_name = "accelerometer--org.md2k.phonesensor--phone"
gyro_stream_name = "gyroscope--org.md2k.phonesensor--phone"

gen_battery_data(CC, study_name=study_name, user_id=user_id, stream_name=battery_stream_name)
gen_location_datastream(CC, study_name=study_name, user_id=user_id, stream_name=location_stream_name)
gen_semantic_location_datastream(CC, study_name=study_name, user_id=user_id, stream_name=semantic_location_stream_name)
gen_accel_gyro_data(CC, study_name=study_name, user_id=user_id, stream_name=accel_stream_name)
gen_accel_gyro_data(CC, study_name=study_name, user_id=user_id, stream_name=gyro_stream_name)