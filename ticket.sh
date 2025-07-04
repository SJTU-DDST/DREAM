# # ../test.sh insert run "" MYHASH
../test.sh read run "" MYHASH
# # ../test.sh update run "" MYHASH
# # ../test.sh update_zipf99 run "" MYHASH

../test.sh ycsb_a run "1,224" "MYHASH"
../test.sh ycsb_b run "1,224" "MYHASH"
../test.sh ycsb_c run "1,224" "MYHASH"
../test.sh ycsb_d run "1,224" "MYHASH"

../test.sh insert90_read10 run "224" "MYHASH"
../test.sh insert70_read30 run "224" "MYHASH"
../test.sh insert50_read50 run "224" "MYHASH"
../test.sh insert30_read70 run "224" "MYHASH"
../test.sh insert10_read90 run "224" "MYHASH"

# 对比SPLIT_DEV_NUMA的RACE&SEPHASH
# ../test.sh insert run "" "RACE-Partitioned,SEPHASH"
../test.sh read run "" "SEPHASH"
# ../test.sh update run "" "RACE,SEPHASH"
# ../test.sh update_zipf99 run "" "RACE,SEPHASH"

# ../test.sh ycsb_a run "1,224" "RACE,SEPHASH"
# ../test.sh ycsb_b run "1,224" "RACE,SEPHASH"
# ../test.sh ycsb_c run "1,224" "RACE,SEPHASH"
# ../test.sh ycsb_d run "1,224" "RACE-Partitioned,SEPHASH"

# ../test.sh insert90_read10 run "224" "RACE-Partitioned,SEPHASH"
# ../test.sh insert70_read30 run "224" "RACE-Partitioned,SEPHASH"
# ../test.sh insert50_read50 run "224" "RACE-Partitioned,SEPHASH"
# ../test.sh insert30_read70 run "224" "RACE-Partitioned,SEPHASH"
# ../test.sh insert10_read90 run "224" "RACE-Partitioned,SEPHASH"

../test.sh ycsb_a run "1,224" "SEPHASH"
../test.sh ycsb_b run "1,224" "SEPHASH"
../test.sh ycsb_c run "1,224" "SEPHASH"
../test.sh ycsb_d run "1,224" "SEPHASH"

../test.sh insert90_read10 run "224" "SEPHASH"
../test.sh insert70_read30 run "224" "SEPHASH"
../test.sh insert50_read50 run "224" "SEPHASH"
../test.sh insert30_read70 run "224" "SEPHASH"
../test.sh insert10_read90 run "224" "SEPHASH"