# ../test.sh ycsb_a run "224" "MYHASH" 这里面的update很慢？但是纯update没那么慢？因为update比较少，没有掩盖去重
# 如果不去重，会深度太大。
# RACE的zipf99在50%更新时就没那么多失败了
# MYHASH在zipf0.99时去重其实很慢？之前ops多掩盖了。TODO: 集成完整key
# ../test.sh ycsb_b run "224" "MYHASH"
# ../test.sh ycsb_c run "224" "MYHASH"
# ../test.sh ycsb_d run "224" "MYHASH"

# ../test.sh ycsb_a run "224" "SEPHASH"
# ../test.sh ycsb_b run "224" "SEPHASH"
# ../test.sh ycsb_c run "224" "SEPHASH"
# ../test.sh ycsb_d run "224" "SEPHASH"


# # Plush should ALLOW_KEY_OVERLAP during LOAD?
# ../test.sh ycsb_a run "224" "Plush"
# ../test.sh ycsb_b run "224" "Plush"
# ../test.sh ycsb_c run "224" "Plush"
# ../test.sh ycsb_d run "224" "Plush"

# # Use RACE_ycsb branch, run manually, not ../test.sh here, disable ALLOW_KEY_OVERLAP (RACE-Partitioned) or it will fail
../test.sh ycsb_a run "224" "RACE"
../test.sh ycsb_b run "224" "RACE"
../test.sh ycsb_c run "224" "RACE"
# ../test.sh ycsb_d run "224" "RACE-Partitoned"

# TODO:
# ../test.sh ycsb_a run "1" "MYHASH"
# ../test.sh ycsb_b run "1" "MYHASH"
# ../test.sh ycsb_c run "1" "MYHASH"
# ../test.sh ycsb_d run "1" "MYHASH"

# ../test.sh ycsb_a run "1" "SEPHASH"
# ../test.sh ycsb_b run "1" "SEPHASH"
# ../test.sh ycsb_c run "1" "SEPHASH"
# ../test.sh ycsb_d run "1" "SEPHASH"


# # Plush should ALLOW_KEY_OVERLAP during LOAD?
# ../test.sh ycsb_a run "1" "Plush"
# ../test.sh ycsb_b run "1" "Plush"
# ../test.sh ycsb_c run "1" "Plush"
# ../test.sh ycsb_d run "1" "Plush"

# # Use RACE_ycsb branch, run manually, not ../test.sh here, disable ALLOW_KEY_OVERLAP (RACE-Partitioned) or it will fail
# ../test.sh ycsb_a run "1" "RACE"
# ../test.sh ycsb_b run "1" "RACE"
# ../test.sh ycsb_c run "1" "RACE"
# ../test.sh ycsb_d run "1" "RACE-Partitoned"