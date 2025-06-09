# reuse insert for 1 server
# run.py
# servers = [
#     # {'host': '192.168.98.70', 'user': 'congyong', 'password': '1111'}, # server
#     {'host': '192.168.98.74', 'user': 'congyong', 'password': '1111'},
#     {'host': '192.168.98.73', 'user': 'congyong', 'password': '1111'},
#     {'host': '192.168.98.72', 'user': 'congyong', 'password': '1111'},
#     {'host': '192.168.98.71', 'user': 'congyong', 'password': '1111'},
# ]
# sync.sh
# clis=("192.168.98.74" "192.168.98.73" "192.168.98.72" "192.168.98.71")
../test.sh insert_4_servers run "16" MYHASH,SEPHASH,RACE-Partitioned,Plush
../test.sh insert_4_servers run "32" MYHASH,SEPHASH,RACE-Partitioned,Plush