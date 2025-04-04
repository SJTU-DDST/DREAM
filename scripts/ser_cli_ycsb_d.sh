# /bin/bash
# usage: 
#       server: ../ser_cli.sh server
#       client: ../ser_cli.sh machine_id num_cli num_coro num_machine
#       client_0: ../ser_cli.sh 0 1 1 2
#       client_1: ../ser_cli.sh 1 1 1 2
#       num_cli : 0~4
#       num_coro : 1~4

# Server auto run clients:
#      server: ../ser_cli.sh server num_cli num_coro num_machine
export LD_LIBRARY_PATH="/usr/local/lib64/:$LD_LIBRARY_PATH"
export LD_LIBRARY_PATH="/usr/local/gcc-14.2.0/lib64:$LD_LIBRARY_PATH"
num_op=10000000
if [ "$1" = "server" ]
then
    # echo "server"
    cd /home/congyong/SepHash/build && bash ../sync.sh out $4 && \
    # ./ser_cli_var_kv --server \
    ./ser_cli_var_kv --server --auto_run_client \
    --gid_idx 1 \
    --max_coro 256 --cq_size 64 \
    --mem_size 191268055040 \
    --num_cli $2 --num_coro $3 --num_machine $4

    bash ../sync.sh in $4
    # grep -H . ./out*.txt
else
    echo "machine" $1 "num_machine" $4

    for num_cli in `seq $2 $2`;do
        for num_coro in `seq 1 $3`;do
            for load_num in 10000000;do
                echo "num_cli" $num_cli "num_coro" $num_coro "load_num" $load_num "op_num" $num_op
                # ./ser_cli_var_kv \
                ./ser_cli_var_kv \
                --server_ip 192.168.98.70 --num_machine $4 --num_cli $num_cli --num_coro $num_coro \
                --gid_idx 1 \
                --max_coro 256 --cq_size 64 \
                --machine_id $1  \
                --load_num $load_num \
                --num_op $num_op \
                --pattern_type 3 \
                --insert_frac 0.05 \
                --read_frac   0.95 \
                --update_frac  0.0 \
                --delete_frac  0.0 \
                --read_size     64
            done 
        done
    done
fi

# YCSB A : read:0.5,update:0.5 zipfian(2)
# YCSB B : read:0.95,update:0.05 zipfian(2)
# YCSB C : read:1.0,update:0.0 zipfian(2)
# YCSB D : read:0.95,insert:0.05 latest(3)
# YCSB E : scan--不考虑
# YCSB F : read:0.5,rmq:0.5 zipfian(2) -- RMW ，不考虑