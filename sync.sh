#!/bin/bash
# usage: 
#       ./sync.sh out num_server
#       ./sync.sh in num_server cli_num(1,2,4,8,16) coro_num(1,2,3,4)
# clis=("192.168.98.71" "192.168.98.72" "192.168.98.73" "192.168.98.74" "192.168.98.75")
clis=("192.168.98.74" "192.168.98.73" "192.168.98.72" "192.168.98.71")
if [ "$1" = "out" ]
then
    # make ser_cli_var_kv -j112
    make ser_cli -j112 > /dev/null
    for cli in ${clis[@]:0:$2}
    do 
        # echo "sync out cli" $cli
        sshpass -p '1111' scp ./ser_cli congyong@$cli:/home/congyong/
        # sshpass -p '1111' scp ./ser_cli_var_kv congyong@$cli:/home/congyong/
        sshpass -p '1111' scp ../ser_cli.sh congyong@$cli:/home/congyong/
        sshpass -p '1111' scp ../run.sh congyong@$cli:/home/congyong/
    done
else
    cnt=1
    rm -f ./insert_lat*192.168*.txt
    rm -f ./search_lat*192.168*.txt
    for cli in ${clis[@]:0:$2}
    do 
        # echo "sync in cli" $cli
        rm -f ./out$cli.txt
        sshpass -p '1111' scp congyong@$cli:/home/congyong/out.txt ./out$cli.txt 2>/dev/null || true
        echo -n "cli $cli: "
        grep "Run IOPS" ./out$cli.txt
        cli_num=$3
        coro_num=$4
        for ((cli_id=0; cli_id<cli_num; cli_id++)); do
            for ((coro_id=0; coro_id<coro_num; coro_id++)); do
                # 设置要匹配的文件名
                filename="insert_lat${cli_id}${coro_id}.txt"
                echo $filename
                # 执行SCP命令，从远程服务器复制文件到本地并修改文件名
                sshpass -p "1111" scp "congyong@$cli:/home/congyong/$filename" "./insert_lat${cli_id}${coro_id}$cli.txt"
                
                filename="search_lat${cli_id}${coro_id}.txt"
                echo $filename
                # 执行SCP命令，从远程服务器复制文件到本地并修改文件名
                sshpass -p "1111" scp "congyong@$cli:/home/congyong/$filename" "./search_lat${cli_id}${coro_id}$cli.txt"
            done
        done
        ((cnt += 1))
    done
fi
