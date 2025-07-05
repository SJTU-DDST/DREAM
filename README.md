# DREAM
WIP

## Building

### Prepare and build dependencies

- g++-14
- C++23
- MLNX_OFED higher than 5.0
- pip install fabric

### Build

- Configuring server/ip information:
    - Change the SepHash path in all bash scripts (/home/congyong/SepHash) to your own directory.
    - Edit the list of client-nodes in run.py and sync.sh.
    - set server's ip in ser_cli.sh.
- Generate executable and copy to all client nodes.

```bash
$ mkdir build 
$ cd build
$ cmake ..
$ make -j112
$ ../sync.sh out #client-nodes
```

- one-key test (MYHASH is DREAM)

```bash
$ cd build
$ ../test.sh insert run "1,4,8,16,32,56,112,168,224" "MYHASH,SEPHASH"
$ grep -r "Run IOPS" ./data_insert/
```

- run servers

```bash
$ ../ser_cli.sh server
```

- run clients

```bash
$ python3 ../run.py #client-nodes client #client-per-node #coroutine-per-client
```

- Collecting data

```bash
$ ../sync.sh in #client-nodes
```

## WorkLoads and comparison objects

### Workloads

Modify the parameters in ser_cli.sh to apply different workloads. 
- load_num: amount of pre-loaded data.
- num_op: amount of operations during run phase.
- XXX_frac: ratios of corresponding operations in the run phase, need to sum to 1.0.
- pattern_type: different distributions of keys. 0 represents sequential workloads, 1 represents uniformly workloads, 2 represents zipfian workloads, and 3 represents lastest workloads.

### Comparison
Modify the called executable in ser_cli.sh to replace different comparison objects.
- ser_cli.cc : fixed length KV test
- ser_cli_var_kv.cc : variable length KV test
- Change the ClientType and ServerType in ser_cli.cc and ser_cli_var_kv.cc to switch between different comparison objects.