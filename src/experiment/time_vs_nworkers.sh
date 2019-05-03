#！/bin/bash

rm -rf ./*.statef

nohup python time_vs_nworkers.py --compress_single False --postfix 1g --worker 1 --dataset HDFS > HDFS_1g_1.statef 2>&1 &
sleep 10s
nohup python time_vs_nworkers.py --compress_single False --postfix 1g --worker 2 --dataset HDFS > HDFS_1g_2.statef 2>&1 &
sleep 10s
nohup python time_vs_nworkers.py --compress_single False --postfix 1g --worker 4 --dataset HDFS > HDFS_1g_4.statef 2>&1 &
sleep 10s
nohup python time_vs_nworkers.py --compress_single False --postfix 1g --worker 1 --dataset Andriod > Andriod_1g_1.statef 2>&1 &
sleep 10s
nohup python time_vs_nworkers.py --compress_single False --postfix 1g --worker 2 --dataset Andriod > Andriod_1g_2.statef 2>&1 &
sleep 10s
nohup python time_vs_nworkers.py --compress_single False --postfix 1g --worker 4 --dataset Andriod > Andriod_1g_4.statef 2>&1 &
sleep 10s


nohup python time_vs_nworkers.py --compress_single False --postfix 1g --worker 1 --dataset Spark > Spark_1g_1.statef 2>&1 &
sleep 10s
nohup python time_vs_nworkers.py --compress_single False --postfix 1g --worker 2 --dataset Spark > Spark_1g_2.statef 2>&1 &
sleep 10s
nohup python time_vs_nworkers.py --compress_single False --postfix 1g --worker 4 --dataset Spark > Spark_1g_4.statef 2>&1 &
sleep 10s
nohup python time_vs_nworkers.py --compress_single False --postfix 1g --worker 1 --dataset Thunderbird > Thunderbird_1g_1.statef 2>&1 &
sleep 10s
nohup python time_vs_nworkers.py --compress_single False --postfix 1g --worker 2 --dataset Thunderbird > Thunderbird_1g_2.statef 2>&1 &
sleep 10s
nohup python time_vs_nworkers.py --compress_single False --postfix 1g --worker 4 --dataset Thunderbird > Thunderbird_1g_4.statef 2>&1 &

sleep 15m

nohup python time_vs_nworkers.py --compress_single False --postfix 1g --worker 1 --dataset Windows > Windows_1g_1.statef 2>&1 &
sleep 10s
nohup python time_vs_nworkers.py --compress_single False --postfix 1g --worker 2 --dataset Windows > Windows_1g_2.statef 2>&1 &
sleep 10s
nohup python time_vs_nworkers.py --compress_single False --postfix 1g --worker 4 --dataset Windows > Windows_1g_4.statef 2>&1 &
sleep 10s
nohup python time_vs_nworkers.py --compress_single False --postfix 1g --worker 8 --dataset HDFS > HDFS_1g_8.statef 2>&1 &
sleep 10s
nohup python time_vs_nworkers.py --compress_single False --postfix 1g --worker 16 --dataset HDFS > HDFS_1g_16.statef 2>&1 &

sleep 15m

nohup python time_vs_nworkers.py --compress_single False --postfix 1g --worker 8 --dataset Andriod > Andriod_1g_8.statef 2>&1 &
sleep 2
nohup python time_vs_nworkers.py --compress_single False --postfix 1g --worker 16 --dataset Andriod > Andriod_1g_16.statef 2>&1 &

sleep 10m

nohup python time_vs_nworkers.py --compress_single False --postfix 1g --worker 8 --dataset Spark > Spark_1g_8.statef 2>&1 &
sleep 10s
nohup python time_vs_nworkers.py --compress_single False --postfix 1g --worker 16 --dataset Spark > Spark_1g_16.statef 2>&1 &

sleep 10m

nohup python time_vs_nworkers.py --compress_single False --postfix 1g --worker 8 --dataset Thunderbird > Thunderbird_1g_8.statef 2>&1 &
sleep 10s
nohup python time_vs_nworkers.py --compress_single False --postfix 1g --worker 16 --dataset Thunderbird > Thunderbird_1g_16.statef 2>&1 &

sleep 10m

nohup python time_vs_nworkers.py --compress_single False --postfix 1g --worker 8 --dataset Windows > Windows_1g_8.statef 2>&1 &
sleep 10s
nohup python time_vs_nworkers.py --compress_single False --postfix 1g --worker 16 --dataset Windows > Windows_1g_16.statef 2>&1 &


sleep 10m
nohup python time_vs_nworkers.py --compress_single False --postfix 1g --worker 32 --dataset Windows > Windows_1g_32.statef 2>&1 &
sleep 10m
nohup python time_vs_nworkers.py --compress_single False --postfix 1g --worker 32 --dataset Thunderbird > Thunderbird_1g_32.statef 2>&1 &
sleep 10m
nohup python time_vs_nworkers.py --compress_single False --postfix 1g --worker 32 --dataset Andriod > Andriod_1g_32.statef 2>&1 &
sleep 10m
nohup python time_vs_nworkers.py --compress_single False --postfix 1g --worker 32 --dataset HDFS > HDFS_1g_32.statef 2>&1 &
sleep 10m
nohup python time_vs_nworkers.py --compress_single False --postfix 1g --worker 32 --dataset Spark > Spark_1g_32.statef 2>&1 &
sleep 10m

