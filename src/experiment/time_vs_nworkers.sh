#！/bin/bash

rm -rf ./*.statef
rm -rf ./*.state

python time_vs_nworkers.py --compress_single True --postfix 1g --worker 1 --dataset HDFS > HDFS_1g_1.statet 2>&1
python time_vs_nworkers.py --compress_single True --postfix 1g --worker 2 --dataset HDFS > HDFS_1g_2.statet 2>&1
python time_vs_nworkers.py --compress_single True --postfix 1g --worker 4 --dataset HDFS > HDFS_1g_4.statet 2>&1
python time_vs_nworkers.py --compress_single True --postfix 1g --worker 8 --dataset HDFS > HDFS_1g_8.statet 2>&1
#python time_vs_nworkers.py --compress_single True --postfix 1g --worker 16 --dataset HDFS > HDFS_1g_16.statet 2>&1
#python time_vs_nworkers.py --compress_single True --postfix 1g --worker 32 --dataset HDFS > HDFS_1g_32.statet 2>&1


