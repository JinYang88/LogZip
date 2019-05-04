#！/bin/bash

rm -rf ./*.statef
rm -rf ./*.state

#python time_vs_nworkers.py --compress_single True --postfix 1g --worker 1 --dataset HDFS > HDFS_1g_1.statet 2>&1
#python time_vs_nworkers.py --compress_single True --postfix 1g --worker 2 --dataset HDFS > HDFS_1g_2.statet 2>&1
#python time_vs_nworkers.py --compress_single True --postfix 1g --worker 4 --dataset HDFS > HDFS_1g_4.statet 2>&1
#python time_vs_nworkers.py --compress_single True --postfix 1g --worker 8 --dataset HDFS > HDFS_1g_8.statet 2>&1
#python time_vs_nworkers.py --compress_single True --postfix 1g --worker 16 --dataset HDFS > HDFS_1g_16.statet 2>&1
#python time_vs_nworkers.py --compress_single True --postfix 1g --worker 32 --dataset HDFS > HDFS_1g_32.statet 2>&1

python time_vs_nworkers.py --compress_single True --postfix 1g --worker 1 --dataset Andriod > Andriod_1g_1.statet 2>&1
python time_vs_nworkers.py --compress_single True --postfix 1g --worker 2 --dataset Andriod > Andriod_1g_2.statet 2>&1
python time_vs_nworkers.py --compress_single True --postfix 1g --worker 4 --dataset Andriod > Andriod_1g_4.statet 2>&1
python time_vs_nworkers.py --compress_single True --postfix 1g --worker 8 --dataset Andriod > Andriod_1g_8.statet 2>&1
python time_vs_nworkers.py --compress_single True --postfix 1g --worker 16 --dataset Andriod > Andriod_1g_16.statet 2>&1
python time_vs_nworkers.py --compress_single True --postfix 1g --worker 32 --dataset Andriod > Andriod_1g_32.statet 2>&1

python time_vs_nworkers.py --compress_single True --postfix 1g --worker 1 --dataset Spark > Spark_1g_1.statet 2>&1
python time_vs_nworkers.py --compress_single True --postfix 1g --worker 2 --dataset Spark > Spark_1g_2.statet 2>&1
python time_vs_nworkers.py --compress_single True --postfix 1g --worker 4 --dataset Spark > Spark_1g_4.statet 2>&1
python time_vs_nworkers.py --compress_single True --postfix 1g --worker 8 --dataset Spark > Spark_1g_8.statet 2>&1
python time_vs_nworkers.py --compress_single True --postfix 1g --worker 16 --dataset Spark > Spark_1g_16.statet 2>&1
python time_vs_nworkers.py --compress_single True --postfix 1g --worker 32 --dataset Spark > Spark_1g_32.statet 2>&1


python time_vs_nworkers.py --compress_single True --postfix 1g --worker 1 --dataset Thunderbird > Thunderbird_1g_1.statet 2>&1
python time_vs_nworkers.py --compress_single True --postfix 1g --worker 2 --dataset Thunderbird > Thunderbird_1g_2.statet 2>&1
python time_vs_nworkers.py --compress_single True --postfix 1g --worker 4 --dataset Thunderbird > Thunderbird_1g_4.statet 2>&1
python time_vs_nworkers.py --compress_single True --postfix 1g --worker 8 --dataset Thunderbird > Thunderbird_1g_8.statet 2>&1
python time_vs_nworkers.py --compress_single True --postfix 1g --worker 16 --dataset Thunderbird > Thunderbird_1g_16.statet 2>&1
python time_vs_nworkers.py --compress_single True --postfix 1g --worker 32 --dataset Thunderbird > Thunderbird_1g_32.statet 2>&1


python time_vs_nworkers.py --compress_single True --postfix 1g --worker 1 --dataset Windows > Windows_1g_1.statet 2>&1
python time_vs_nworkers.py --compress_single True --postfix 1g --worker 2 --dataset Windows > Windows_1g_2.statet 2>&1
python time_vs_nworkers.py --compress_single True --postfix 1g --worker 4 --dataset Windows > Windows_1g_4.statet 2>&1
python time_vs_nworkers.py --compress_single True --postfix 1g --worker 8 --dataset Windows > Windows_1g_8.statet 2>&1
python time_vs_nworkers.py --compress_single True --postfix 1g --worker 16 --dataset Windows > Windows_1g_16.statet 2>&1
python time_vs_nworkers.py --compress_single True --postfix 1g --worker 32 --dataset Windows > Windows_1g_32.statet 2>&1

