#！/bin/bash


python lossy.py --compress_single True --postfix 1g --worker 4 --dataset HDFS > HDFS_1g_lossy.statet 2>&1
python lossy.py --compress_single True --postfix 1g --worker 4 --dataset Spark > Spark_1g_lossy.statet 2>&1
python lossy.py --compress_single True --postfix 1g --worker 4 --dataset Andriod > Andriod.statet 2>&1
python lossy.py --compress_single True --postfix 1g --worker 4 --dataset Windows > Windows_1g_lossy.statet 2>&1
python lossy.py --compress_single True --postfix 1g --worker 4 --dataset Thunderbird > Thunderbird_1g_lossy.statet 2>&1
