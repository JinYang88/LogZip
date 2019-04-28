#！/bin/bash

nohup python run_2.py --file ../logs/error.log > error.l 2>&1 &
nohup python run_2.py --file ../logs/HDFS_2k.log > HDFS_2k.l 2>&1 &
nohup python run_2.py --file ../logs/HDFS.log > HDFS.l 2>&1 &
nohup python run_2.py --file ../logs/Andriod.log > Andriod.l 2>&1 &
nohup python run_2.py --file ../logs/Spark.log > Spark.l 2>&1 &