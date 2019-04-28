#！/bin/bash

nohup python run_2.py ../logs/error.log > error.l 2>&1 &
nohup python run_2.py ../logs/HDFS_2k.log > HDFS_2k.l 2>&1 &
nohup python run_2.py ../logs/HDFS.log > HDFS.l 2>&1 &
nohup python run_2.py ../logs/Andriod.log > Andriod.l 2>&1 &
nohup python run_2.py ../logs/Spark.log > Spark.l 2>&1 &