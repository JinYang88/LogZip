#！/bin/bash


#python run_1.py --file ../logs/HDFS_2k.log
#python run_1.py --file ../logs/Andriod_2k.log
#python run_1.py --file ../logs/Spark_2k.log
#python run_1.py --file ../logs/Thunder_2k.log
#python run_1.py --file ../logs/Windows_2k.log


nohup python run_1.py --file ../logs/Thunder_al > Thunder_al.l1 2>&1 &
nohup python run_1.py --file ../logs/Thunder_ao > Thunder_ao.l1 2>&1 &
nohup python run_1.py --file ../logs/Thunder_ag > Thunder_ag.l1 2>&1 &

nohup python run_1.py --file ../logs/Windows_al > Windows_al.l1 2>&1 &
nohup python run_1.py --file ../logs/Windows_ao > Windows_ao.l1 2>&1 &
nohup python run_1.py --file ../logs/Windows_ag > Windows_ag.l1 2>&1 &

nohup python run_2.py --file ../logs/Thunder_al > Thunder_al.l 2>&1 &
nohup python run_2.py --file ../logs/Thunder_ao > Thunder_ao.l 2>&1 &
nohup python run_2.py --file ../logs/Thunder_ag > Thunder_ag.l 2>&1 &

nohup python run_2.py --file ../logs/Windows_al > Windows_al.l 2>&1 &
nohup python run_2.py --file ../logs/Windows_ao > Windows_ao.l 2>&1 &
nohup python run_2.py --file ../logs/Windows_ag > Windows_ag.l 2>&1 &