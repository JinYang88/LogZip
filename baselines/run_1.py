# -*- coding: utf-8 -*-

import argparse
import os
import shutil
import time
import glob
import subprocess


            
parser = argparse.ArgumentParser()
parser.add_argument('--file', type=str, default="../logs/HDFS_2k.log")
args = vars(parser.parse_args())

#4;Timestamp;"[%d/%b/%Y:%H:%M:%S %z] "|1;IP
#timeformat = {
#        "Spark": '1;Timestamp;%y/%m/%d %H:%M:%S ', # 15/09/01 18:14:51 
#        "Windows": '1;Timestamp;"%Y-%m-%d %H:%M:%S", ',  # 2016-09-28 04:30:31
#        "Thunder": '3;Timestamp;%Y.%m.%d ', # 2005.11.09 
#        "Andriod": '1;Timestamp;%m-%d', # 04-23
#        "HDFS": '' # 04-23
#        }

timeformat = {
        "Spark": '',
        "Windows": '',
        "Thunder": '',
        "Andriod": '',
        "HDFS": '' # 04-23
        }

def get_FileSize(filePath):
    fsize = os.path.getsize(filePath)
    fsize = fsize/float(1024*1024)
    return round(fsize, 2)

def copytree(src, dst, symlinks=False, ignore=None):
    for item in os.listdir(src):
        s = os.path.join(src, item)
        d = os.path.join(dst, item)
        if os.path.isdir(s):
            shutil.copytree(s, d, symlinks, ignore)
        else:
            shutil.copy2(s, d)
            
def runfile(filepath):
    timemark = time.strftime('%m%d-%H%M%S', time.localtime(time.time()))
    logname = os.path.basename(filepath).strip(".log")
    # init a new dir
    output_dir = os.path.join("./1_output", logname)
    if not os.path.isdir(output_dir):
        os.makedirs(output_dir)
    
    # run compression in the dir
#    os.chdir(output_dir)
    dst_log = os.path.join(output_dir, logname)
    cmd = "ln {} {}".format(filepath, dst_log)
    try:
        subprocess.call(cmd, stderr=subprocess.STDOUT, shell=True)
    except Exception as e:
        print(e)
        
    src = os.path.join("./1_CCGrid15/bin")
    dst = os.path.join(output_dir, "bin")
    try:
        shutil.copytree(src, dst)
    except Exception as e:
        print(e)
# 
    src = os.path.join("./1_CCGrid15/script")
    dst = os.path.join(output_dir, "script")
    try:
        shutil.copytree(src, dst)
    except Exception as e:
        print(e)
    content = []
    with open(os.path.join(dst, "generate_report.rb"), "r") as fr:
        for line in fr.readlines():
            if "logfile_path" in line:
                line = line.replace("logfile_path", f"./{logname}")
            content.append(line)
    with open(os.path.join(dst, "generate_report.rb"), "w") as fw:
        fw.writelines(content)
    
    src = os.path.join("./1_CCGrid15/config.ini")
    dst = os.path.join(output_dir, "config.ini")
    try:
        shutil.copyfile(src, dst)    
    except Exception as e:
        print(e)
    content = []
    with open(os.path.join(dst), "r") as fr:
        for line in fr.readlines():
            if "TimeFormat" in line:
                for key in timeformat:
                    if key in logname:
                        line = line.replace("TimeFormat", timeformat[key])
                        print("Use timeformat {} for {}".format(timeformat[key], logname))
            content.append(line)
    with open(os.path.join(dst), "w") as fw:
        fw.writelines(content)
    
    os.chdir(output_dir)
    start = time.time()
    cmd = "ruby script/generate_report.rb"
    subprocess.call(cmd, stderr=subprocess.STDOUT, shell=True)
    end = time.time()
    time_taken = round(end-start, 3)
    os.chdir("../../")

    outfiles = glob.glob(os.path.join(output_dir, "*.mdl")) + \
               glob.glob(os.path.join(output_dir, "*.aux")) + \
               glob.glob(os.path.join(output_dir, "*.dat")) + \
               glob.glob(os.path.join(output_dir, "*.idx"))
    
    compressed_size = round(sum([get_FileSize(file) for file in outfiles]), 2)
    original_size = get_FileSize(filepath)
    compress_ratio = round(original_size / compressed_size, 2)
#    
    firstline = True
    if os.path.isfile("report_1.csv"):
        firstline = False
    with open(f"report_1.csv", "a+") as fw:
        if firstline:
            fw.write("timemark,logname,original_size,compressed_size,compress_ratio,time_taken\n")
        fw.write(f"{timemark},{logname},{original_size},{compressed_size},{compress_ratio},{time_taken}\n")
        
if __name__ == "__main__":
    runfile(args["file"])