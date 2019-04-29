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
    
def get_FileSize(filePath):
    fsize = os.path.getsize(filePath)
    fsize = fsize/float(1024*1024)
    return round(fsize, 2)

def runfile(filepath):
    timemark = time.strftime('%m%d-%H%M%S', time.localtime(time.time()))

    if os.path.basename(filepath).endswith(".log"):
        logname = os.path.basename(filepath).strip(".log")
    else:
        logname = os.path.basename(filepath)
    
    # init a new dir
    output_dir = os.path.join("./2_output", logname)
    if not os.path.isdir(output_dir):
        os.makedirs(output_dir)
    
    # run compression in the dir
    os.chdir(output_dir)
    shutil.copyfile(os.path.join("../../2_SIGMOD13/", "archiver.o"),
                    os.path.join("./archiver.o"))
    logpath = os.path.join("../../", filepath)
    cmd = f"cat {logpath} | ../../2_SIGMOD13/archiver.o -c --jhistory 10 --buckets 16 --est"
    start = time.time()
    subprocess.call(cmd, stderr=subprocess.STDOUT, shell=True)
    end = time.time()
    time_taken = round(end-start, 3)
    os.chdir("../../")
    
    outfiles = glob.glob(os.path.join(output_dir, "*.bz2"))
    
    compressed_size = sum([get_FileSize(file) for file in outfiles])
    original_size = get_FileSize(filepath)
    compress_ratio = round(original_size / compressed_size, 2)
    
    firstline = True
    if os.path.isfile("report_2.csv"):
        firstline = False
    with open(f"report_2.csv", "a+") as fw:
        if firstline:
            fw.write("timemark,logname,original_size,compressed_size,compress_ratio,time_taken\n")
        fw.write(f"{timemark},{logname},{original_size},{compressed_size},{compress_ratio},{time_taken}\n")
        
if __name__ == "__main__":
    runfile(args["file"])