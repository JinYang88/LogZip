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
                line = line.replace("logfile_path", "./logname")
            content.append(line)
    with open(os.path.join(dst, "generate_report.rb"), "w") as fw:
        fw.writelines(content)
    
    src = os.path.join("./1_CCGrid15/config.ini")
    dst = os.path.join(output_dir, "config.ini")
    try:
        shutil.copyfile(src, dst)    
    except Exception as e:
        print(e)
    
    os.chdir(output_dir)
    start = time.time()
    cmd = "ruby script/generate_report.rb"
    subprocess.call(cmd, stderr=subprocess.STDOUT, shell=True)
    end = time.time()
    time_taken = round(end-start, 3)
    os.chdir("../../")

#    outfiles = glob.glob(os.path.join(output_dir, "*.bz2"))
#    
#    compressed_size = sum([get_FileSize(file) for file in outfiles])
#    original_size = get_FileSize(filepath)
#    compress_ratio = round(original_size / compressed_size, 2)
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