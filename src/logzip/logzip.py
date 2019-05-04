import zlib
import pandas as pd
import sys
import os
import tarfile
import glob
import multiprocessing as mp
import re
import json
import pickle
import time
import itertools
import shutil
import lzma
import gc
from io import StringIO
import gzip
from collections import defaultdict
from itertools import zip_longest
from itertools import islice
import pickle
import subprocess
import argparse
import numpy as np
    

def boolean_string(s):
    if s not in {'False', 'True'}:
        raise ValueError('Not a valid boolean string')
    return s == 'True'

def get_FileSize(filePath, unit="kb"):
    fsize = os.path.getsize(filePath)
    if unit == "mb":
        fsize = fsize/float(1024*1024)
    if unit == "kb":
        fsize = fsize/float(1024)
    return round(fsize, 2)

def zip_file(filepath, outdir, log_format, n_workers=2,
             level=3, top_event=2000, kernel="gz", compress_single=False,
             report_file="./report.csv"):
    time_start = time.time()

    # new tmp dirs
    logname = os.path.basename(filepath)
    timemark = time.strftime('%Y%m%d-%H%M%S', time.localtime(time.time()))
    tmp_dir = os.path.join(outdir, logname + "_tmp_" + timemark)
    print("Tmp files are in {}".format(tmp_dir))
    if os.path.isdir(tmp_dir):
        shutil.rmtree(tmp_dir)
    if not os.path.isdir(tmp_dir):
        os.makedirs(tmp_dir)
        
    
    # split files
    kb_per_chunk = int(get_FileSize(filepath) // n_workers) + 1
    cmd = "split -b {}k {} {}".format(kb_per_chunk, filepath, os.path.join(tmp_dir, f"{logname}_"))
    subprocess.call(cmd, stderr=subprocess.STDOUT, shell=True)
    
    
    # run subprocesses
    processes = []
    for idx, file in enumerate(sorted(glob.glob(os.path.join(tmp_dir, f"{logname}_*")))):
        script_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "zipper_longgest.py")
        per_tmp_dir = os.path.join(tmp_dir, str(idx))
        cmd = ('python {} --file {} --log_format "{}"'+ \
                ' --tmp_dir {} --out_dir {} --compress_single {} --n_workers {}') \
                                            .format(script_path, file, log_format,
                                              per_tmp_dir, per_tmp_dir,
                                              compress_single, n_workers)
        print(cmd)
        processes.append(subprocess.Popen(cmd, stderr=subprocess.STDOUT, shell=True))
    [p.wait() for p in processes]
    
    compressed_size = 0
    for idx in range(len(processes)):
        sub_outfile = glob.glob(os.path.join(tmp_dir, str(idx), "*logzip*"))[0]
        dst = os.path.join(outdir, os.path.basename(sub_outfile) + f".{idx+1}of{len(processes)}")
        shutil.move(sub_outfile, dst)
        compressed_size += get_FileSize(dst, "mb")

    [os.remove(chunk) for chunk in glob.glob(os.path.join(tmp_dir, f"{logname}_*"))]
    original_size = get_FileSize(filepath, "mb")
    compress_ratio = round(original_size / compressed_size, 2)

    time_end = time.time()    
    total_time_taken = time_end - time_start

    firstline = True
    if os.path.isfile(report_file):
        firstline = False
    with open(report_file, "a+") as fw:
        if firstline:
            fw.write("timemark,logname,original_size,compressed_size,compress_ratio,time_taken,n_workers,compress_single\n")
        fw.write(f"{timemark},{logname},{original_size},{compressed_size},{compress_ratio},{total_time_taken},{n_workers},{compress_single}\n")


if __name__ == "__main__":
    logfile       = "../../logs/HDFS_100MB.log"  # Raw log file."
    outdir        = "../../zip_out/"  # Output directory, if not exists, it will be created.
    log_format    = '<Date> <Time> <Pid> <Level> <Component>: <Content>'  # Log format to extract fields.
    n_workers     = 3
    level         = 3
    top_event     = 2000
    kernel        = "gz"
    compress_single = False
    report_file   = "./report.csv"
             
             
    zip_file(logfile, outdir, log_format, n_workers=n_workers,
                 level=level,
                 top_event=top_event,
                 kernel=kernel,
                 compress_single=compress_single,
                 report_file=report_file)