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


    
split_regex = re.compile("([^a-zA-Z0-9]+)")

class Ziplog():
    def __init__(self, outdir, kernel="gz", level=3):
        self.outdir = outdir
        self.kernel = kernel
        self.io_time = 0
        self.level = level

    def directly_zip(self):
        ignore_columns = ["LineId", "EventTemplate", "ParameterList", "EventId"]
        focus_columns = [col for col in self.para_df.columns if col not in ignore_columns]
        for column in focus_columns:
            self.para_df[column].to_csv(os.path.join(self.tmp_dir, column+"_0.csv"), index=False)

    def zip_normal(self):
        ignore_columns = ["LineId", "EventTemplate", "ParameterList", "EventId"]
        focus_columns = [col for col in self.para_df.columns if col not in ignore_columns]
        splited_df = split_normal(self.para_df[focus_columns])
        self.para_df.drop(focus_columns, axis=1, inplace=True)
        
        t1 = time.time()
        for column_name in splited_df.columns:
            subdf = pd.DataFrame(splited_df[column_name].tolist())
            subdf.fillna("", inplace=True)
            subdf.columns = range(subdf.shape[1])
            for col in subdf.columns:
                filepath = os.path.join(self.tmp_dir, column_name + "_" + str(col) + ".csv")
                subdf[col].to_csv(filepath, index=False)
        self.para_df["EventId"].to_csv(os.path.join(self.tmp_dir, "EventId" + "_" + str(0) + ".csv"),
                             index=False)
        t2 = time.time()
        self.io_time += t2 - t1

        del splited_df
        gc.collect()

    def zip_content(self):
        template_mapping = dict(zip(self.para_df["EventId"], self.para_df["EventTemplate"]))
        with open(os.path.join(self.tmp_dir, "template_mapping.json"), "w") as fw:
            json.dump(template_mapping, fw)

        print("Splitting parameters.")
        t1 = time.time()
        filename_para_dict = defaultdict(list)
        eids = self.para_df["EventId"].unique()
        print("{} events total.".format(len(eids)))
        eids = [eid for eid in eids if "<*>" in template_mapping[eid]]
        print("{} events to be split.".format(len(eids)))
        filename_para_dict = split_para2(self.para_df.loc[self.para_df["EventId"].isin(eids), ["EventId", "ParameterList"]])
        t2 = time.time()
        print("Splitting parameters done. Time taken {:.2f}s".format(t2 - t1))
        del self.para_df
        gc.collect()

        if self.level == 3:
            print("Indexing parameters.")
            t1 = time.time()
            para_idx = 1
            para_idx_dict = {}
            idx_para_dict = {}
            para_idx_dict[""] = "0"
            idx_para_dict["0"] = ""
            for filename, para_lists in filename_para_dict.items():
                for para_list in para_lists:
                    for para in para_list:
                        if para not in para_idx_dict:
                            idx_64 = baseN((para_idx), 64)
                            para_idx_dict[para] = idx_64
                            idx_para_dict[idx_64] = para
                            para_idx += 1
            t2 = time.time()
            with open(os.path.join(self.tmp_dir, "parameter_mapping.json"), "w") as fw:
                json.dump(idx_para_dict, fw)
            print("Indexing parameters done. Time taken {:.2f}s".format(t2 - t1))
        else:
            para_idx_dict = None

        print("Saving parameters.")
        t1 = time.time()
        save_para(filename_para_dict, self.tmp_dir, para_idx_dict)
        t2 = time.time()
        self.io_time += t2 - t1
        print("Saving parameters done. Time taken {:.2f}s".format(t2 - t1))
    
    def zip_folder(self, zipname):
        allfiles = glob.glob(os.path.join(self.tmp_dir, "*.csv")) + glob.glob(os.path.join(self.tmp_dir, "*.json"))
        files_to_tar(allfiles, self.kernel)
        if self.kernel == "bz2" or self.kernel == "gz":
            tarall = tarfile.open(os.path.join(self.outdir, "{}.tar.{}".format(zipname, self.kernel)) , "w:{}".format(self.kernel))
            for idx, filepath in enumerate(glob.glob(os.path.join(self.tmp_dir, "*.tar.{}".format(self.kernel))), 1):
                tarall.add(filepath, arcname=os.path.basename(filepath))
            tarall.close()
        elif self.kernel == "lzma":
            tarall = tarfile.open(os.path.join(self.outdir, "{}.tar.{}".format(zipname, self.kernel)) , "w:bz2")
            for idx, filepath in enumerate(glob.glob(os.path.join(self.tmp_dir, "*.lzma")), 1):
                tarall.add(filepath, arcname=os.path.basename(filepath))
            tarall.close()

    def zip_para_df(self):
        if self.level == 1:
            self.directly_zip()
        else:
            self.para_df.drop("Content", inplace=True, axis=1)
            self.zip_normal()
            print("Zip content begin.")
            t1 = time.time()
            self.zip_content()
            t2 = time.time()
            print("Zip content done. Time taken: {:.2f}s".format(t2-t1))

        print("Zip folder begin.")
        t1 = time.time()
        self.zip_folder(zipname=self.outname)
        t2 = time.time()
        self.io_time += t2 - t1
        print("Zip folder done. Time taken: {:.2f}s".format(t2-t1))

    def zip_file(self, tmp_dir, outname , para_file_path=None, para_df=None, delete_tmp=True):
        self.outname = outname
        self.tmp_dir = tmp_dir

        t1 = time.time()
        if para_file_path:
            self.para_df = pd.read_csv(para_file_path, nrows=100000)
            t2 = time.time()
            print("Loading file done, Time taken: {:.2f}s".format(t2-t1))
            self.io_time += t2 - t1
        elif para_df is not None:
            self.para_df = para_df
            self.para_df.fillna("", inplace=True)
        self.zip_para_df()
        t3 = time.time()
        print("Zip log done, Time taken: all: {:.2f}s, IO: {:.2f}s, real: {:.2f}s".format(t3-t1, self.io_time, t3-t1-self.io_time))
        if delete_tmp:
            shutil.rmtree(self.tmp_dir)
    

def baseN(num, b):
    if isinstance(num, str):
        num = int(num)
    if num is None: return ""
    return ((num == 0) and "0") or \
            (baseN(num // b, b).lstrip("0") + "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz+="[num % b])

def split_normal(df):
    column_dict = {}
    for column in df.columns:
        df[column] =  df[column].map(lambda x: split_regex.split(x))
    return df

def split_para2(df):
    worker_id = os.getpid()
    filename_para_dict = defaultdict(list)
    print("Worker {} start splitting {} lines.".format(worker_id, df.shape[0]))
    for eidx, row in df.iterrows():
        eid = row["EventId"]
        for idx1, para in enumerate(row["ParameterList"]):
            para = split_regex.split(para)
            filename = "{}_{}".format(eid, idx1)
            filename_para_dict[filename].append(para)
    return filename_para_dict

def save_para(filename_para_dict, path, para_idx_dict=None):
    for filename in filename_para_dict:
        maxlen = sorted([len(v) for v in filename_para_dict[filename]])[-1]
        for col2 in range(maxlen):
            filepath = os.path.join(path, filename + "_" + str(col2) + ".csv")
            if para_idx_dict:
                lines = [para_idx_dict[item[col2]] if col2 < len(item) else para_idx_dict[""] for item in filename_para_dict[filename]]
            else:
                lines = [item[col2] if col2 < len(item) else "" for item in filename_para_dict[filename]]
            with open(filepath, "w") as fw:
                fw.writelines("\n".join(lines))

def files_to_tar(filepaths, kernel):
    worker_id = os.getpid()
    print("Worker {} start taring {} files.".format(worker_id, len(filepaths)))
    for idx, filepath in enumerate(filepaths, 1):
        if len(filepaths) > 10 and idx % (len(filepaths)// 10) == 0:
            print("Worker {}, {}/{}".format(worker_id, idx, len(filepaths)))
        if kernel == "gz" or kernel == "bz2":
            tar = tarfile.open(filepath + ".tar.{}".format(kernel) , "w:{}".format(kernel))
            tar.add(filepath, arcname=os.path.basename(filepath))
            tar.close()
        elif kernel == "lzma":
            os.system('lzma -k {}'.format(filepath))

def split_item(astr):
    return split_regex.split(astr)

def split_list(alist):
    return list(map(split_item, alist))

def split_para(seires):
    return seires.map(split_list)

def chunk_dict(data, SIZE=10000):
    it = iter(data)
    for i in range(0, len(data), SIZE):
        yield {k:data[k] for k in islice(it, SIZE)}
        
def gzip_dict(adict):
    return b"".join([gzip.compress(bytes(str({k:v}), encoding="utf-8"))\
                             for k, v in adict.items()])

def get_FileSize(filePath, unit="kb"):
    fsize = os.path.getsize(filePath)
    if unit == "mb":
        fsize = fsize/float(1024*1024)
    if unit == "kb":
        fsize = fsize/float(1024)
    return round(fsize, 2)



        
def __zip_file(filepath, tmp_dir, log_format, outname, level=3, top_event=2000, kernel="gz"):
    print("Tmp files are in {}".format(tmp_dir))
    if not os.path.isdir(tmp_dir):
        os.makedirs(tmp_dir)
        
    outdir = tmp_dir # output to current tmp dir
    parser = NaiveParser.LogParser(tmp_dir, outdir, log_format, n_workers=1, top_event=top_event)
    structured_log = parser.parse(filepath)
    
    zipper = Ziplog(outdir=outdir, kernel=kernel, level=level)
    
    #        tmp_dir, outname, filename, para_file_path=None, para_df=None, delete_tmp=True):
    
    zipper.zip_file(tmp_dir=tmp_dir, outname=outname,
                    para_df=structured_log, delete_tmp=False)
    

def zip_file(filepath, outdir, log_format, n_workers=2, level=3, top_event=2000, kernel="gz", report_file="./report.csv"):
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
    cmd = "split -b {}k {} {}".format(kb_per_chunk, filepath, os.path.join(tmp_dir, "tmplog_"))
    subprocess.call(cmd, stderr=subprocess.STDOUT, shell=True)
    
    
    # run subprocesses
    processes = []
    for idx, file in enumerate(glob.glob(os.path.join(tmp_dir, "tmplog_*"))):
        script_path = os.path.abspath(__file__)
        cmd = ('python {} --file {} --log_format "{}"'+ \
                ' --subprocess True --tmp_dir {}').format(script_path, file, log_format,
                                              os.path.join(tmp_dir, str(idx)))
        print(cmd)
        time.sleep(3)
        processes.append(subprocess.Popen(cmd, stderr=subprocess.STDOUT, shell=True))
    [p.wait() for p in processes]
#        processes.append(subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True))
#    [p.wait() for p in processes]
    
    compressed_size = 0
    for idx in range(len(processes)):
        sub_outfile = glob.glob(os.path.join(tmp_dir, str(idx), "*logzip*"))[0]
        basename = os.path.basename(sub_outfile)
        dst = os.path.join(outdir, basename + f".{idx}")
        shutil.move(sub_outfile, dst)
        compressed_size += get_FileSize(dst, "mb")

    
    [os.remove(chunk) for chunk in glob.glob(os.path.join(tmp_dir, "tmplog_*"))]
    original_size = get_FileSize(filepath, "mb")
    compress_ratio = round(original_size / compressed_size, 2)

    time_end = time.time()    
    total_time_taken = time_end - time_start

    firstline = True
    if os.path.isfile(report_file):
        firstline = False
    with open(report_file, "a+") as fw:
        if firstline:
            fw.write("timemark,logname,original_size,compressed_size,compress_ratio,time_taken,n_workers\n")
        fw.write(f"{timemark},{logname},{original_size},{compressed_size},{compress_ratio},{total_time_taken},{n_workers}\n")



def logzip(logfile, outdir, log_format, n_workers=1,
                 level=3, top_event=2000, kernel="gz", report_file='./report.csv'):
    args = None
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument('--file', type=str, default="../logs/HDFS_2k.log")
        parser.add_argument('--log_format', type=str, default="")
        parser.add_argument('--tmp_dir', type=str, default="")
        parser.add_argument('--subprocess', type=bool, default=False)
        args = vars(parser.parse_args())
    except:
        pass


    outname = os.path.basename(logfile) + ".logzip"
    if args and args["subprocess"]:
        __zip_file(args["file"], args["tmp_dir"], args["log_format"], outname=outname)
    else:
        zip_file(logfile, outdir, log_format, n_workers=n_workers,
                 level=level, top_event=top_event, kernel=kernel, report_file=report_file)
    
if __name__ == "__main__":
    import NaiveParser

    logfile       = "../../logs/HDFS_2k.log"  # Raw log file."
    outdir        = "../../zip_out/"  # Output directory, if not exists, it will be created.
    log_format    = '<Date> <Time> <Pid> <Level> <Component>: <Content>'  # Log format to extract fields.
    n_workers     = 2
    level         = 3
    top_event     = 2000
    kernel        = "gz"
    report_file   = "./report.csv"
        
    logzip(logfile, outdir, log_format, n_workers=n_workers,
                 level=level, top_event=top_event, kernel=kernel, report_file=report_file)
