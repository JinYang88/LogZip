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

split_regex = re.compile("([^a-zA-Z0-9]+)")

class Ziplog():
    def __init__(self, outdir, outname, kernel="gz", tmp_dir="", level=3, n_workers=1, compress_single=True):
        self.outdir = outdir
        self.outname = outname
        self.kernel = kernel
        self.io_time = 0
        self.level = level
        self.tmp_dir = tmp_dir
        self.n_workers = n_workers
        self.compress_single =compress_single
        

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

    def zip_file(self, para_file_path=None, para_df=None, delete_tmp=False):
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

        
def main():
    args = None
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument('--file', type=str, default="../../logs/HDFS_2k.log")
        parser.add_argument('--log_format', type=str, default="<Date> <Time> <Pid> <Level> <Component>: <Content>")
        parser.add_argument('--tmp_dir', type=str, default="../../zip_out/tmp_dir")
        parser.add_argument('--out_dir', type=str, default="../../zip_out/")
        parser.add_argument('--compress_single', type=boolean_string, default=False)
        parser.add_argument('--n_workers', type=int, default=1)
        parser.add_argument('--level', type=int, default=3)
        parser.add_argument('--top_event', type=int, default=2000)        
        parser.add_argument('--kernel', type=str, default="gz")
        args = vars(parser.parse_args())
    except Exception as e:
        print(e)
        pass
    
    filepath = args["file"] 
    kernel = args["kernel"]
    log_format = args["log_format"]
    top_event = args["top_event"]
    compress_single = args["compress_single"]
    n_workers = args["n_workers"]
    level = args["level"]
    tmp_dir = args["tmp_dir"]
    out_dir = args["out_dir"]
    
    
    logname = os.path.basename(filepath)
    outname = logname + ".logzip"
    print("Tmp files are in {}".format(tmp_dir))
    
    if not os.path.isdir(tmp_dir):
        os.makedirs(tmp_dir)
    if not os.path.isdir(out_dir):
        os.makedirs(out_dir)
        
    parser = NaiveParser.LogParser(tmp_dir, out_dir,
                                   log_format,
                                   top_event=top_event)
    structured_log = parser.parse(filepath, dump=False)
    
   
        
    zipper = Ziplog(outdir=out_dir,
                    outname=outname,
                    kernel=kernel,
                    tmp_dir=tmp_dir,
                    level=level,
                    compress_single=compress_single,
                    n_workers=n_workers)
    
    zipper.zip_file(para_df=structured_log)
    
    
if __name__ == "__main__":
    import NaiveParser
    main()