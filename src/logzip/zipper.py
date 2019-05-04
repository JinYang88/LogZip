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

    
#split_chars = "([\.\-_\:\,\*&#@|{}() $]+)"
#split_regex = re.compile(split_chars)

split_regex = re.compile("([^a-zA-Z0-9]+)")

def boolean_string(s):
    if s not in {'False', 'True'}:
        raise ValueError('Not a valid boolean string')
    return s == 'True'

def split_item(astr):
    return split_regex.split(astr)

def split_list(alist):
    return list(map(split_item, alist))

def split_para(seires):
    return seires.map(split_list)

def split_normal(dataframe):
    return [dataframe[col].map(split_item).tolist() \
            for col in dataframe.columns]

def baseN(num, b):
    if isinstance(num, str):
        num = int(num)
    if num is None: return ""
    return ((num == 0) and "0") or \
            (baseN(num // b, b).lstrip("0") + "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz+="[num % b])

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


class Ziplog():
    def __init__(self, outdir, outname, tmp_dir, kernel="gz", level=3, compress_single=False, n_workers=1):
        self.outname = outname
        self.outdir = outdir
        self.tmp_dir = tmp_dir
        self.kernel = kernel
        self.level = level
        self.compress_single = compress_single        
        self.n_workers = n_workers

        self.transpose_time = 0
        self.packing_time = 0
        self.field_extraction_time = 0
        self.mapping_time = 0
        self.all_filename_obj_dict = {}
        self.file_normal_column_dict = {}

    def compress_all(self):
        self.file_all_column_dict = {}
        ignore_columns = ["LineId", "EventTemplate", "ParameterList", "EventId"]
        focus_columns = [col for col in self.para_df.columns if col not in ignore_columns]
        for column in focus_columns:
            filename = column+"_0"
            self.file_all_column_dict[filename] = self.para_df[column]
        del self.para_df

    def compress_normal(self):
        ignore_columns = ["LineId", "EventTemplate", "ParameterList", "EventId", "Content"]
        focus_columns = [col for col in self.para_df.columns if col not in ignore_columns]
        
        t1 = time.time()
        splited_columns = split_normal(self.para_df[focus_columns])
        t2 = time.time()
        self.field_extraction_time += t2 - t1 

        self.para_df.drop(focus_columns, axis=1,inplace=True)
        
        ## Writing begin
        t1 = time.time()
        for idx, colname in enumerate(focus_columns):
            max_col_num = len( sorted(splited_columns[idx], key=lambda x: len(x))[-1] )
            buffers = [[] for _ in range(max_col_num)]
            for row in splited_columns[idx]:
                lrow = len(row)
                for col_idx in range(max_col_num):
                    if col_idx < lrow:
                        buffers[col_idx].append(row[col_idx] + "\n")
                    else:
                        buffers[col_idx].append("\n")
            for sub_idx in range(max_col_num):
                with open( os.path.join(self.tmp_dir, f"{colname}_{sub_idx}.csv"), "w") as fw:
                    fw.writelines(buffers[sub_idx])
        with open(os.path.join(self.tmp_dir, "Event_Id_0.csv"), "w") as fw:
            fw.writelines([item + "\n" for item in self.para_df["EventId"]])
        t2 = time.time()
        self.transpose_time += t2 - t1
        ## Writing end


    def __pack_params(self, dataframe):
        '''
        Input: dataframe with tow columns [EventId, ParameterList]
        '''
        self.buffer_time = 0
        self.writing_time = 0
        for eid in dataframe["EventId"].unique():
            t1 = time.time()         
            paras = dataframe.loc[dataframe["EventId"]==eid, "ParameterList"].tolist()
            star_position = len(paras[0])
            star_split_mapping = {}
            for star_idx in range(star_position):
                max_split_position = len(sorted(paras, key=\
                                                lambda x: len(x[star_idx]))[-1][star_idx])
                star_split_mapping[star_idx] = max_split_position


            buffers = [[] for star_idx in range(star_position)
                for split_idx in range(star_split_mapping[star_idx])]
                
            for row in paras:
                writer_idx = 0
                for star_idx in range(star_position):
                    split_position = star_split_mapping[star_idx]
                    max_row_split_len = len(row[star_idx])
                    for split_idx in range(split_position):
                        if split_idx < max_row_split_len:
                            if self.level == 3:
                                buffers[writer_idx].append(\
                                       self.para_index_dict[row[star_idx][split_idx]] + "\n")
                            else:
                                buffers[writer_idx].append(row[star_idx][split_idx] + "\n")
                        else:
                            buffers[writer_idx].append("\n")
                        writer_idx += 1
            t2 = time.time()
            self.buffer_time += t2 - t1

            idx = 0
            for star_idx in range(star_position):
                split_position = star_split_mapping[star_idx]
                for split_idx in range(split_position):
                    with open(os.path.join(self.tmp_dir,\
                                              f"{eid}_{star_idx}_{split_idx}.csv"), "w") as fw:
                        fw.writelines(buffers[idx])
                        idx += 1
                        
            t3 = time.time()
            self.writing_time += t3 - t2
        print(f"Buffer taken {self.buffer_time}.")
        print(f"Write lines taken {self.writing_time}.")
            
            
    def __build_para_index(self, dataframe):
        index = 0
        self.para_index_dict = {}
        for paras in dataframe["ParameterList"]:
            para_set = set([item for sublist in paras for item in sublist])
            for upara in para_set:
                index += 1
                index_64 = baseN(index, 64)
                self.para_index_dict[upara] = index_64
        self.index_para_dict = {v:k for k,v in self.para_index_dict.items()}
        
                
        
    def compress_content(self):
        self.template_mapping = dict(zip(self.para_df["EventId"],\
                                    self.para_df["EventTemplate"]))
        eids = [eid for eid in self.template_mapping \
                    if "<*>" in self.template_mapping[eid]]
        print("{} events to be split.".format(len(eids)))
        focus_index = self.para_df["EventId"].isin(eids)
        focus_df = self.para_df.loc[focus_index,\
                                    ["EventId","ParameterList"]]
        del self.para_df
        
        ### EXTRACT FIELD begin
        t1 = time.time()
        splitted_para = split_para(focus_df["ParameterList"])
        t2 = time.time()
        focus_df["ParameterList"] = splitted_para
        self.field_extraction_time += t2 - t1
        ### EXTRACT FIELD end
        
        
        ### MAPPING begin
        t1 = time.time()
        if self.level == 3:
            self.__build_para_index(focus_df)
        t2 = time.time()
        self.mapping_time += t2 - t1
        ### MAPPING end
        
        
        ### PACKING begin
        t1 = time.time()
        self.__pack_params(focus_df)
        t2 = time.time()
        self.transpose_time += t2 - t1
        ### PACKING end
        
        gc.collect()


    def __kernel_compress(self):
        '''
        level1 : only normal
        [self.file_all_column_dict]
        ---
        level2 : parse without index 
        [self.file_para_dict, self.file_normal_column_dict]
        ---
        leve3: parse and index 
        [self.file_para_dict, self.file_normal_column_dict]
        '''
        
        def output_dict(adict):
            for filename, content_list in adict.items():
                with open(os.path.join(self.tmp_dir, filename+".csv"), "w") as fw:
                    fw.writelines("\n".join(list(content_list)))
        
        ## merge begin
        with open(os.path.join(self.tmp_dir, "template_mapping.json"), "w") as fw:
            json.dump(self.template_mapping, fw)

        if self.level==3:
            with open(os.path.join(self.tmp_dir, "parameter_mapping.json"), "w") as fw:
                json.dump(self.index_para_dict, fw)        

        gzip_file_name = os.path.join(self.outdir, \
                                  "{}.tar.{}".format(self.outname, self.kernel))
        
        # compress begin (compress all into one)
        
        if not self.compress_single:
            cmd = f'tar zcvf {gzip_file_name} {self.tmp_dir}'
            subprocess.check_output(cmd, shell=True)
        else:
    #        # compress begin (compress each one and then to one)
            files = glob.glob(os.path.join(self.tmp_dir, "*"))
            for files in np.array_split(files, self.n_workers):
                for file in files:
                    per_outname = file + ".tar.gz"
                    cmd = f'tar zcvf {per_outname} {file}'
                    processes = [subprocess.Popen(cmd, shell=True)]
                [p.wait() for p in processes]
            [os.remove(file) for file in glob.glob(os.path.join(self.tmp_dir, "*.csv"))]
            
            cmd = f'tar zcvf {gzip_file_name} {self.tmp_dir}'
            subprocess.check_output(cmd, shell=True)  

        
    def zip_file(self, para_df=None, delete_tmp=True):
        self.para_df = para_df.fillna("")
        
        if self.level == 1:
            self.compress_all()
        elif self.level == 2 or self.level==3:
            self.compress_normal()
            self.compress_content()
            
        t1 = time.time()
        self.__kernel_compress()
        t2 = time.time()
        self.packing_time += t2 - t1

        
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