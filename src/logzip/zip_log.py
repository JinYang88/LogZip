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


parser = argparse.ArgumentParser()
parser.add_argument('--file', type=str, default="../logs/HDFS_2k.log")
parser.add_argument('--log_format', type=str, default="")
parser.add_argument('--tmp_dir', type=str, default="")
parser.add_argument('--subprocess', type=bool, default=False)


args = vars(parser.parse_args())


split_chars = "([\.\-_\:\,\*&#@|{}() $]+)"
split_regex = re.compile(split_chars)

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

def get_FileSize(filePath):
    fsize = os.path.getsize(filePath)
    fsize = fsize/float(1024)
    return round(fsize, 2)


class Ziplog():
    def __init__(self, outdir, n_workers, kernel="gz", level=3):
        self.outdir = outdir
        self.n_workers = n_workers
        self.kernel = kernel
        self.level = level
        
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
            file_writers = [open( os.path.join(self.tmp_dir, f"{colname}_{sub_idx}.csv"), "w")
                            for sub_idx in range(max_col_num)]
            buffers = [[] for _ in file_writers]
            for row in splited_columns[idx]:
                lrow = len(row)
                for col_idx in range(max_col_num):
                    if col_idx < lrow:
                        buffers[col_idx].append(row[col_idx] + "\n")
                    else:
                        buffers[col_idx].append("\n")
            [fw.writelines(buffers[idx]) for idx, fw in enumerate(file_writers)]
            [fw.close() for fw in file_writers]
        self.file_normal_column_dict["EventId_0"] = self.para_df["EventId"]
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

        with open(os.path.join(self.tmp_dir, "parameter_mapping.json"), "w") as fw:
            json.dump(self.index_para_dict, fw)        

        # compress begin
        gzip_file_name = os.path.join(self.outdir, \
                                  "{}.tar.{}".format(self.outname, self.kernel))
        os.system(f'tar zcvf {gzip_file_name} {self.tmp_dir}')

        
    def zip_file(self, outname, filename, tmp_dir, para_df=None, delete_tmp=True):
        self.outname = outname
        self.tmp_dir = tmp_dir
        self.para_df = para_df.fillna("")
        if os.path.isdir(self.tmp_dir):
            shutil.rmtree(self.tmp_dir)
        os.makedirs(self.tmp_dir)
        
        if self.level == 1:
            self.compress_all()
        elif self.level == 2 or self.level==3:
            self.compress_normal()
            self.compress_content()
            
        t1 = time.time()
        self.__kernel_compress()
        t2 = time.time()
        self.packing_time += t2 - t1

        
def __zip_file(filepath, tmp_dir, log_format, n_workers=1, level=3, top_event=2000, kernel="gz"):
    print("Tmp files are in {}".format(tmp_dir))
    if os.path.isdir(tmp_dir):
        shutil.rmtree(tmp_dir)
    if not os.path.isdir(tmp_dir):
        os.makedirs(tmp_dir)
    outname = os.path.basename(filepath) + ".logzip"
    outdir = tmp_dir
    parser = NaiveParser.LogParser(tmp_dir, outdir, log_format, n_workers=n_workers, top_event=top_event)
    structured_log = parser.parse(logfile)
    
    zipper = Ziplog(outdir=outdir, n_workers=n_workers, kernel=kernel, level=level)
    zipper.zip_file(outname=outname, filename=logfile, tmp_dir=tmp_dir,
                    para_df=structured_log)
    

def zip_file(filepath, outdir, log_format, n_workers=2, level=3, top_event=2000, kernel="gz"):
    # split file into files
    logname = os.path.basename(filepath)
    timemark = time.strftime('%Y%m%d-%H%M%S', time.localtime(time.time()))
    tmp_dir = os.path.join(outdir, logname + "_tmp_" + timemark)
    print("Tmp files are in {}".format(tmp_dir))
    if os.path.isdir(tmp_dir):
        shutil.rmtree(tmp_dir)
    if not os.path.isdir(tmp_dir):
        os.makedirs(tmp_dir)
        
    kb_per_chunk = int(get_FileSize(filepath) // n_workers) + 1
    cmd = "split -b {}k {} {}".format(kb_per_chunk, filepath, os.path.join(tmp_dir, "tmplog_"))
#    print("run:", cmd)
    subprocess.call(cmd, stderr=subprocess.STDOUT, shell=True)
    
    for idx, file in enumerate(glob.glob(os.path.join(tmp_dir, "tmplog_*"))):
        cmd = f'python ./zip_log.py --file {file} --log_format "{log_format}" --subprocess True --tmp_dir {tmp_dir}_{idx}'
#        print("run", cmd)
        out = subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True)
        print(out)
    
if __name__ == "__main__":
    import NaiveParser
    logfile       = "../../logs/HDFS_2k.log"  # Raw log file."
    outdir        = "../../zip_out/"  # Output directory, if not exists, it will be created.
    log_format    = '<Date> <Time> <Pid> <Level> <Component>: <Content>'  # Log format to extract fields.
    
    if args["subprocess"]:
        __zip_file(logfile, args["tmp_dir"], log_format)
    else:
        zip_file(logfile, outdir, log_format)
    
    

