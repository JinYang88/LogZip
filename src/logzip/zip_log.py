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

#split_regex = re.compile("([^a-zA-Z0-9]+)")

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
        if self.n_workers == 1:
            splited_columns = split_normal(self.para_df[focus_columns])
        else:
            chunk_size = min(1000000, self.para_df.shape[0] // self.n_workers)
            result_chunks = []
            pool = mp.Pool(processes=self.n_workers)
            result_chunks = [pool.apply_async(split_normal,\
                            args=(self.para_df[focus_columns].iloc[i:i+chunk_size],))
                            for i in range(0, self.para_df.shape[0], chunk_size)]
            pool.close()
            pool.join()
            splited_columns = [[] for _ in range(len(focus_columns))] 
            for result in result_chunks:
                for idx, col in enumerate(result.get()):
                    splited_columns[idx].extend(col)
        t2 = time.time()
        self.field_extraction_time += t2 - t1 

        self.para_df.drop(focus_columns, axis=1,inplace=True)
        
        ## PACKING begin
        t1 = time.time()
        self.file_normal_column_dict = {}
        for idx, colname in enumerate(focus_columns):
            columns_t = list(zip_longest(*splited_columns[idx], fillvalue=""))  # transpose
            for sub_idx, col in enumerate(columns_t):
                filename = f"{colname}_{sub_idx}"
                self.file_normal_column_dict[filename] = col
        self.file_normal_column_dict["EventId_0"] = self.para_df["EventId"]
        t2 = time.time()
        ## PACKING end
        self.transpose_time += t2 - t1


    def __pack_params(self, dataframe):
        '''
        Input: dataframe with tow columns [EventId, ParameterList]
        '''
        self.file_para_dict = {}
        for eid in dataframe["EventId"].unique():
            paras = dataframe.loc[dataframe["EventId"]==eid, "ParameterList"]
            paracolumns = list(zip_longest(*paras, fillvalue=""))
            for para_idx, subparas in enumerate(paracolumns):
                filename = f"{eid}_{para_idx}"
                self.file_para_dict[filename] = subparas
                
#                subparas_columns = list(zip_longest(*subparas, fillvalue=""))
#                for sub_para_idx, sub_subparas in enumerate(subparas_columns):
#                    filename = f"{eid}_{para_idx}_{sub_para_idx}"
#                    self.file_para_dict[filename] = sub_subparas


    def __build_para_index(self):
        index = 0
        para_index_dict = {}
        for filename, paras in self.file_para_dict.items():
            para_set = set(paras)
            for upara in para_set:
                index += 1
                index_64 = baseN(index, 64)
                para_index_dict[upara] = index_64
            paras_mapped = [para_index_dict[para] for para in paras]
            self.file_para_dict[filename] = paras_mapped
        self.index_para_dict = {v:k for k,v in para_index_dict.items()}
        
                
        
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
#        t1 = time.time()
#        if self.n_workers == 1:
#            splitted_para = split_para(focus_df["ParameterList"])
#        else:
#            chunk_size = min(1000000, 1 + focus_df.shape[0] // self.n_workers)
#            result_chunks = []
#            pool = mp.Pool(processes=self.n_workers)
#            result_chunks = [pool.apply_async(split_para,\
#                             args=(focus_df["ParameterList"].iloc[i:i+chunk_size],))
#                             for i in range(0, focus_df.shape[0], chunk_size)]
#            pool.close()
#            pool.join()
#            splitted_para = []
#            [splitted_para.extend(_.get()) for _ in result_chunks]
#        t2 = time.time()
#        focus_df["ParameterList"] = splitted_para
#        self.field_extraction_time += t2 - t1
        ### EXTRACT FIELD end
        
        
        ### PACKING begin
        t1 = time.time()
        self.__pack_params(focus_df)
        t2 = time.time()
        self.transpose_time += t2 - t1
        ### PACKING end
        
        
        ### MAPPING begin
        t1 = time.time()
        if self.level == 3:
            self.__build_para_index()
        t2 = time.time()
        self.mapping_time += t2 - t1
        ### MAPPING end
        
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
        
        def files_to_tar(filepaths):
            worker_id = os.getpid()
            print("Worker {} start taring {} files.".format(worker_id, len(filepaths)))
            for idx, filepath in enumerate(filepaths, 1):
                if len(filepaths) > 10 and idx % (len(filepaths)// 10) == 0:
                    print("Worker {}, {}/{}".format(worker_id, idx, len(filepaths)))
                if self.kernel == "gz" or self.kernel == "bz2":
                    tar = tarfile.open(filepath + ".tar.{}".format(self.kernel ),\
                                       "w:{}".format(self.kernel ))
                    tar.add(filepath, arcname=os.path.basename(filepath))
                    tar.close()
                elif self.kernel  == "lzma":
                    os.system('lzma -k {}'.format(filepath)) 
            
        ## merge begin
        if self.level==3:        
            self.all_filename_obj_dict[os.path.join(self.tmp_dir, "template_mapping.json")] \
                                    = self.template_mapping
        
        if self.level > 1:
            self.all_filename_obj_dict[os.path.join(self.tmp_dir, "parameter_mapping.json")] \
                                    = self.index_para_dict
        
        if self.level == 1:
            for k,v in self.file_all_column_dict.items():
                self.all_filename_obj_dict[k] = v
            del self.file_all_column_dict
        elif self.level == 2 or self.level == 3:
            for k,v in self.file_para_dict.items():
                self.all_filename_obj_dict[k] = v
            for k,v in self.file_normal_column_dict.items():
                self.all_filename_obj_dict[k] = v
            del self.file_normal_column_dict
            del self.file_para_dict
        else:
            raise RuntimeError(f"The level {self.level} is illegal!")
        ## merge end
        
        print("Packing {} files.".format(len(self.all_filename_obj_dict)))

        # compress begin
        gzip_file_name = os.path.join(self.outdir, \
                                  "{}.tar.{}".format(self.outname, self.kernel))

        if self.n_workers == 1:
            gzipped_bytes = gzip_dict(self.all_filename_obj_dict)
        else:
            chunk_size = max(100, len(self.all_filename_obj_dict) // self.n_workers)
            pool = mp.Pool(processes=self.n_workers)
            gzipped_bytes_list = [pool.apply_async(gzip_dict,\
                            args=(adict,))
                            for adict in chunk_dict(self.all_filename_obj_dict, chunk_size)]
            pool.close()
            pool.join()
            gzipped_bytes = b"".join([result.get() for result in gzipped_bytes_list])

        t1 = time.time()
        with open(gzip_file_name, "wb") as fw:
            fw.write(gzipped_bytes)
        t2 = time.time()
        print(f"writing for {t2-t1:.3f}s")
        # compress end

        
    def zip_file(self, outname, filename, para_df=None, delete_tmp=True):
        self.outname = outname
        self.tmp_dir = os.path.join(self.outdir, filename + "_tmp")
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
        
                
    

if __name__ == "__main__":
    import NaiveParser
    
    n_workers     = 2  # Number of processes.
    level         = 3  # Compression level.
    top_event     = 2000 # Only templates whose occurrence is ranked above top_event are taken into consideration.
    kernel        = "gz"  # Compression kernels. Options: "gz", "bz2", "lzma".
    log_format    = '<Date> <Time> <Pid> <Level> <Component>: <Content>'  # Log format to extract fields.
    
#    logfile       = "HDFS_1g.log"  # Raw log file.
#    logfile       = "HDFS_100MB.log"
    logfile       = "HDFS_2k.log"  # Raw log file."
    indir         = "../../logs/"  # Input directory
    outdir        = "../../zip_out/"  # Output directory, if not exists, it will be created.
    outname       = logfile + ".nnlogzip"  # Output file name.
    
    parser = NaiveParser.LogParser(indir, outdir, log_format, n_workers=n_workers, top_event=top_event)
    structured_log = parser.parse(logfile)
    zipper = Ziplog(outdir=outdir, n_workers=n_workers, kernel=kernel, level=level)
    zipper.zip_file(outname=outname, filename=logfile, para_df=structured_log)
