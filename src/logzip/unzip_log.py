import pandas as pd
import sys
import os
import tarfile
import glob
import multiprocessing as mp
import numpy as np
import re
import json
import pickle
import time
import itertools
from collections import defaultdict
import shutil


class Unziplog():
    def __init__(self, indir, outdir, log_format, n_workers, kernel="gz"):
        self.indir = indir
        self.outdir = outdir
        self.n_workers = n_workers
        self.log_format = log_format
        self.ori_df = pd.DataFrame()
        self.kernel = kernel
        self.headers = [item.strip('<>') for idx, item in enumerate(re.split(r'(<[^<>]+>)', log_format)) if idx % 2 != 0]


    def unzip(self, filename, outname, delete_tmp=True):
        filename = os.path.join(self.indir, filename)
        self.tmp_dir = os.path.join(self.outdir, os.path.basename(filename) + "_unzip_tmp")
        if os.path.isdir(self.tmp_dir):
            shutil.rmtree(self.tmp_dir)
        os.makedirs(self.tmp_dir)
        print("Tmp files are in ", self.tmp_dir)

        self.filename = filename
        self.outname = outname
        t1 = time.time()
        self.extract_all()
        t2 = time.time()
        print("Extract files done. Time taken [{:.2f}]".format(t2-t1))

        t1 = time.time()
        self.merge_normal()
        t2 = time.time()
        print("Merge normal columns done. Time taken [{:.2f}]".format(t2-t1))

        t1 = time.time()
        self.merge_para()
        t2 = time.time()
        print("Merge parameter columns done. Time taken [{:.2f}]".format(t2-t1))

        t1 = time.time()
        self.merge_non_para()
        t2 = time.time()
        print("Merge non parameter columns done. Time taken [{:.2f}]".format(t2-t1))

        t1 = time.time()
        self.dump_normal()
        t2 = time.time()
        
        if delete_tmp:
            shutil.rmtree(self.tmp_dir)

    def extract_all(self):
        if self.kernel == "lzma":
            extract_tar(tar_path=self.filename, target_path=self.tmp_dir, kernel="bz2")
            for tarfiles in glob.glob(os.path.join(self.tmp_dir, "*.lzma")):
                os.system("lzma -d {}".format(tarfiles))
        else:
            extract_tar(tar_path=self.filename, target_path=self.tmp_dir, kernel=self.kernel)
            for tarfiles in glob.glob(os.path.join(self.tmp_dir, "*.tar.{}".format(self.kernel))):
                extract_tar(tarfiles, self.tmp_dir, kernel=self.kernel)
        
        self.para_files = glob.glob(os.path.join(self.tmp_dir, "E*_*_*.csv"))
        with open(os.path.join(self.tmp_dir, "parameter_mapping.json")) as fr:
            self.para_mapping = json.load(fr)
        with open(os.path.join(self.tmp_dir, "template_mapping.json")) as fr:
            self.template_mapping = json.load(fr)

    def merge_para(self):
        self.para_prefix = set(list(map(lambda x: "_".join(x.split("_")[0:-1]), self.para_files)))
        self.para_prefix = sorted(list(self.para_prefix), key=lambda x: int(x.split("_")[-1]))
        self.para_eid = list(set(list(map(lambda x: "_".join(x.split("_")[0:-2]), self.para_files))))
        
        eid_content_dict = {}
        if self.n_workers == 1:
            eid_content_dict = parse_eid_para(self.para_eid, self.para_prefix, self.template_mapping, self.para_mapping)
        else:
            pool = mp.Pool(processes=self.n_workers)
            chunk_size = len(self.para_eid) // self.n_workers + 1
            result_chunks = [pool.apply_async(parse_eid_para, args=(self.para_eid[i:i + chunk_size],
                             self.para_prefix, self.template_mapping, self.para_mapping, ))
                             for i in range(0, len(self.para_eid), chunk_size)]
            pool.close()
            pool.join()
            for result in result_chunks:
                tmp_dict = result.get()
                eid_content_dict.update(tmp_dict)
        for eid in eid_content_dict:
            self.ori_df.loc[self.ori_df["EventId"] == eid, "Content"] = eid_content_dict[eid]

    def merge_normal(self):
        self.normal_files = list(filter(lambda x: x not in self.para_files, glob.glob(os.path.join(self.tmp_dir, "*.csv"))))
        self.normal_prefix = list(set(list(map(lambda x: "_".join(x.split("_")[0:-1]), self.normal_files))))
        
        colname_col_dict = {}
        if self.n_workers == 1:
            colname_col_dict = parse_normal(self.normal_prefix)
        else:
            pool = mp.Pool(processes=self.n_workers)
            chunk_size = len(self.normal_prefix) // self.n_workers + 1
            result_chunks = [pool.apply_async(parse_normal, args=(self.normal_prefix[i:i + chunk_size],))
                             for i in range(0, len(self.normal_prefix), chunk_size)]
            pool.close()
            pool.join()
            for result in result_chunks:
                tmp_dict = result.get()
                colname_col_dict.update(tmp_dict)      
        for colname, col in colname_col_dict.items():
            self.ori_df[colname] = col

    def merge_non_para(self):
        non_para_tempaltes_dict = {k: v for k, v in self.template_mapping.items() if "<*>" not in v}
        for eid in non_para_tempaltes_dict:
            self.ori_df.loc[self.ori_df["EventId"] == eid, "Content"] = non_para_tempaltes_dict[eid]

    def dump_normal(self):
        print("Dumping logs..")
        splitters = re.split(r'<[^<>]+>', self.log_format)
        self.ori_df = self.ori_df[self.headers].fillna("")
        for idx, item in enumerate(splitters):
            self.ori_df.insert(2*idx, column=str(idx), value=item.strip("\\?\(\)"))
        merged = self.ori_df.apply(lambda x: "".join(x), axis=1).tolist()
        try:
            with open(os.path.join(self.tmp_dir, "failed_logs.json"), "r") as fr:
                failed_lines_dict = json.load(fr)
            for lineid in sorted(failed_lines_dict.keys()):
                merged.insert(int(lineid), failed_lines_dict[lineid])
        except Exception as e:
            pass
        with open(os.path.join(self.outdir, self.outname), "w") as fw:
            fw.writelines("\n".join(merged))
    
    def dump_sturct(self):
        print("Dumping structured logs..")
        self.ori_df.to_csv(os.path.join(self.outdir, self.outname), index=False, columns=self.headers)

    def dump_tempalte(self):
        print("Dumping templates..")
        

def extract_tar(tar_path, target_path, kernel):
    try:
        if kernel == "gz" or kernel == "bz2":
            tar = tarfile.open(tar_path, "r:{}".format(kernel))
            file_names = tar.getnames()
            for file_name in file_names:
                tar.extract(file_name, target_path)
            tar.close()

    except Exception as e:
        print(e)

def parse_eid_para(eid_list, all_para_prefix, template_mapping, para_mapping):
    eid_content_dict = {}
    for eidx, para_eid in enumerate(eid_list, 1):
        eid = os.path.split(para_eid)[-1]
        # if eidx % (len(eid_list)//10) == 0:
        #     print("Processing eid [{}/{}]".format(eidx, len(eid_list)))
        para_df = pd.DataFrame()
        filter_para_prefix = [item for item in all_para_prefix if os.path.split(item)[-1].split("_")[0] == eid]
        for pos, para_prefix in enumerate(filter_para_prefix, 0):
            parafiles = sorted(glob.glob(para_prefix + "_*.csv"), key=lambda x: int(x.split("_")[-1].strip(".csv")))
            para = pd.concat([pd.read_csv(pf, header=None, dtype=str, skip_blank_lines=False, na_filter=False) for pf in parafiles], axis=1)
            para = para.apply(lambda x: "".join([para_mapping[item] if not pd.isnull(item) else ""  for item in x.tolist()]), axis=1)
            para_df[pos] = para
        full_df = pd.DataFrame()
        template = template_mapping[eid].split("<*>")
        df_col = 0
        for idx, item in enumerate(template):
            if idx < len(template) - 1:
                full_df[df_col + 1] = para_df[idx]
            full_df[df_col] = item
            df_col += 2
        full_df.fillna("", inplace=True)
        full_df["Content"] = full_df[list(range(len(full_df.columns)))].apply(lambda x: "".join(x), axis=1)
        eid_content_dict[eid] = full_df["Content"].tolist()
    return eid_content_dict

def parse_normal(normal_prefixs):
    colname_col_dict = {}
    for prefix in normal_prefixs:
        print(prefix)
        column_name = os.path.basename(prefix)
        files = sorted(glob.glob(os.path.join(prefix +  "*.csv")), key=lambda x: int(x.split("_")[-1].strip(".csv")))
        files = [name for name in files if os.path.basename(name).split("_")[0] == column_name]
        df = pd.concat([pd.read_csv(f, header=None, dtype=str, skip_blank_lines=False, na_filter=False) for f in files], axis=1)
        df.fillna("", inplace=True)
        column = df.apply(lambda x: "".join(x.tolist()), axis=1)
        colname_col_dict[column_name] = column
    return colname_col_dict


if __name__ == "__main__":
    n_workers = 2
    indir = "../HDFS_all_out"
    ziped_file = "HDFS_out_splited.tar.gz"
    outdir = "../HDFS_all_out/"
    outname = "HDFS.unzip.log"

    unzipper = Unziplog(outdir=outdir, n_workers=n_workers)
    unzipper.unzip(os.path.join(indir, ziped_file), outname)
