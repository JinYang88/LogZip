"""
Description: This file implements the naive algorithm for log parsing
Author: LogPAI team
License: MIT
"""

import sys
import os
import re
import multiprocessing as mp
import numpy as np
import hashlib
import pandas as pd
try:
    from . import logloader
except:
    import logloader
from collections import defaultdict
from datetime import datetime
import shutil
import time

class LogParser(object):
    def __init__(self, indir, outdir, log_format, top_event=2000, rex=[], n_workers=1):
        self.indir = indir
        self.outdir = outdir
        self.log_format = log_format
        self.rex = rex
        self.n_workers = n_workers
        self.log_dataframe = pd.DataFrame()
        self.top_event = top_event
        
        self.field_extraction_time = 0
        self.parse_time = 0

    def preprocess(self, x):
        for currentRex in self.rex:
            x = re.sub(currentRex, '<*>', x)
        return x

    def read_data(self, logname):
        timemark = time.strftime('%Y%m%d-%H%M%S', time.localtime(time.time()))
        self.tmp_dir = os.path.join(self.outdir, logname + "_tmp_" + timemark)
        print("Tmp files are in {}".format(self.tmp_dir))
        if os.path.isdir(self.tmp_dir):
            shutil.rmtree(self.tmp_dir)
        if not os.path.isdir(self.tmp_dir):
            os.makedirs(self.tmp_dir)
        
        ########## Field Extraction TIME begin
        start_extract_time = time.time()
        loader = logloader.LogLoader(self.log_format, self.tmp_dir, self.n_workers)
        self.log_dataframe = loader.load_to_dataframe(os.path.join(self.indir, logname))
        end_extract_time = time.time()
        self.field_extraction_time = end_extract_time - start_extract_time
        ########## Field Extraction TIME end
        
        
        ########## PARSING TIME begin
        start_parse_time = time.time()
        templates = []
        paras = []
        if self.n_workers == 1:
            templates, paras = parse_chunk(self.log_dataframe)
        else:
            chunk_size = min(5000000, self.log_dataframe.shape[0] // self.n_workers)
            result_chunks = []
            pool = mp.Pool(processes=self.n_workers)
            result_chunks = [pool.apply_async(parse_chunk, \
                             args=(self.log_dataframe.iloc[i:i + chunk_size],))
                             for i in range(0, self.log_dataframe.shape[0], chunk_size)]
            pool.close()
            pool.join()
            for result in result_chunks:
                result = result.get()
                templates.extend(result[0])
                paras.extend(result[1])
        print("Finish filter numbers.")
        
        ## filter top events begin
        self.log_dataframe['EventTemplate'] = templates
        self.log_dataframe['ParameterList'] = paras
        top_events = list(filter(lambda x: "<*>" in x, self.log_dataframe['EventTemplate'].value_counts().index))[0: self.top_event]
        false_index = ~self.log_dataframe["EventTemplate"].isin(top_events)
        self.log_dataframe.loc[false_index, "EventTemplate"] = self.log_dataframe.loc[false_index, "Content"]
        self.log_dataframe.loc[false_index, "ParameterList"] = ""
        ## filter top events end
        
        self.template_eid_mapping = {evt: "E"+str(idx) for idx, evt in enumerate(self.log_dataframe['EventTemplate'].unique())}
        self.log_dataframe['EventId'] = self.log_dataframe['EventTemplate'].map(lambda x: self.template_eid_mapping[x])
        self.log_dataframe.drop(["LineId"], axis=1, inplace=True)
        end_parse_time = time.time()
        
        self.parse_time = end_parse_time - start_parse_time
        ########## PARSING TIME end
            
    def dump(self, logname):
        self.log_dataframe.to_csv(os.path.join(self.outdir, logname + '_structured.csv'), index=False)
        occ_dict = dict(self.log_dataframe['EventTemplate'].value_counts())
        df_event = pd.DataFrame()
        df_event['EventTemplate'] = self.log_dataframe['EventTemplate'].unique()
        df_event['EventId'] = df_event['EventTemplate'].map(lambda x: self.template_eid_mapping[x])
        df_event['Occurrences'] = df_event['EventTemplate'].map(occ_dict)
        df_event.to_csv(os.path.join(self.outdir, logname + '_templates.csv'), index=False, columns=['EventId', 'EventTemplate', 'Occurrences'])

    def parse(self, logname, dump=True):
        self.read_data(logname)
        if dump:
            self.dump(logname)
        return self.log_dataframe
        print('Parsing done.')

def parse_chunk(chunk):
    templates = []
    paras = []
    for msg in chunk['Content']:
        template, para = filter_number(msg)
        templates.append(template)
        paras.append(para)
    return templates, paras

def filter_number(message):
    tokens = re.split(r'([\s:\.=\(\)\{\}\[\]]+)', message)
    paras = []
    i = 0
    while i < len(tokens):
        if tokens[i] and not tokens[i].isalpha():
            paras.append(tokens[i])
            tokens[i] = '<*>'
        i += 2
    return ''.join(tokens), paras