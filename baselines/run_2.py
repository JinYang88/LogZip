# -*- coding: utf-8 -*-

import argparse
import os
parser = argparse.ArgumentParser()
parser.add_argument('--file', type=str, default="../logs/HDFS_2k.log")
args = vars(parser.parse_args())



dirname = os.path.basename(args["file"]).strip(".log")
output_dir = os.path.join("./2_output", dirname)
if not os.path.isdir():
    os.makedirs()
    
cmd = f"cat {args['file']} | ./Archiver -c --jhistory 10 --buckets 16 --est"