# -*- coding: utf-8 -*-

import argparse
import os
parser = argparse.ArgumentParser()
parser.add_argument('--file', type=str, default="HDFS_2k.log")
args = vars(parser.parse_args())



cmd = "cat log.txt | ./Archiver -c --jhistory 10 --buckets 16 --est"