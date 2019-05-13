# -*- coding: utf-8 -*-
import sys
import os
import argparse
sys.path.append("../")
from logzip.logzip import zip_file


outdir        = "../../zip_out/"  # Output directory, if not exists, it will be created.
level         = 3
top_event     = 2000
kernel        = "gz"
report_file   = "./report.csv"

log_format_dict = {
    "HDFS": '<Date> <Time> <Pid> <Level> <Component>: <Content>',
    "Spark": '<Date> <Time> <Level> <Component>: <Content>',
    "Andriod": '<Date> <Time>  <Pid>  <Tid> <Level> <Component>: <Content>',
    "Thunderbird": '<Label> <Timestamp> <Date> <User> <Month> <Day> <Time> <Location> <Component>(\[<PID>\])?: <Content>',
    "Windows": '<Date> <Time>, <Level>                  <Component>    <Content>'
}


parser = argparse.ArgumentParser()
parser.add_argument("--worker", type=int, default="1")
parser.add_argument("--postfix", type=str, default="2k")
parser.add_argument("--dataset", type=str, default="HDFS")
parser.add_argument('--compress_single', type=str, default="True")
parser.add_argument('--lossy', type=str, default="True")
args = vars(parser.parse_args())
del parser

print(args)

postfix = args["postfix"]
n_workers = args["worker"]  # Number of processes.
dataset = args["dataset"]
compress_single = args["compress_single"]
lossy = args["lossy"]

logfile = os.path.join("../../logs", f"{dataset}_{postfix}.log")
zip_file(logfile, outdir, log_format_dict[dataset], n_workers=n_workers,
             level=level,lossy=lossy, top_event=top_event, kernel=kernel,
             compress_single=compress_single)
