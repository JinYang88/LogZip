import sys
import os
sys.path.append("../")
from logzip import unzip_log

n_workers = 1
logfile = "HDFS_2k.log"
ziped_file = "{}.logzip.tar.gz".format(logfile)

indir = "./zip_out"
outdir = "./unzip_out/"
outname = "{}.unziped".format(logfile)
log_format = '<Date> <Time> <Pid> <Level> <Component>: <Content>'

if __name__ == "__main__":
    unzipper = unzip_log.Unziplog(indir=indir, outdir=outdir, log_format=log_format,    n_workers=n_workers, kernel="gz")
    unzipper.unzip(ziped_file, outname)