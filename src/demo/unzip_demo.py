import sys
import os
sys.path.append("../")
from logzip import unzip_log

n_workers     = 1  # Number of processes.
level         = 3  # Decompression level, must be the same as compression level.
kernel        = "gz"  # Compression kernels, must be the same as compression level. Options: "gz", "bz2", "lzma".
log_format    = '<Date> <Time> <Pid> <Level> <Component>: <Content>'  # Log format to extract fields.

logfile       = "HDFS_2k.log" 
ziped_file    = "{}.logzip.tar.gz".format(logfile) # Raw compressed file
indir         = "../../zip_out/" # Input directory
outdir        = "../../unzip_out/" # Output directory, if not exists, it will be created.
outname       = "{}.unziped".format(logfile) # Output file name.

if __name__ == "__main__":
    unzipper = unzip_log.Unziplog(indir=indir, outdir=outdir, log_format=log_format,    n_workers=n_workers, kernel=kernel, level=level)
    unzipper.unzip(ziped_file, outname)