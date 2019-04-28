import sys
sys.path.append("../")
from logzip import zip_log
from logzip import NaiveParser

n_workers     = 8  # Number of processes.
level         = 3  # Compression level.
top_event     = 2000 # Only templates whose occurrence is ranked above top_event are taken into consideration.
kernel        = "gz"  # Compression kernels. Options: "gz", "bz2", "lzma".
log_format    = '<Date> <Time> <Pid> <Level> <Component>: <Content>'  # Log format to extract fields.

logfile       = "HDFS_2k.log"  # Raw log file.
#logfile       = "HDFS_1g.log"
indir         = "../../logs/"  # Input directory
outdir        = "../../zip_out/"  # Output directory, if not exists, it will be created.
outname       = logfile + ".logzip"  # Output file name.

if __name__ == "__main__":
    parser = NaiveParser.LogParser(indir, outdir, log_format, n_workers=n_workers)
    structured_log = parser.parse(logfile)
    zipper = zip_log.Ziplog(outdir=outdir, n_workers=n_workers, kernel=kernel, level=level)
    zipper.zip_file(outname=outname, filename=logfile, para_df=structured_log)