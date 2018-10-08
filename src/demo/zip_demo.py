import sys
sys.path.append("../")
from logzip import zip_log
from logzip import NaiveParser

n_workers = 1
logfile = "HDFS_2k.log"
log_format = '<Date> <Time> <Pid> <Level> <Component>: <Content>'

indir = "./"
outdir = "./zip_out/"
outname = logfile + ".logzip"

if __name__ == "__main__":
    parser = NaiveParser.LogParser(indir, outdir, log_format, n_workers=n_workers)
    structured_log = parser.parse(logfile, dump=False)
    zipper = zip_log.Ziplog(outdir=outdir, n_workers=n_workers, kernel="gz")
    zipper.zip_file(outname=outname, filename=logfile, para_df=structured_log)