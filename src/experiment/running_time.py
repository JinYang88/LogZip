import sys
sys.path.append("../")
import json
import argparse
from logzip import zip_log
from logzip import NaiveParser


level         = 3  # Compression level.
top_event     = 2000 # Only templates whose occurrence is ranked above top_event are taken into consideration.
kernel        = "gz"  # Compression kernels. Options: "gz", "bz2", "lzma".
indir         = "../../logs/"  # Input directory
outdir        = "../../zip_out/"  # Output directory, if not exists, it will be created.


log_format_dict = {
    "HDFS": '<Date> <Time> <Pid> <Level> <Component>: <Content>',
    "Spark": '<Date> <Time> <Level> <Component>: <Content>',
    "Andriod": '<Date> <Time>  <Pid>  <Tid> <Level> <Component>: <Content>',
    "Thunderbird": '<Label> <Timestamp> <Date> <User> <Month> <Day> <Time> <Location> <Component>(\[<PID>\])?: <Content>',
    "Windows": '<Date> <Time>, <Level>                  <Component>    <Content>'
}


def record_time(parser, zipper, filename):
    field_extraction_time = round(parser.field_extraction_time\
                            + zipper.field_extraction_time, 3)
                            
    parse_time = round(parser.parse_time, 3)
    
    mapping_time = round(zipper.mapping_time, 3) 

    packing_time = round(zipper.packing_time, 3)
    
    time_dict = {"field_extraction_time":field_extraction_time,
                 "parse_time": parse_time,
                 "mapping_time": mapping_time,
                 "packing_time": packing_time}
    return {filename + f"_workers({n_workers})": time_dict}
    

def run(logfile, log_format):
    outname = logfile + ".logzip"  # Output file name.
    parser = NaiveParser.LogParser(indir, outdir, log_format, n_workers=n_workers)
    structured_log = parser.parse(logfile, dump=False)
    zipper = zip_log.Ziplog(outdir=outdir, n_workers=n_workers, kernel=kernel, level=level)
    zipper.zip_file(outname=outname, filename=logfile, para_df=structured_log)
    
    return record_time(parser, zipper, logfile)
    

if __name__ == "__main__":
    ## run:
    ## python running_time.py --worker 4 --postfix 1g
    ## python running_time.py --worker 8 --postfix 1g
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--worker", type=int, default="4")
    parser.add_argument("--postfix", type=str, default="2k")
    args = vars(parser.parse_args())

    postfix = args["postfix"]
    n_workers = args["worker"]  # Number of processes.

    datasets = ["HDFS", "Spark", "Windows", "Thunderbird", "Andriod"]
    merged_time_record = {}
    for dataset in datasets:
        print(f"Running {dataset}")
        try:
            logfile = f"{dataset}_{postfix}.log"  # Raw log file.
            time_record = run(logfile, log_format_dict[dataset])
            merged_time_record.update(time_record)
            with open(f"Running-time-experiment_{n_workers}.json", "w") as fw:
                json.dump(merged_time_record, fw, indent=4)
        except Exception as e:
            print(f"Run {dataset} failed for! ")

    
    
    
    
    
    
    
    
    