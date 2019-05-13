import sys
sys.path.append("../logzip/")
import os
import glob
import time
import shutil
import subprocess
from logparser import Drain
    

def boolean_string(s):
    if s not in {'False', 'True'}:
        raise ValueError('Not a valid boolean string')
    return s == 'True'

def get_FileSize(filePath, unit="kb"):
    fsize = os.path.getsize(filePath)
    if unit == "mb":
        fsize = fsize/float(1024*1024)
    if unit == "kb":
        fsize = fsize/float(1024)
    return round(fsize, 2)

def zip_file(filepath, outdir, log_format, template_file="", n_workers=2,
             level=3, lossy=False, top_event=2000, kernel="gz", compress_single=False,
             report_file="./report.csv"):
    time_start = time.time()

    # new tmp dirs
    logname = os.path.basename(filepath)
    timemark = time.strftime('%Y%m%d-%H%M%S', time.localtime(time.time()))
    tmp_dir = os.path.join(outdir, logname + "_tmp_" + timemark)
    print("Tmp files are in {}".format(tmp_dir))
    if os.path.isdir(tmp_dir):
        shutil.rmtree(tmp_dir)
    if not os.path.isdir(tmp_dir):
        os.makedirs(tmp_dir)
    
    if not template_file:
        """
        0. sampling
        """
        
        line_num = subprocess.check_output("wc -l {}".format(filepath), shell=True)
        line_num = int(line_num.split()[0])
        sample_num = 50000
        sample_file_path = filepath + ".sample"
        try:
            subprocess.check_output("gshuf -n{} {} > {}".format(sample_num, filepath,
                                sample_file_path), shell=True)
        except:
            subprocess.check_output("shuf -n{} {} > {}".format(sample_num, filepath,
                                sample_file_path), shell=True)
        
        """
        1. get template file  
        """
        st         = 0.5  # Similarity threshold
        depth      = 4  # Depth of all leaf nodes
        regex      = [
        r'blk_(|-)[0-9]+' , # block id
        r'(/|)([0-9]+\.){3}[0-9]+(:[0-9]+|)(:|)', # IP
        r'(?<=[^A-Za-z0-9])(\-?\+?\d+)(?=[^A-Za-z0-9])|[0-9]+$', # Numbers
        ]
        
        parse_begin_time = time.time()
        parser = Drain.LogParser(log_format, outdir=tmp_dir,  depth=depth, st=st, rex=regex)
        templates = parser.parse(sample_file_path)
        os.remove(sample_file_path)
        parse_end_time = time.time()
        template_file = os.path.join(tmp_dir, "log_templates.csv")
        with open(template_file, "w") as fw:
            [fw.write(item+"\n") for item in templates]
        print("Parser cost [{:.3f}s]".format(parse_end_time-parse_begin_time))
    
    
    # split files
    kb_per_chunk = int(get_FileSize(filepath) // n_workers) + 1
    cmd = "split -b {}k {} {}".format(kb_per_chunk, filepath, os.path.join(tmp_dir, f"{logname}_"))
    subprocess.call(cmd, stderr=subprocess.STDOUT, shell=True)
    
    
    # run subprocesses
    processes = []
    for idx, file in enumerate(sorted(glob.glob(os.path.join(tmp_dir, f"{logname}_*")))):
        script_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "zipper_longgest.py")
        per_tmp_dir = os.path.join(tmp_dir, str(idx))
        cmd = ('python {} --file {} --log_format "{}" --level {} --lossy {} --template_file {}'+ \
                ' --tmp_dir {} --out_dir {} --compress_single {} --n_workers {}') \
                                            .format(script_path, file, log_format, level, lossy, template_file,
                                              per_tmp_dir, per_tmp_dir,
                                              compress_single, n_workers)
        print(cmd)
        processes.append(subprocess.Popen(cmd, stderr=subprocess.STDOUT, shell=True))
    [p.wait() for p in processes]
    
    compressed_size = 0
    for idx in range(len(processes)):
        sub_outfile = glob.glob(os.path.join(tmp_dir, str(idx), "*logzip*"))[0]
        dst = os.path.join(outdir, os.path.basename(sub_outfile) + f".{idx+1}of{len(processes)}")
        shutil.move(sub_outfile, dst)
        compressed_size += get_FileSize(dst, "mb")

    [os.remove(chunk) for chunk in glob.glob(os.path.join(tmp_dir, f"{logname}_*"))]
    original_size = get_FileSize(filepath, "mb")
    compress_ratio = round(original_size / compressed_size, 2)

    time_end = time.time()    
    total_time_taken = time_end - time_start

    firstline = True
    if os.path.isfile(report_file):
        firstline = False
    with open(report_file, "a+") as fw:
        if firstline:
            fw.write("timemark,logname,original_size,compressed_size,compress_ratio,time_taken,n_workers,compress_single\n")
        fw.write(f"{timemark},{logname},{original_size},{compressed_size},{compress_ratio},{total_time_taken},{n_workers},{compress_single}\n")


if __name__ == "__main__":
    logfile       = "../../logs/HDFS_2k.log"  # Raw log file."
    outdir        = "../../zip_out/"  # Output directory, if not exists, it will be created.
    log_format    = '<Date> <Time> <Pid> <Level> <Component>: <Content>'  # Log format to extract fields.
    n_workers     = 3
    level         = 3
    top_event     = 2000
    kernel        = "gz"
    compress_single = True
    lossy = True
    report_file   = "./report.csv"
    template_file = ""
             
    zip_file(logfile, outdir, log_format,
                 template_file=template_file,
                 n_workers=n_workers,
                 level=level,
                 top_event=top_event,
                 kernel=kernel,
                 lossy=lossy,
                 compress_single=compress_single,
                 report_file=report_file)