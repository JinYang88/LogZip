<p align="center"> <a href="https://github.com/logpai"> <img src="https://github.com/logpai/logpai.github.io/blob/master/img/logpai_logo.jpg" width="500" height="125"/> </a>
</p>

# logzip

An efficient compression tool specific for log files.  It compresses log files by utilizing the inherent structures of raw log messages, and thereby achieves a high compression ratio.  Details can be seen in [this](link) paper.

## Prerequisites 



## Installation 



## Data



## Usage

A demo is uploaded to this repo (logzip/src/demo). We use a HDFS log file with 2k lines as a demo.

#### Compression

```shell
$ cd logzip/src/demo/
$ python3 zip_demo.py
```

#### Decompression

```shell
$ cd logzip/src/demo/
$ python3 unzip_demo.py
```

