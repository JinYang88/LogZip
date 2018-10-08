# logzip

## Introduction

This is a compression tool specific for log files which achieves a high compression ratio.  It utilizes structures of log files to compress logs in a compact way.  Details can be seen in [this](link) paper.

## Usage

A demo is uploaded to this repo (logzip/src/demo). We use a HDFS log file with 2k lines as a demo.

#### Compression:

```shell
$ cd logzip/src/demo/
$ python3 zip_demo.py
```

#### Decompression:

```shell
$ cd logzip/src/demo/
$ python3 unzip_demo.py
```

