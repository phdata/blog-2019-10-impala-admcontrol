## How To Tame Apache Impala Users with Admission Control   


In order to make informed and accurate decisions on how to allocate resources for various users and applications, we need to gather detailed metrics.  We’ve written a Python script to streamline this process. The script generates a csv report and does not make any changes. 

## Python Environment Setup  
Setup a virtualenv:  
    `$ python3 -m venv penv`  
    `$ source ./penv/bin/activate`    
    `$ pip install -r requirements.txt`

Create a config file for your cluster/environment:  
`$ cat config_prod.ini`  
```
[config]
hostname = manager.cluster1.phdata.io
username = phdata_admin
password = *****
protocol = https
port = 7183
version = v19
cluster = cluster  
```

## Script Usage    
```
$ ./impala-admcontrol-memory.py
usage: impala-admcontrol-memory.py [-h] -c CONFIG -d NUMBER_OF_DAYS
```

`$ ./impala-admcontrol-memoty.py -c config_prod.ini -d 30 > prod_report.csv`

## CSV output    
The csv report includes overall and per-user stats for:  
- (queries_count) - number of queries ran  
- (queries_count_missing_stats) - number of queries ran without stats  
- (aggregate_avg_gb) - average memory used across nodes  
- (aggregate_99th_gb) - 99% max memory used across nodes  
- (aggregate_max_gb) - max memory used across nodes  
- (per_node_avg_gb) - average memory used per node  
- (per_node_99th_gb) - 99% max memory used per node  
- (per_node_max_gb) - max memory used per node  
- (duration_avg_minutes) - average query duration (in minutes)  
- (duration_99th_minutes) - 99% query duration (in minutes)  
- (duration_max_minutes) - max query duration (in minutes)  


