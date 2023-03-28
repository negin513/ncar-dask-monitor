# Dask Memory Usage History
![CI](https://img.shields.io/github/actions/workflow/status/negin513/ncar-dask-monitor/main.yml?label=CI&logo=GitHub&style=flat-square)


This Python package summarizes the memory usage history of Dask workers in NCAR clusters and provides insights to both individual users and system administrators, allowing them to better understand memory usage patterns and optimize resource allocation.

Users can track memory usage for individual or all users over a given period, gaining insights for better resource allocation and workflow optimization.

## Installation

To install the package directly from github:
```
pip install git+https://github.com/negin513/dask-monitor.git
```
`dask_mem_usage` will now be added to your PATH.

## Usage
The following options are available within `dask_mem_usage`:
```
usage: dask_mem_usage [-h] [-s START_DATE | -d DAYS] [-e END_DATE] [-u USER] [--filename FILENAME] [-t] [-v]

|------------------------------------------------------------------|
|---------------------  Instructions  -----------------------------|
|------------------------------------------------------------------|
Extract Dask job statistics and memory usage.

This script extracts job statistics from a qhist file for a specified
user and date range.

-------------------------------------------------------------------
To see the available options:

    ./dask_mem_usage.py --help

Examples:
    ./dask_mem_usage.py --start_date 20230304 --end_date 20230314

or
    ./dask_mem_usage.py --day 10 --user all --table

options:
  -h, --help            show this help message and exit
  -s START_DATE, --start_date START_DATE
                        The start date of the date range to extract.
  -d DAYS, --days DAYS  number of previous days to extract.
  -e END_DATE, --end_date END_DATE
                        The end date of the date range to extract.
  -u USER, --user USER  Username of the user! [default: negins]
  --filename FILENAME   The name of the qhist output. [default: log.txt]
  -t, --table           Write user report in a table format. [default: False]
  -v, --verbose         Increase output verbosity.
```

For one user:
```
./dask_mem_usage.py --start_date 20230301 --end_date 20230314 --user $USER
```

For one user, the sample output looks like:

```
Memory usage summary of Dask workers
Number of jobs :  4169
Unused Mem (%):
    mean: 97.57
    min: 63.20
    max: 100.00
Req Mem (GB):
    mean: 25.00
    min: 25.00
    max: 25.00
Used Mem(GB):
    mean: 0.61
    min: 0.00
    max: 9.20
Elapsed (h):
    mean: 2.79
    min: 0.00
    max: 6.04
Walltime (h):
    mean: 6.00
    min: 6.00
    max: 6.00
------------------------
Summary Overview:
Unused Mem (%) Jobs %
         >=75% 99.50%
        50-75%  0.50%
        25-50%  0.00%
          <25%  0.00%

```
The output can be extracted in a table format using `--table` argument.


To see all users for the past 30 days:

```
./dask_mem_usage.py --user all -d 30 --table

or

dask_mem_usage --user all -d 30
```

```
Memory usage summary of Dask workers
Number of jobs :  85548
Unused Mem (%):
    mean: 87.03
    min: 0.00
    max: 100.00
Req Mem (GB):
    mean: 27.72
    min: 1.00
    max: 300.00
Used Mem(GB):
    mean: 3.31
    min: 0.00
    max: 115.70
Elapsed (h):
    mean: 1.19
    min: 0.00
    max: 23.99
Walltime (h):
    mean: 3.64
    min: 0.17
    max: 24.00
------------------------
Summary Overview:
Unused Mem (%) Jobs %
         >=75% 76.05%
        50-75% 10.86%
        25-50%  7.31%
          <25%  5.77%
           Req Mem (GB)  Unused Mem (GB)  Unused Mem (%)  Elapsed (h)  Dask job count  Unused Core-Hour (GB.hr)
User                                                                                                           
fredc             31.68            27.50           83.43         1.01           19212                 534807.90
jvance            25.00            24.15           96.61         2.32            9520                 533713.30
islas             19.44            14.02           71.69         8.67            2133                 259401.92
wchapman          25.00            21.36           85.45         1.25            5303                 141648.71
burcuboza         25.00            23.76           95.04         0.33           17070                 133745.83
...                 ...              ...             ...          ...             ...                       ...
dgagne            10.00             6.47           64.69         1.97              16                    203.90
apauling         200.00           185.28           92.64         0.15               6                    166.75
vanderwb           4.00             1.20           30.00         0.63             100                     75.72
juliob            20.00            19.91           99.54         0.04              23                     16.72
```
