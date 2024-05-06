# Dask Resources Monitor
![CI](https://img.shields.io/github/actions/workflow/status/negin513/ncar-dask-monitor/main.yml?label=CI&logo=GitHub&style=flat-square)


This Python package summarizes the resource usage history of Dask workers in NCAR clusters and provides insights to both individual users and system administrators, allowing them to better understand memory usage patterns and optimize resource allocation.

Users can track memory usage for individual or all users over a given period, gaining insights for better resource allocation and workflow optimization.

## How to use this script?

First, clone this repository:
```
git clone https://github.com/negin513/ncar-dask-monitor.git
```
Next, you can run the ./run_dask_mem_usage.py from the top directory.

The following options are available within `dask_mem_usage`:
```
usage: ./dask_resource_monitor [-h] [-s START_DATE | -d DAYS] [-e END_DATE] [-u USER] [--filename FILENAME] [-t] [-v]

|------------------------------------------------------------------|
|---------------------  Instructions  -----------------------------|
|------------------------------------------------------------------|
Extract Dask job statistics and memory usage.

This script extracts job statistics from a qhist file for a specified
user and date range.

-------------------------------------------------------------------
To see the available options:

    ./dask_resource_monitor--help

Examples:
    ./dask_resource_monitor --start_date 20230304 --end_date 20230314

or
    ./dask_resource_monitor --day 10 --user all --table

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
./dask_resource_monitor --start_date 20230301 --end_date 20230314 --user $USER
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
./dask_resource_monitor --user all -d 30 --table
```
or
```
./dask_resource_monitor --user all -d 30
```

```
      Memory usage summary of dask workers                                                   
                            Unused Mem (%) Req Mem (GB) Used Mem(GB) Elapsed (h) Walltime (h)
count                             81482.00     81482.00     81482.00    81482.00     81482.00
mean                                 87.87        25.00         2.96        1.10         3.35
min                                   0.00         2.00         0.00        0.00         0.08
max                                 100.00       466.00       241.20       24.97        24.00
| User        |   Req Mem (GB) |   Unused Mem (GB) |   Unused Mem (%) |   Elapsed (h) |   Dask job count |   Unused Core-Hour (GB.hr) |
|:------------|---------------:|------------------:|-----------------:|--------------:|-----------------:|---------------------------:|
| jvance      |       25       |          23.9262  |          95.7049 |    1.98803    |             8862 |              421530        |
| burcuboza   |       24.9573  |          24.078   |          96.4796 |    0.546916   |            23425 |              308475        |
| fredc       |       22.749   |          18.4449  |          79.406  |    1.08334    |            13194 |              263643        |
| mauricio    |       31.8962  |          24.276   |          75.7924 |    0.859131   |             5088 |              106117        |
| islas       |       18.4071  |          14.2842  |          78.1084 |    8.11669    |              904 |              104810        |
| wmikim      |       35.1724  |          32.6683  |          91.9319 |    5.37088    |              580 |              101765        |
| wchapman    |       25       |          20.2686  |          81.0745 |    2.12573    |             1950 |               84016.9      |
| mneedham    |       25       |          22.0855  |          88.3418 |    5.23524    |              440 |               50874        |
| rudradutt   |       37.368   |          30.16    |          78.4714 |    1.16754    |             1269 |               44685.3      |
| kristenk    |       35.1218  |          25.1782  |          67.7205 |    2.56838    |              657 |               42486.4      |
| jaye        |       28.6725  |          22.3102  |          79.7864 |    0.550789   |             3047 |               37442.3      |
| shartke     |       10       |           7.6948  |          76.948  |    2.57047    |             1653 |               32695.1      |
| pmongwe     |       50       |          49.0363  |          98.0725 |    7.53403    |               80 |               29555.3      |
| fhanifah    |       26.1434  |          24.4692  |          93.5981 |    0.198449   |             5936 |               28824.5      |
| djk2120     |       19.7382  |          18.1682  |          92.7479 |    1.4803     |              955 |               25684.2      |
| sshams      |       13.1268  |           8.35135 |          50.4845 |    1.46894    |             1735 |               21284.4      |
| yeager      |       19.5004  |          17.8849  |          91.7998 |    0.857255   |             1301 |               19946.8      |
| cbecker     |       49.1151  |          25.701   |          60.1684 |    0.751933   |              782 |               15112.5      |
| zephyrs     |       25       |          22.7422  |          90.9689 |    3.61504    |              180 |               14798.5      |

```

Alternatively, you can install this package using pip as outlined in the following and use `dask_mem_usage`. 

## Installation

To install the package directly from github:
```
pip install git+https://github.com/negin513/dask-monitor.git
```
`dask_resource_monitor` will now be added to your PATH.

Alternatively, you can use `run_dask_mem_usage.py` in the top directory without installing it. 
