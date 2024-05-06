# Dask Resources Monitor
![CI](https://img.shields.io/github/actions/workflow/status/negin513/ncar-dask-monitor/main.yml?label=CI&logo=GitHub&style=flat-square)


This Python package summarizes the resource usage history of Dask workers in NCAR clusters and provides insights to both individual users and system administrators, allowing them to better understand memory usage patterns and optimize resource allocation.

Users can track memory usage for individual or all users over a given period, gaining insights for better resource allocation and workflow optimization.


## How to use this script?

You can use this script to see your dask resource usage over a period of time.

For example:
```
# Check resource usage from March 1, 2023 to March 14, 2023
dask_resource_monitor --start_date 20230301 --end_date 20230314 --table
```

or
```
# Monitor last 10 days of resource usage for the current user
dask_resource_monitor -d 10 --table --user $USER
```


Here is an example summary output: 

```
       Resource usage summary of dask workers                                              
                               Unused Mem (%) Req Mem (GB) Used Mem(GB) CPU (%) Elapsed (h)
count                                  144.00       144.00       144.00  144.00      144.00
mean                                    95.17        25.00         1.21   14.78        0.87
median                                  98.40        25.00         0.40   12.00        1.01
min                                     77.20        25.00         0.20    1.00        0.10
max                                     99.20        25.00         5.70  100.00        1.03

```



To see all available options:

```
dask_resource_monitor --help
```



### Options
The dask_resource_monitor script comes with several options:
`-h`, `--help`: Show the help message and exit.
`-s START_DATE`, `--start_date START_DATE`: The start date of the date range to extract.
`-d DAYS`, `--days DAYS`: Number of previous days to extract.
`-e END_DATE`, `--end_date END_DATE`: The end date of the date range to extract.
`-u USER`, `--user USER`: Username of the user (default: Your username).
`--filename FILENAME`: The name of the qhist output (default: log.txt).
`-t`, `--table`: Write user report in a table format (default: False).
`-v`, `--verbose`: Increase output verbosity.



## Installation

To install the package directly from github:
```
pipx install git+https://github.com/negin513/dask-monitor.git
```
`dask_resource_monitor` will now be added to your PATH. 

-----

Alternatively, you can clone the repository and run `./dask_resource_monitory` from top level.  
First, clone this repository:
```
git clone https://github.com/negin513/ncar-dask-monitor.git
```
Next, you can run the `./dask_resource_monitor` from the top directory.




## Checking Resource Selections For all Users (CSG Team): 

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




Alternatively, you can use `run_dask_mem_usage.py` in the top directory without installing it. 
