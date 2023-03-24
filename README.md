# Dask Memory Usage History

This Python code provides a way to track the memory usage of Dask workers over time,
either for a specific user or for all users.

## Usage

For one user:
```
./dask_mem_usage.py --start_date 20230301 --end_date 20230314 --user $USER
```

For one user, the sample output looks like:

```
Memory usage summary of dask workers
Number of jobs :  4169
Mean Unused Mem (%): 97.57
Min Unused Mem (%): 63.20
Max Unused Mem (%): 100.00
------------------------
Mean Req Mem (GB): 25.00
Min Req Mem (GB): 25.00
Max Req Mem (GB): 25.00
------------------------
Mean Used Mem(GB): 0.61
Min Used Mem(GB): 0.00
Max Used Mem(GB): 9.20
------------------------
------------------------
Summary:
Unused Mem (%) Jobs %
         >=75% 99.50%
        50-75%  0.50%
        25-50%  0.00%
          <25%  0.00%
```

For all users and to see top users:
```
./dask_mem_usage.py --start_date 20230301 --end_date 20230314 --user all 
```

```
Memory usage summary of dask workers
Number of jobs :  20247
Mean Unused Mem (%): 88.32
Min Unused Mem (%): 0.00
Max Unused Mem (%): 100.00
------------------------
Mean Req Mem (GB): 35.70
Min Req Mem (GB): 2.00
Max Req Mem (GB): 300.00
------------------------
Mean Used Mem(GB): 3.86
Min Used Mem(GB): 0.00
Max Used Mem(GB): 115.70
------------------------
Summary:
Unused Mem (%) Jobs %
         >=75% 81.97%
        50-75% 12.14%
        25-50%  2.66%
          <25%  3.22%
             Req Mem (GB)  ...  Unused Core-Hour (GB.hr)
User                       ...                          
fredc               50.00  ...                 287578.20
jvance              25.00  ...                 283971.52
islas               20.00  ...                 209901.07
rudradutt          101.76  ...                  54609.80
tking               18.20  ...                  45410.40
fhanifah            70.00  ...                  44483.42
yeager              20.89  ...                  16487.47
eromashkova         28.18  ...                  13175.80
cmv                300.00  ...                  11148.95
pmongwe             50.00  ...                   7283.09
huili7             105.49  ...                   6013.18

```

To see all available options:
```
 ./dask_mem_usage.py --help
usage: dask_mem_usage.py [-h] [-s START_DATE | -d DAYS] [-e END_DATE]
                         [-u USER] [--filename FILENAME] [-v]

|------------------------------------------------------------------|
|---------------------  Instructions  -----------------------------|
|------------------------------------------------------------------|
Extract Dask job statistics and memory usage.

This script extracts job statistics from a qhist file for a specified
user and date range.

-------------------------------------------------------------------
To see the available options:

    ./dask_mem_usage.py --help

optional arguments:
  -h, --help            show this help message and exit
  -s START_DATE, --start_date START_DATE
                        The start date of the date range to extract.
  -d DAYS, --days DAYS  number of previous days to extract.
  -e END_DATE, --end_date END_DATE
                        The end date of the date range to extract.
  -u USER, --user USER  Username of the user! [default: negins]
  --filename FILENAME   The name of the qhist output. [default: log.txt]
  -v, --verbose         Increase output verbosity.

```
