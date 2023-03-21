#!/usr/bin/env python3

import logging
import argparse
import pandas as pd
from qhist_runner import QhistRunner
from getpass import getuser

def get_parser():
    """
    Creates and returns an ArgumentParser object for dask_reporter.

    Returns:

    """
    myname = getuser()

    parser = argparse.ArgumentParser(description=__doc__ ,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)

    parser.add_argument("--start_date",
                        type=str,
                        dest = 'start_date',
                        required = True,
                        action = "store",
                        help="The start date of the date range to extract.",
                        )


    parser.add_argument("--end_date",
                        type=str,
                        dest = 'end_date',
                        required = True,
                        action = "store",
                        help="The end date of the date range to extract.",
                        )

    group = parser.add_mutually_exclusive_group()

    group.add_argument("-u", "--user",
                        type=str,
                        dest='user',
                        required = False,
                        action = "store",
                        default = myname,
                        help=" Username of the user! [default: %(default)s]",
                        )

    #group.add_argument('--all',
    #                    action='store_true',
    #                    dest = 'allusers',
    #                    help=' All users! ')

    parser.add_argument("--filename",
                        type=str,
                        dest = 'filename',
                        required = False,
                        action = "store",
                        default = 'log.txt',
                        help="The name of the file to extract data from.",
                        )

    parser.add_argument("-v","--verbose",
                        action = 'store_true',
                        help="Increase output verbosity.",
                        )

    return parser


def main():
    """
    """

    parser = get_parser()
    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG, format='%(message)s')

        logging.info('User selection:')
        logging.info(f'\tstart_date : {args.start_date}')
        logging.info(f'\tend_date   : {args.end_date}')
        logging.info(f'\tuser       : {args.user}')
        logging.info(f'\tfilename   : {args.filename}')



    runner = QhistRunner(args.start_date, args.end_date, args.filename, args.user)
    result = runner.run_shell_code()

    jobs = pd.read_csv(args.filename)

    dask_jobs = jobs[jobs['Job Name'] == 'dask-worker']

    data_types = {'Req Mem (GB)': float, 'Used Mem(GB)': float, 'Elapsed (h)':float}

    dask_jobs = dask_jobs.astype(data_types)
    #print (dask_jobs.columns)
    #print (dask_jobs.dtypes)

    dask_jobs['Unused Mem (GB)'] = dask_jobs['Req Mem (GB)'] - dask_jobs['Used Mem(GB)']
    dask_jobs['Unused Mem (%)'] = dask_jobs['Unused Mem (GB)'] /dask_jobs['Req Mem (GB)'] * 100.0

    # -- overall statitics :
    # create bins based on desired ranges
    bins = [0, 25, 50, 75, 100]
    labels = ['<25%','25-50%','50-75%','>=75%']

    # create a new column with the bins
    dask_jobs['bin'] = pd.cut(dask_jobs['Unused Mem (%)'], bins=bins, include_lowest=True, right=False, labels=labels)

    # calculate the percentage of jobs in each bin
    percentages = dask_jobs['bin'].value_counts(normalize=True) * 100

    # show the resulting percentages
    #percentages= percentages.sort_index(ascending=False)

    percentages_str = percentages.map('{:.2f}%'.format)

    print(percentages_str.rename_axis('Unused Mem (%)').reset_index(name='Jobs %').to_string(index=False))

    # -- what percent of dask workers use more than 75% of requested memory?
    threshold = 75
    condition = dask_jobs['Unused Mem (%)'] > threshold
    percentage = condition.mean() * 100

    print ("*****")
    print (percentage, "of jobs had more than", threshold,"% of unused memory.")


    summary = dask_jobs['Unused Mem (%)'].describe()

    count = summary.loc['count']
    mean = summary.loc['mean']
    min = summary.loc['min']
    max = summary.loc['max']

    # print the results
    print("Number of jobs:", count)
    print("Mean Unused Memory:", mean)
    print("Min  Unused Memory:", min)
    print("Max  Unused Memory:", max)


if __name__ == '__main__':
    main()

