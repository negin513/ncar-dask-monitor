#!/usr/bin/env python3
"""
|------------------------------------------------------------------|
|---------------------  Instructions  -----------------------------|
|------------------------------------------------------------------|
Extract Dask job statistics and memory usage.

This script extracts job statistics from a qhist file for a specified
user and date range.

-------------------------------------------------------------------
To see the available options:

    ./dask_mem_usage.py --help

"""
import argparse
import logging
from getpass import getuser

import pandas as pd

from qhist_parser import JobsSummary
from qhist_runner import QhistRunner


def get_parser():
    """
    Creates and returns an ArgumentParser object for this script.

    Returns:
        argparse.ArgumentParser: An ArgumentParser object for dask_mem_usage.
    """
    myname = getuser()

    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )

    group = parser.add_mutually_exclusive_group()

    group.add_argument(
        "-s",
        "--start_date",
        type=str,
        dest="start_date",
        action="store",
        help="The start date of the date range to extract.",
    )

    group.add_argument(
        "-d",
        "--days",
        type=int,
        dest="days",
        action="store",
        help="number of previous days to extract.",
    )

    parser.add_argument(
        "-e",
        "--end_date",
        type=str,
        dest="end_date",
        action="store",
        help="The end date of the date range to extract.",
    )

    parser.add_argument(
        "-u",
        "--user",
        type=str,
        dest="user",
        required=False,
        action="store",
        default=myname,
        help=" Username of the user! [default: %(default)s]",
    )

    parser.add_argument(
        "--filename",
        type=str,
        dest="filename",
        required=False,
        action="store",
        default="log.txt",
        help="The name of the qhist output. [default: %(default)s]",
    )

    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Increase output verbosity.",
    )

    return parser


def main():
    """
    Main function for extracting Dask job statistics.
    """

    parser = get_parser()
    args = parser.parse_args()

    # Check if start and end dates are provided correctly
    if args.start_date and not args.end_date:
        parser.error("End date is required if start date is provided.")
    if args.end_date and not args.start_date:
        parser.error("Start date is required if end date is provided.")
    if not args.start_date and not args.d:
        raise ValueError("Either start-date or -d option must be provided.")

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG, format="%(message)s")

        logging.info("User selection:")
        logging.info(f"\tstart_date : {args.start_date}")
        logging.info(f"\tend_date   : {args.end_date}")
        logging.info(f"\tuser       : {args.user}")
        logging.info(f"\tfilename   : {args.filename}")

    runner = QhistRunner(args.start_date, args.end_date, args.filename, args.user)
    result = runner.run_shell_code()

    jobs = JobsSummary(args.filename)
    jobs.user_report()

    if args.user == "all":
        report = "users_" + args.start_date + "-" + args.end_date + ".txt"
        jobs.csg_report(report)


if __name__ == "__main__":
    main()
