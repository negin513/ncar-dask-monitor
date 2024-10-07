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

    ./dask_resource_monitor --help

Examples:
    ./dask_resource_monitor --start_date 20230304 --end_date 20230314

or
    ./dask_resource_monitor --day 10 --user all --table

"""
import os
import argparse
import logging
from getpass import getuser
from datetime import datetime, timedelta

import pandas as pd

from .qhist_runner import QhistRunner
from .report_generator import JobsSummary


def get_parser():
    """
    Creates and returns an ArgumentParser object for this script.

    Returns:
        argparse.ArgumentParser: An ArgumentParser object for dask_mem_usage.
    """
    myname = getuser()
    today_date = datetime.today().strftime('%Y%m%d')
    default_filename = f"qhist_log_{today_date}.txt"

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
        default=os.path.join("/glade/derecho/scratch",myname,default_filename),
        help="The name of the qhist output. [default: %(default)s]",
    )

    parser.add_argument(
        "-t", "--table",
        dest="table",
        required=False,
        action="store_true",
        help="Provide a summary of user report in a table format. [default: %(default)s]",
    )

    parser.add_argument(
        "--worker",
        type=str,
        dest="worker",
        required=False,
        action="store",
        default="dask*",
        help="Name of the Dask jobs if other than default. [default: %(default)s]",
    )

    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Increase output verbosity.",
    )

    return parser

def parse_arguments():
    """
    Parse command-line arguments using the get_parser function.

    Returns:
        argparse.Namespace: A namespace object containing the parsed arguments.
    """
    parser = get_parser()
    args = parser.parse_args()
    return args

def validate_dates(args, parser):
    """
    Validate the start and end dates and update them based on provided days.

    Args:
        args (argparse.Namespace): A namespace object containing the parsed arguments.
        parser (argparse.ArgumentParser): The ArgumentParser object used for error handling.
    """

    # error checks:
    # -- check at least days or start-date is provided.
    if not args.start_date and not args.days:
        parser.error("Either --start-date or -d/-days option must be provided.")

    if args.start_date and args.days:
        parser.error("Please use either --start-date or -d/-days option.")

    # -- convert days
    if args.days:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=args.days)
        args.start_date = start_date.strftime('%Y%m%d')
        args.end_date = end_date.strftime('%Y%m%d')

    # -- check if start and end dates are provided correctly
    if args.start_date and not args.end_date:
        end_date = datetime.now()
        args.end_date = end_date.strftime('%Y%m%d')
        #parser.error("End date is required if start date is provided.")

    if args.end_date and not args.start_date:
        parser.error("Start date is required if end date is provided.")

    # -- check if end-time is bigger than start-time
    if args.start_date and args.end_date:
        start_date_dt = datetime.strptime(args.start_date, "%Y%m%d")
        end_date_dt = datetime.strptime(args.end_date, "%Y%m%d")
        if end_date_dt <= start_date_dt:
            parser.error("End date must be greater than start date.")

def run_qhist(args):
    """
    Run QhistRunner and generate the report.

    Args:
        args (argparse.Namespace): A namespace object containing the parsed arguments.
    """
    # Check if the directory of args.filename exists, create it if it does not
    filename_directory = os.path.dirname(args.filename)
    if not os.path.exists(filename_directory):
        os.makedirs(filename_directory)
        if args.verbose:
            logging.info(f"Created directory: {filename_directory}")

    runner = QhistRunner(args.start_date, args.end_date, args.filename, args.user)
    result = runner.run_shell_code(args.verbose)

    jobs = JobsSummary(args.filename, args.worker,args.verbose)
    jobs.dask_user_report(args.table,args.verbose)

    if args.user == "all":
        report = "users_" + args.start_date + "-" + args.end_date + ".txt"
        logging.info(f"All users report is saved in {report}")
        jobs.dask_csg_report(report)


def main():
    """
    Main function for extracting Dask job statistics.
    """

    args = parse_arguments()
    validate_dates(args, get_parser())
    start_date_dt = datetime.strptime(args.start_date, "%Y%m%d")
    end_date_dt = datetime.strptime(args.end_date, "%Y%m%d")



    if args.verbose:
        logging.basicConfig(level=logging.DEBUG, format="%(message)s")

        logging.info("User selection:")
        logging.info(f"\tstart_date : {start_date_dt}")
        logging.info(f"\tend_date   : {end_date_dt}")
        logging.info(f"\tuser       : {args.user}")
        #logging.info(f"\tfilename   : {args.filename}")

    run_qhist(args)

if __name__ == "__main__":
    main()
