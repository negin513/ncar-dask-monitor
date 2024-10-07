#!/usr/bin/env python3
import os
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
        "-d", "--days", type=int, action="store", help="number of previous days from today to create a range."
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

    # group.add_argument('--all',
    #                    action='store_true',
    #                    dest = 'allusers',
    #                    help=' All users! ')

    parser.add_argument(
        "--filename",
        type=str,
        dest="filename",
        required=False,
        action="store",
        default=os.path.join("/glade/derecho/scratch", myname, "qhist_log.txt"),
        help="The name of the file to extract data from.",
    )

    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Increase output verbosity.",
    )

    return parser


def compute_summary_stats(df: pd.DataFrame, field_name: str) -> None:
    """
    Compute and print the count, mean, min, and max values of a field in a DataFrame.

    Parameters:
        df (pd.DataFrame): The input DataFrame.
        field_name (str): The name of the field to compute the summary statistics for.

    Returns:
        None: The function prints the summary statistics.

    Example:
        # create a sample DataFrame
        data = {'Unused Mem (%)': [10.5, 20.1, 15.7, 25.3, 18.9]}
        df = pd.DataFrame(data)

        # call the function for the 'Unused Mem (%)' field
        compute_summary_stats(df, 'Unused Mem (%)')
    """
    summary = df[field_name].describe()
    count = summary.loc["count"]
    mean_val = summary.loc["mean"]
    min_val = summary.loc["min"]
    max_val = summary.loc["max"]
    result_str = (
        f"Mean {field_name}: {mean_val:.2f}\n"
        f"Min {field_name}: {min_val:.2f}\n"
        f"Max {field_name}: {max_val:.2f}"
    )
    print(result_str)


def main():
    """ """

    parser = get_parser()
    args = parser.parse_args()

    if args.start_date and not args.end_date:
        parser.error("End date is required if start date is provided.")
    if args.end_date and not args.start_date:
        parser.error("Start date is required if end date is provided.")

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG, format="%(message)s")

        logging.info("User selection:")
        logging.info(f"\tstart_date : {args.start_date}")
        logging.info(f"\tend_date   : {args.end_date}")
        logging.info(f"\tuser       : {args.user}")
        logging.info(f"\tfilename   : {args.filename}")

    runner = QhistRunner(args.start_date, args.end_date, args.filename, args.user)
    result = runner.run_shell_code()

    jobs = pd.read_csv(args.filename)

    dask_jobs = jobs[jobs["Job Name"] == "dask-worker"]

    data_types = {"Req Mem (GB)": float, "Used Mem(GB)": float, "Elapsed (h)": float}

    dask_jobs = dask_jobs.astype(data_types)
    # print (dask_jobs.columns)
    # print (dask_jobs.dtypes)

    dask_jobs["Unused Mem (GB)"] = dask_jobs["Req Mem (GB)"] - dask_jobs["Used Mem(GB)"]
    dask_jobs["Unused Mem (%)"] = (
        dask_jobs["Unused Mem (GB)"] / dask_jobs["Req Mem (GB)"] * 100.0
    )

    # -- overall statitics :
    # create bins based on desired ranges
    bins = [0, 25, 50, 75, 100]
    labels = ["<25%", "25-50%", "50-75%", ">=75%"]

    # create a new column with the bins
    dask_jobs["bin"] = pd.cut(
        dask_jobs["Unused Mem (%)"],
        bins=bins,
        include_lowest=True,
        right=False,
        labels=labels,
    )

    # calculate the percentage of jobs in each bin
    percentages = dask_jobs["bin"].value_counts(normalize=True) * 100

    # show the resulting percentages
    percentages = percentages.sort_index(ascending=False)

    percentages_str = percentages.map("{:.2f}%".format)

    # -- what percent of dask workers use more than 75% of requested memory?
    threshold = 75
    condition = dask_jobs["Unused Mem (%)"] > threshold
    percentage = condition.mean() * 100

    print("------------------------")
    print(
        "Memory usage summary of dask workers for period of ",
        args.start_date,
        "-",
        args.end_date,
    )

    summary = dask_jobs["Unused Mem (%)"].describe()

    count = summary.loc["count"]
    mean = summary.loc["mean"]
    min = summary.loc["min"]
    max = summary.loc["max"]

    # print the results
    print("Number of jobs : ", count)

    compute_summary_stats(dask_jobs, "Unused Mem (%)")
    print("------------------------")
    compute_summary_stats(dask_jobs, "Req Mem (GB)")
    print("------------------------")
    compute_summary_stats(dask_jobs, "Used Mem(GB)")

    print("------------------------")
    print("Summary:")
    print(
        percentages_str.rename_axis("Unused Mem (%)")
        .reset_index(name="Jobs %")
        .to_string(index=False)
    )
    print(f"{percentage:.2f}% of jobs had more than {threshold}% of unused memory.")

    print("------------------------")
    dj = dask_jobs
    if args.user == "all":
        print("hello!")
        grouped_dj = dj.groupby("User").agg(
            {
                "Unused Mem (GB)": "mean",
                "Unused Mem (%)": "mean",
                "Elapsed (h)": "mean",
                "Job ID": "count",
            }
        )
        dj_90 = grouped_dj[grouped_dj["Unused Mem (%)"] > 80]
        dj_90["Wasted Memory-Hour (GB-hr)"] = (
            dj_90["Unused Mem (GB)"] * dj_90["Elapsed (h)"] * dj_90["Job ID"]
        )
        # dj_90=dj_90.rename(columns = {'Unused Mem':'Unused Mem (GB)'})

        print(
            dj_90.sort_values(by=["Wasted Memory-Hour (GB-hr)"], ascending=False)[0:10]
        )


if __name__ == "__main__":
    main()
