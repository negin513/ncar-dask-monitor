import sys
import warnings

import pandas as pd


def compute_summary_stats(df: pd.DataFrame, field_name: str, verbose=False) -> dict:
    """
    Compute and print the count, mean, min, and max values of a field in DataFrame

    Parameters:
        df (pd.DataFrame): The input DataFrame.
        field_name (str): The name of the field to compute the summary statistics
        verbose (bool): Whether or not to print the summary statistics as a string (default False).

    Returns:
        None: The function prints the summary statistics.

    Example:
        # create a sample DataFrame
        data = {'Unused Mem (%)': [10.5, 20.1, 15.7, 25.3, 18.9]}
        df = pd.DataFrame(data)

        # call the function for the 'Unused Mem (%)' field
        result_dict = compute_summary_stats(df, 'Unused Mem (%)')
    """
    summary = df[field_name].describe()
    count = summary.loc["count"]
    mean_val = summary.loc["mean"]
    min_val = summary.loc["min"]
    max_val = summary.loc["max"]
    result_dict = {
        field_name: {"count": count, "mean": mean_val, "min": min_val, "max": max_val}
    }
    if verbose:
        result_str = (
            f"Mean {field_name}: {mean_val:.2f}\n"
            f"Min {field_name}: {min_val:.2f}\n"
            f"Max {field_name}: {max_val:.2f}"
        )
        print(result_str)
    return result_dict


def bin_summary(
    df: pd.DataFrame, field_name: str, bins: list = None, labels: list = None
) -> None:
    """
    Compute and print the percentage of a df column in each bin.

    Parameters:
        df (pd.DataFrame): The input DataFrame containing Dask jobs.
        field_name (str): The name of the field to compute the summary statistics.
        bins (list): List of bins for binning. Default is [0, 25, 50, 75, 100].
        labels (list): List of labels for the bins. Default is ['<25%', '25-50%', '50-75%', '>=75%'].

    Returns:
        None: The function prints the percentage of the column in each bin.
    """
    if bins is None:
        bins = [0, 25, 50, 75, 100]

    if labels is None:
        labels = ["<25%", "25-50%", "50-75%", ">=75%"]

    # create a new column with the bins
    df["bin"] = pd.cut(
        df[field_name], bins=bins, include_lowest=True, right=False, labels=labels
    )

    # calculate the percentage of jobs in each bin
    percentages = df["bin"].value_counts(normalize=True) * 100

    # show the resulting percentages
    percentages = percentages.sort_index(ascending=False)
    percentages_str = percentages.map("{:.2f}%".format)

    print(
        percentages_str.rename_axis("Unused Mem (%)")
        .reset_index(name="Jobs %")
        .to_string(index=False)
    )


class JobsSummary:
    """
    A class that reads a qhist log file, parse, provide some statistics on Dask jobs memory usage.

    Attributes:
        filename (str): The name of the file to extract data from.
    """

    def __init__(self, filename):
        """
        Initializes a JobsSummary object.

        Args:
            filename (str): The name of the file to extract data from.
        """
        self.filename = filename
        self._read_all_jobs()

    def _read_all_jobs(self) -> None:
        """
        Read the qhist file and select Dask jobs only.
        """
        jobs = pd.read_csv(self.filename)

        # -- check if there is any jobs for this user
        if len(jobs) == 0:
            warnings.warn("Warning! No jobs found for this user and this time period!")
            sys.exit()

        # -- select dask-jobs
        dask_jobs = jobs[jobs["Job Name"].str.contains("dask-worker.*")]
        # dask_jobs = jobs[jobs["Job Name"] == "dask-worker"]

        # remove all rows with "economy" in the "queue" column
        dask_jobs = dask_jobs[dask_jobs["Queue"] != "economy"]

        data_types = {
            "Req Mem (GB)": float,
            "Used Mem(GB)": float,
            "Elapsed (h)": float,
        }
        dask_jobs = dask_jobs.astype(data_types)

        # -- check if there are any dask jobs for this user
        if len(dask_jobs) == 0:
            warnings.warn("Warning! No Dask Jobs Found!")
            sys.exit()

        dask_jobs["Unused Mem (GB)"] = (
            dask_jobs["Req Mem (GB)"] - dask_jobs["Used Mem(GB)"]
        )
        dask_jobs["Unused Mem (%)"] = (
            dask_jobs["Unused Mem (GB)"] / dask_jobs["Req Mem (GB)"] * 100.0
        )
        self.dask_jobs = dask_jobs

    def dask_user_report(self, table=False) -> None:
        """
        Print memory usage summary of Dask workers.

        Parameters:
        -----------
        table (bool, optional):
                If True, prints the summary statistics in a tabular form. Defaults to False.
        """

        # -- compute summary stats for all fields
        fields = [
            "Unused Mem (%)",
            "Req Mem (GB)",
            "Used Mem(GB)",
            "Elapsed (h)",
            "Walltime (h)",
        ]
        result_dict = {}
        for field in fields:
            field_dict = compute_summary_stats(self.dask_jobs, field)
            result_dict.update(field_dict)

        if table:
            df = pd.DataFrame(result_dict)
            # Create a multi-level column header
            header = pd.MultiIndex.from_product(
                [["Memory usage summary of dask workers"], df.columns]
            )
            df.columns = header
            # -- two digits of precision
            df = df.applymap(lambda x: "{:.2f}".format(x))
            print(df)

        else:
            # print the results
            print("Memory usage summary of Dask workers")
            print("Number of jobs : ", len(self.dask_jobs))
            for key, inner_dict in result_dict.items():
                print(f"{key}:")
                print(f"\tmean: {inner_dict['mean']:.2f}")
                print(f"\tmin: {inner_dict['min']:.2f}")
                print(f"\tmax: {inner_dict['max']:.2f}")

            print("------------------------")
            print("Summary Overview:")
            bins = [0, 25, 50, 75, 100]
            labels = ["<25%", "25-50%", "50-75%", ">=75%"]
            bin_summary(self.dask_jobs, "Unused Mem (%)", bins, labels)

    def dask_csg_report(self, report: str, save_csv: bool = True) -> None:
        """
        Generate a report on Dask job usage for CSG staff.

        Parameters:
        -----------
        report : str
            A string containing the file path to save the report.
        save_csv : bool, optional
            A boolean specifying whether to write the report to a CSV file or not, by default True.

        Returns:
        --------
        None
        """
        grouped_dj = self.dask_jobs.groupby("User").agg(
            {
                "Req Mem (GB)": "mean",
                "Unused Mem (GB)": "mean",
                "Unused Mem (%)": "mean",
                "Elapsed (h)": "mean",
                "Job ID": "count",
            }
        )
        # -- show all users with Unused Mem > 0%
        dj_80 = grouped_dj[grouped_dj["Unused Mem (%)"] >= 0]
        dj_80["Unused Core-Hour (GB.hr)"] = (
            dj_80["Unused Mem (GB)"] * dj_80["Elapsed (h)"] * dj_80["Job ID"]
        )
        dj_80 = dj_80.rename(columns={"Job ID": "Dask job count"})
        pd.options.display.float_format = "{:.2f}".format
        print(dj_80.sort_values(by=["Unused Core-Hour (GB.hr)"], ascending=False).to_markdown())
        if save_csv:
            dj_80.to_csv(report, index=False)
