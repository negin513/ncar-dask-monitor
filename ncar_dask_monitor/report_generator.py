import os
import sys
import io
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
    median_val = summary.loc["50%"]
    min_val = summary.loc["min"]
    max_val = summary.loc["max"]
    result_dict = {
            field_name: { "mean": mean_val,"median":median_val, "min": min_val, "max": max_val}
    }
    if verbose:
        result_str = (
            f"Mean {field_name}   : {mean_val:.2f}\n"
            f"Median {field_name} : {median_val:.2f}\n"
            f"Min {field_name}    : {min_val:.2f}\n"
            f"Max {field_name}    : {max_val:.2f}"
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
        percentages_str.rename_axis(field_name)
        .reset_index(name="Jobs %")
        .to_string(index=False)
    )

class JobsSummary:
    """
    A class that reads a qhist log file, parse, provide some statistics on Dask jobs memory usage.

    Attributes:
        filename (str): The name of the file to extract data from.
        worker (str, optional): Name of the Dask job workers.
    """

    def __init__(self, filename, worker='dask',verbose=False):
        """
        Initializes a JobsSummary object.

        Args:
            filename (str): The name of the file to extract data from.
            worker (str, optional): Name of the Dask job workers.
        """
        self.filename = filename
        self.worker = worker
        self._read_all_jobs(verbose)

    def _read_all_jobs(self,verbose=False) -> None:
        """
        Read the qhist file and select Dask jobs only.
        """
        date_columns = ['Job Start', 'Job End']
        #date_format = '%Y-%m-%dT%H:%M:%S'

        # Check file existence
        if not os.path.exists(self.filename):
            warnings.warn(f"File not found: {self.filename}")
            return
        
        # Check if the file is empty or not
        with open(self.filename, 'r') as file:
    	    content = file.read()

        if "No jobs found matching search criteria" in content:
            warnings.warn("No jobs found matching search criteria!")
            return

        #jobs = pd.read_csv(self.filename, na_values='-',parse_dates=date_columns, date_format=date_format, skiprows=1)

        # Filter out any warning lines like '/glade/u/apps/opt/...'
        #lines = [line for line in content.splitlines() if not line.startswith("/glade/u/apps/opt/")]
        lines = content.splitlines()
        if lines and lines[0].startswith("/glade/u/apps/opt/"):
            # Skip the first two lines (warning + wrapped header fragment)
            lines = lines[2:]

        df = pd.read_csv(io.StringIO("\n".join(lines)))

        # Try reading as CSV
        try:
            jobs = pd.read_csv(
                io.StringIO("\n".join(lines)),
                na_values='-',
                parse_dates=date_columns,
                #infer_datetime_format=True
            )
        except Exception as e:
            warnings.warn(f"Error reading CSV: {e}")
            return


        #jobs = jobs.rename(columns={"AVG CPU": "AVG CPU(%)"})
        #print (jobs)
        jobs['Elapsed (h)'] = (jobs['Job End'] - jobs['Job Start']).dt.total_seconds() / 3600

        print ('-----')
        print (jobs)

        # -- check if there is any jobs for this user
        if len(jobs) == 0:
            warnings.warn("Warning! No jobs found for this user and this time period!")
            return

        # -- select dask-jobs
        #print (self.worker)
        #nan_values = jobs["Job Name"].isna().sum()
        #print("Number of NaN values in 'Job Name' column:", nan_values)
        jobs.dropna(subset=["Job Name"], inplace=True)
        
        if self.worker != 'all':
            if verbose:
                print(f"Selecting jobs with worker name containing '{self.worker}'")
            dask_jobs = jobs[jobs["Job Name"].str.contains(self.worker)]
        else:
            if verbose:
                print("Selecting all jobs as worker is set to 'all'")
            dask_jobs = jobs

        #dask_jobs = jobs[jobs["Job Name"].str.contains("dask-worker*")]
        ## dask_jobs = jobs[jobs["Job Name"] == "dask-worker"]

        # remove all rows with "economy" in the "queue" column
        dask_jobs = dask_jobs[dask_jobs["Queue"] != "economy"]

        dask_jobs= dask_jobs.dropna()

        data_types = {
            "Req Mem": float,
            "Used Mem": float,
            "Elapsed": float,
        }
        dask_jobs = dask_jobs.astype(data_types)

        # -- check if there are any dask jobs for this user
        if len(dask_jobs) == 0:
            #warnings.warn("Warning! No Dask Jobs Found!")
            warnings.warn("Warning! No jobs found for this user and this time period!")
            sys.exit()

        dask_jobs["Unused Mem"] = (
            dask_jobs["Req Mem"] - dask_jobs["Used Mem"]
        )
        dask_jobs["Unused Mem (%)"] = (
            dask_jobs["Unused Mem"] / dask_jobs["Req Mem"] * 100.0
        )

        #pd.options.display.float_format = '{:,.2f}'.format

        if verbose:
            exclude_columns = ["Job End", "Job Start"]
            pd.set_option("display.max_rows", None)
            pd.options.display.float_format = "{:,.2f}".format

            print("\n==============================")
            print("Dask Job Summary [Filtered]")
            print("==============================\n")

            # Print summary table (excluding start/end times)
            print(dask_jobs.drop(columns=exclude_columns, errors="ignore").to_string(index=False))
            print("\n------------------------------")

            # Find and display jobs with min/max unused memory
            max_unused = dask_jobs.loc[dask_jobs["Unused Mem (%)"].idxmax()].drop(labels=exclude_columns, errors="ignore")
            min_unused = dask_jobs.loc[dask_jobs["Unused Mem (%)"].idxmin()].drop(labels=exclude_columns, errors="ignore")

            print("Job with Highest Unused Memory (%):")
            print(max_unused.to_string())
            print("\nJob with Lowest Unused Memory (%):")
            print(min_unused.to_string())

            print("\n==============================\n")


        self.dask_jobs = dask_jobs

    def dask_user_report(self, table=False,verbose=False) -> None:
        """
        Print memory usage summary of Dask workers.

        Parameters:
        -----------
        table (bool, optional):
                If True, prints the summary statistics in a tabular form. Defaults to False.
        """
        
        if verbose:
            print ("----------------------------------------------")
            exclude_columns=['Job End', 'Job Start','Exit Status']
            print (self.dask_jobs.drop(columns=exclude_columns).describe())
            print ("----------------------------------------------")
        # -- compute summary stats for all fields
        fields = [
            "Used Mem",
            "Req Mem",
            "Unused Mem (%)",
            "Avg CPU",
            "Elapsed (h)",
            #"Walltime (h)",
        ]
        result_dict = {}
        for field in fields:
            field_dict = compute_summary_stats(self.dask_jobs, field,verbose)
            result_dict.update(field_dict)

        df = pd.DataFrame(result_dict)
        # Create a multi-level column header
        header = pd.MultiIndex.from_product(
            [["Resource Usage Summary of Jobs"], df.columns]
        )
        df.columns = header
        # -- two digits of precision
        #df = df.applymap(lambda x: "{:.2f}".format(x))
        #df = df.apply(lambda x: x.map(lambda y: "{:.2f}".format(y)))

        print("------------------------")
        print("Number of jobs : ", len(self.dask_jobs))

        if table:
            print(df.apply(lambda x: x.map(lambda y: "{:.2f}".format(y))))

        else:
            # print the results
            for key, inner_dict in result_dict.items():
                print(f"{key}:")
                print(f"\tmean   : {inner_dict['mean']:.2f}")
                print(f"\tmedian : {inner_dict['median']:.2f}")
                print(f"\tmin    : {inner_dict['min']:.2f}")
                print(f"\tmax    : {inner_dict['max']:.2f}")

            print("------------------------")
            print("Summary Overview:")
            bins = [0, 25, 50, 75, 100]
            labels = ["<25%", "25-50%", "50-75%", ">=75%"]
            bin_summary(self.dask_jobs, "Unused Mem (%)", bins, labels)
            bin_summary(self.dask_jobs, "Avg CPU", bins, labels)


    def dask_csg_report(self, report: str, save_csv: bool = True) -> None:
        """
        Generate a report on Dask job usage for CSG staff.

        Parameters
        ----------
        report : str
            Path to save the report CSV file.
        save_csv : bool, optional
            Whether to write the report to a CSV file (default: True).
        """
        # ---- Group by user, keeping your original column names
        grp = self.dask_jobs.groupby("User")

        grouped_dj = grp.agg({
            "Req Mem": "mean",
            "Unused Mem": "mean",
            "Unused Mem (%)": "mean",
            "Elapsed (h)": "mean",       # or "Elapsed (h)" if you prefer hours
        })

        # Add a job count column named "Job ID" for consistency
        grouped_dj["Job ID"] = grp.size()

        grouped_dj = grouped_dj.reset_index()

        # ---- Filter users with Unused Mem (%) >= 0
        dj_agg = grouped_dj[grouped_dj["Unused Mem (%)"] >= 0].copy()

        # ---- Compute unused core-hour metric (GB * hr * job count)
        dj_agg["Unused Core-Hour (GB.hr)"] = (
            dj_agg["Unused Mem"] * dj_agg["Elapsed (h)"] * dj_agg["Job ID"]
        )

        # ---- Rename the count column to clarify meaning
        dj_agg = dj_agg.rename(columns={"Job ID": "Dask Job Count"})

        # ---- Display nicely formatted summary
        pd.options.display.float_format = "{:.2f}".format
        print(
            dj_agg.sort_values(
                by=["Unused Core-Hour (GB.hr)"], ascending=False
            ).to_string(index=False)
        )

        # ---- Save to CSV if requested
        if save_csv:
            dj_agg.to_csv(report, index=False)
            print(f"\nReport saved to: {report}")

