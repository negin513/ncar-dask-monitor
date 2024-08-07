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
        date_format = '%Y-%m-%dT%H:%M:%S'

        # Check if the file is empty or not
        with open(self.filename, 'r') as file:
    	    content = file.read()

        if "No jobs found matching search criteria" in content:
            warnings.warn("No jobs found matching search criteria!")
            sys.exit()

        jobs = pd.read_csv(self.filename, na_values='-',parse_dates=date_columns, date_format=date_format)
        jobs = jobs.rename(columns={"Avg CPU (%)": "CPU (%)"})
        #print (jobs)


        jobs['Elapsed (h)'] = (jobs['Job End'] - jobs['Job Start']).dt.total_seconds() / 3600

        # -- check if there is any jobs for this user
        if len(jobs) == 0:
            warnings.warn("Warning! No jobs found for this user and this time period!")
            sys.exit()

        # -- select dask-jobs
        #print (self.worker)
        #nan_values = jobs["Job Name"].isna().sum()
        #print("Number of NaN values in 'Job Name' column:", nan_values)
        jobs.dropna(subset=["Job Name"], inplace=True)
        
        if self.worker != 'all':
            dask_jobs = jobs[jobs["Job Name"].str.contains(self.worker)]
        else:
            dask_jobs = jobs

        #dask_jobs = jobs[jobs["Job Name"].str.contains("dask-worker*")]
        ## dask_jobs = jobs[jobs["Job Name"] == "dask-worker"]

        # remove all rows with "economy" in the "queue" column
        dask_jobs = dask_jobs[dask_jobs["Queue"] != "economy"]

        dask_jobs= dask_jobs.dropna()

        data_types = {
            "Req Mem (GB)": float,
            "Used Mem(GB)": float,
            "Elapsed (h)": float,
        }
        dask_jobs = dask_jobs.astype(data_types)

        # -- check if there are any dask jobs for this user
        if len(dask_jobs) == 0:
            #warnings.warn("Warning! No Dask Jobs Found!")
            warnings.warn("Warning! No jobs found for this user and this time period!")
            sys.exit()

        dask_jobs["Unused Mem (GB)"] = (
            dask_jobs["Req Mem (GB)"] - dask_jobs["Used Mem(GB)"]
        )
        dask_jobs["Unused Mem (%)"] = (
            dask_jobs["Unused Mem (GB)"] / dask_jobs["Req Mem (GB)"] * 100.0
        )

        #pd.options.display.float_format = '{:,.2f}'.format

        if verbose: 
            print ('---------------')
            exclude_columns=['Job End', 'Job Start']
            pd.set_option('display.max_rows', None)
            print (dask_jobs.drop(columns=exclude_columns).to_string(float_format='{:,.2f}'.format))
            #print (dask_jobs.drop(columns=exclude_columns))
            print ('---------------')
            print ('minimum memory % job:')
            print (dask_jobs.drop(columns=exclude_columns).loc[dask_jobs['Unused Mem (%)'].idxmax()])
            print ('maximum memory % job:')
            print (dask_jobs.drop(columns=exclude_columns).loc[dask_jobs['Unused Mem (%)'].idxmin()])


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
            "Used Mem(GB)",
            "Req Mem (GB)",
            "Unused Mem (%)",
            "CPU (%)",
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
            bin_summary(self.dask_jobs, "CPU (%)", bins, labels)


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
        dj_80 = grouped_dj[grouped_dj["Unused Mem (%)"] >= 0].copy()
        dj_80["Unused Core-Hour (GB.hr)"] = (
            dj_80["Unused Mem (GB)"] * dj_80["Elapsed (h)"] * dj_80["Job ID"]
        )
        dj_80 = dj_80.rename(columns={"Job ID": "Dask job count"})
        pd.options.display.float_format = "{:.2f}".format
        print(dj_80.sort_values(by=["Unused Core-Hour (GB.hr)"], ascending=False).to_string())
        #print(dj_80.sort_values(by=["Unused Core-Hour (GB.hr)"], ascending=False).to_markdown())
        
        if save_csv:
            dj_80.to_csv(report)
