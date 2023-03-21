import sys
import pandas as pd

def compute_summary_stats(df: pd.DataFrame, field_name: str) -> None:
    """
    Compute and print the count, mean, min, and max values of a field in a Dat
 
    Parameters:
        df (pd.DataFrame): The input DataFrame.
        field_name (str): The name of the field to compute the summary statist
 
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
    count = summary.loc['count']
    mean_val = summary.loc['mean']
    min_val = summary.loc['min']
    max_val = summary.loc['max']
    result_str = (
        f"Mean {field_name}: {mean_val:.2f}\n"
        f"Min {field_name}: {min_val:.2f}\n"
        f"Max {field_name}: {max_val:.2f}"
    )
    print(result_str)



def bin_summary(dask_jobs: pd.DataFrame, field_name: str, bins:list, labels:list) -> None:
    # -- overall statitics :
    # create bins based on desired ranges
    bins = [0, 25, 50, 75, 100]
    labels = ['<25%','25-50%','50-75%','>=75%']

    # create a new column with the bins
    dask_jobs['bin'] = pd.cut(dask_jobs['Unused Mem (%)'], bins=bins, include_lowest=True, right=False, labels=labels)
 
    # calculate the percentage of jobs in each bin
    percentages = dask_jobs['bin'].value_counts(normalize=True) * 100
 
    # show the resulting percentages
    percentages= percentages.sort_index(ascending=False)
    percentages_str = percentages.map('{:.2f}%'.format)

    print(percentages_str.rename_axis('Unused Mem (%)').reset_index(name='Jobs %').to_string(index=False))


class JobsSummary:
    """
    A class that reads a qhist log file, parse, give some stats.

    Attributes:
        filename (str): The name of the file to extract data from.
    """
    def __init__(self, filename):
        """
        Initializes a QhistSummary object.

        Args:
            filename (str): The name of the file to extract data from.
        """
        self.filename = filename
        self._read_file()

    def _read_file(self):
        """
        Read the qhist file and select dask jobs only!
        """
        jobs = pd.read_csv (self.filename)

        # -- select dask-jobs :
        dask_jobs = jobs[jobs['Job Name'] == 'dask-worker']
        data_types = {'Req Mem (GB)': float, 'Used Mem(GB)': float, 'Elapsed (h)':float}
        dask_jobs = dask_jobs.astype(data_types)

        if len(dask_jobs)==0:
            print ("Warning! No Dask Jobs Found!")

        dask_jobs['Unused Mem (GB)'] = dask_jobs['Req Mem (GB)'] - dask_jobs['Used Mem(GB)']
        dask_jobs['Unused Mem (%)'] = dask_jobs['Unused Mem (GB)'] /dask_jobs['Req Mem (GB)'] * 100.0
        self.dask_jobs = dask_jobs


    def user_report (self):
        """
        """

        # print the results
        print ("Memory usage summary of dask workers")
        print("Number of jobs : ", len(self.dask_jobs))
        compute_summary_stats(self.dask_jobs ,'Unused Mem (%)')
        print ('------------------------')
        compute_summary_stats(self.dask_jobs ,'Req Mem (GB)')
        print ('------------------------')
        compute_summary_stats(self.dask_jobs ,'Used Mem(GB)')

        print ('------------------------')
        print ('Summary:')

        bins = [0, 25, 50, 75, 100]
        labels = ['<25%','25-50%','50-75%','>=75%']

        bin_summary(self.dask_jobs, 'Unused Mem (%)', bins, labels)

    def csg_report (self, report):
        grouped_dj = self.dask_jobs.groupby('User').agg({'Req Mem (GB)':'mean','Unused Mem (GB)': 'mean', 'Unused Mem (%)': 'mean','Elapsed (h)': 'mean', 'Job ID': 'count'})
        dj_80 = grouped_dj[grouped_dj['Unused Mem (%)']>80]
        dj_80 ['Unused Core-Hour (GB.hr)'] = dj_80['Unused Mem (GB)']*dj_80['Elapsed (h)']*dj_80['Job ID']
        dj_80=dj_80.rename(columns = {'Job ID':'Dask job count'})
        pd.options.display.float_format = '{:.2f}'.format
        print (dj_80.sort_values(by=['Unused Core-Hour (GB.hr)'], ascending=False))
        dj_80.to_csv(file_path, index=False)


