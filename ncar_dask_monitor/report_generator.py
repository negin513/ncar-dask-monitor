import os
import io
import sys
import warnings

import pandas as pd
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.columns import Columns
from rich.box import ROUNDED, DOUBLE, HEAVY
from rich.text import Text

console = Console()


def compute_summary_stats(df, field_name: str, verbose: bool = False) -> dict:
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
    if field_name not in df.columns:
        warnings.warn(f"Missing column: {field_name}")
        return {}

    s = pd.to_numeric(df[field_name], errors="coerce").dropna()
    if s.empty:
        warnings.warn(f"No numeric data found in column: {field_name}")
        return {}

    summary = s.describe()
    count = int(summary.loc["count"])
    mean_val = float(summary.loc["mean"])
    median_val = float(summary.loc["50%"])
    min_val = float(summary.loc["min"])
    max_val = float(summary.loc["max"])

    result_dict = {
        field_name: {"mean": mean_val, "median": median_val, "min": min_val, "max": max_val}
    }

    if verbose:
        is_percent = "%" in field_name
        def fmt(x: float) -> str:
            return f"{x:,.2f}%" if is_percent else f"{x:,.2f}"

        table = Table(title=f"Summary: {field_name}", box=ROUNDED, show_header=True, header_style="bold cyan")
        table.add_column("Statistic", style="bold")
        table.add_column("Value", justify="right", style="green")

        table.add_row("Count", f"{count:,}")
        table.add_row("Mean", fmt(mean_val))
        table.add_row("Median", fmt(median_val))
        table.add_row("Min", fmt(min_val))
        table.add_row("Max", fmt(max_val))

        console.print(table)

    return result_dict


def bin_summary(
    df, field_name: str, bins: list = None, labels: list = None, print_table: bool = True
) -> Table:
    """
    Compute and print the percentage of a df column in each bin.

    Parameters:
        df (pd.DataFrame): The input DataFrame containing Dask jobs.
        field_name (str): The name of the field to compute the summary statistics.
        bins (list): List of bins for binning. Default is [0, 25, 50, 75, 100].
        labels (list): List of labels for the bins. Default is ['<25%', '25-50%', '50-75%', '>=75%'].
        print_table (bool): Whether to print the table (default True). If False, returns the table.

    Returns:
        Table: The rich Table object containing the distribution.
    """
    if field_name not in df.columns:
        warnings.warn(f"Missing column: {field_name}")
        return None

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

    # Shorter title for compactness
    short_name = field_name.replace("Unused ", "").replace(" (%)", "")
    table = Table(title=f"{short_name} Usage", box=ROUNDED, show_header=True, header_style="bold cyan")
    table.add_column("Range", style="bold")
    table.add_column("Jobs", justify="right", style="green")

    for idx, val in percentages.items():
        table.add_row(str(idx), f"{val:.1f}%")

    if print_table:
        console.print(table)

    return table


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

        # Filter out any warning lines like '/glade/u/apps/opt/...'
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
            )
        except Exception as e:
            warnings.warn(f"Error reading CSV: {e}")
            return

        # --- Normalize column names ---
        rename_map = {
            "Req Mem": "Req Mem (GB)",
            "Used Mem": "Used Mem (GB)",
            "Avg CPU": "CPU (%)",    # <-- key rename
            "AVG CPU": "CPU (%)",    # <-- for older qhist output variants
        }
        jobs.rename(columns=rename_map, inplace=True)

        jobs['Elapsed (h)'] = (jobs['Job End'] - jobs['Job Start']).dt.total_seconds() / 3600

        # -- check if there is any jobs for this user
        if len(jobs) == 0:
            warnings.warn("Warning! No jobs found for this user and this time period!")
            return

        # -- select dask-jobs
        jobs.dropna(subset=["Job Name"], inplace=True)
        
        if self.worker != 'all':
            if verbose:
                print(f"Selecting jobs with worker name containing '{self.worker}'")
            dask_jobs = jobs[jobs["Job Name"].str.contains(self.worker)]
        else:
            if verbose:
                print("Selecting all jobs as worker is set to 'all'")
            dask_jobs = jobs

        # remove all rows with "economy" in the "queue" column
        dask_jobs = dask_jobs[dask_jobs["Queue"] != "economy"]

        dask_jobs= dask_jobs.dropna()

        data_types = {
            "Req Mem (GB)": float,
            "Used Mem (GB)": float,
            "Elapsed": float,
        }
        dask_jobs = dask_jobs.astype(data_types)

        # -- check if there are any dask jobs for this user
        if len(dask_jobs) == 0:
            warnings.warn("Warning! No jobs found for this user and this time period!")
            sys.exit(1)

        dask_jobs["Unused Mem (GB)"] = (
            dask_jobs["Req Mem (GB)"] - dask_jobs["Used Mem (GB)"]
        )
        dask_jobs["Used Mem (%)"] = (
            dask_jobs["Used Mem (GB)"] / dask_jobs["Req Mem (GB)"] * 100.0
        )
        dask_jobs["Unused Mem (%)"] = (
            dask_jobs["Unused Mem (GB)"] / dask_jobs["Req Mem (GB)"] * 100.0
        )

        if verbose:
            exclude_columns = ["Job End", "Job Start"]
            pd.set_option("display.max_rows", None)
            pd.options.display.float_format = "{:,.2f}".format

            # Print pandas DataFrame directly
            df_display = dask_jobs.drop(columns=exclude_columns, errors="ignore")
            print("\nDask Job Summary [Filtered]")
            print(df_display.head())

            # Find and display jobs with min/max unused memory
            max_unused = dask_jobs.loc[dask_jobs["Unused Mem (%)"].idxmax()].drop(labels=exclude_columns, errors="ignore")
            min_unused = dask_jobs.loc[dask_jobs["Unused Mem (%)"].idxmin()].drop(labels=exclude_columns, errors="ignore")

            max_table = Table(title="Job with Highest Unused Memory (%)", box=ROUNDED, show_header=True, header_style="bold yellow")
            max_table.add_column("Field", style="bold")
            max_table.add_column("Value", justify="right", style="red")
            for field, val in max_unused.items():
                max_table.add_row(str(field), f"{val:.2f}" if isinstance(val, float) else str(val))
            console.print(max_table)

            min_table = Table(title="Job with Lowest Unused Memory (%)", box=ROUNDED, show_header=True, header_style="bold yellow")
            min_table.add_column("Field", style="bold")
            min_table.add_column("Value", justify="right", style="green")
            for field, val in min_unused.items():
                min_table.add_row(str(field), f"{val:.2f}" if isinstance(val, float) else str(val))
            console.print(min_table)

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
            exclude_columns=['Job End', 'Job Start','Exit Status']
            desc_df = self.dask_jobs.drop(columns=exclude_columns, errors="ignore").describe()
            desc_table = Table(title="Job Statistics Summary", box=HEAVY, show_header=True, header_style="bold magenta")
            desc_table.add_column("Stat", style="bold")
            for col in desc_df.columns:
                desc_table.add_column(col, justify="right")
            for idx, row in desc_df.iterrows():
                desc_table.add_row(str(idx), *[f"{v:.2f}" if isinstance(v, float) else str(v) for v in row])
            console.print(desc_table)
        # -- compute summary stats for all fields
        fields = [
            "Used Mem (GB)",
            "Req Mem (GB)",
            "Unused Mem (%)",
            "CPU (%)",
            "Elapsed (h)",
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

        if table:
            console.print(Panel(f"[bold green]Number of jobs: {len(self.dask_jobs)}[/bold green]", box=ROUNDED))
            summary_table = Table(title="Resource Usage Summary of Jobs", box=DOUBLE, show_header=True, header_style="bold cyan")
            summary_table.add_column("Metric", style="bold")
            for col in result_dict.keys():
                summary_table.add_column(col, justify="right")
            for stat in ["mean", "median", "min", "max"]:
                summary_table.add_row(stat, *[f"{result_dict[col][stat]:.2f}" for col in result_dict.keys()])
            console.print(summary_table)

        else:
            # Create prominent summary header with aggregated results
            num_jobs = len(self.dask_jobs)
            total_mem_requested = self.dask_jobs["Req Mem (GB)"].sum()
            total_mem_used = self.dask_jobs["Used Mem (GB)"].sum()
            avg_cpu_pct = self.dask_jobs["CPU (%)"].mean() if "CPU (%)" in self.dask_jobs.columns else None
            avg_elapsed_seconds = self.dask_jobs["Elapsed (h)"].mean() * 3600  # Convert hours to seconds

            # Memory per CPU metrics
            avg_requested_mem_per_cpu = (self.dask_jobs["Req Mem (GB)"] / self.dask_jobs["NCPUs"]).mean()
            avg_used_mem_per_cpu = (self.dask_jobs["Used Mem (GB)"] / self.dask_jobs["NCPUs"]).mean()

            # Calculate average memory utilization (used/requested)
            avg_mem_util_pct = (total_mem_used / total_mem_requested * 100) if total_mem_requested > 0 else 0

            # Build summary panel content with cleaner format
            summary_lines = []
            summary_lines.append(f"  Memory Requested (avg/CPU):   {avg_requested_mem_per_cpu:.2f} GB")
            summary_lines.append(f"  Memory Used (avg/CPU):        {avg_used_mem_per_cpu:.2f} GB")
            summary_lines.append(f"  Memory Utilization:              {avg_mem_util_pct:.1f}%")
            if avg_cpu_pct is not None:
                summary_lines.append(f"  CPU Utilization:                 {avg_cpu_pct:.1f}%")
            summary_lines.append(f"  Job Duration (avg):            {avg_elapsed_seconds:,.0f} s")

            summary_content = "\n".join(summary_lines)

            console.print(Panel(
                summary_content,
                title=f"Resource Usage Summary ({num_jobs:,} jobs)",
                box=DOUBLE,
                padding=(0, 0)
            ))
            console.print()

            # print the results using rich table
            stats_table = Table(box=ROUNDED, show_header=True, header_style="bold")
            stats_table.add_column("Metric", style="bold")
            stats_table.add_column("Mean", justify="right", style="green")
            stats_table.add_column("Median", justify="right", style="yellow")
            stats_table.add_column("Min", justify="right", style="blue")
            stats_table.add_column("Max", justify="right", style="red")

            for key, inner_dict in result_dict.items():
                stats_table.add_row(
                    key,
                    f"{inner_dict['mean']:.2f}",
                    f"{inner_dict['median']:.2f}",
                    f"{inner_dict['min']:.2f}",
                    f"{inner_dict['max']:.2f}"
                )
            console.print(stats_table)
            console.print()

            # Build distribution tables side-by-side
            bins = [0, 25, 50, 75, 100]
            labels = ["<25%", "25-50%", "50-75%", ">=75%"]

            dist_tables = []
            mem_table = bin_summary(self.dask_jobs, "Unused Mem (%)", bins, labels, print_table=False)
            if mem_table:
                dist_tables.append(mem_table)

            if "CPU (%)" in self.dask_jobs.columns:
                cpu_table = bin_summary(self.dask_jobs, "CPU (%)", bins, labels, print_table=False)
                if cpu_table:
                    dist_tables.append(cpu_table)

            if dist_tables:
                console.print(Columns(dist_tables, equal=True, expand=False))

    def dask_csg_report(self, report: str, save_csv: bool = True, sort_var="mem") -> None:
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
            "Req Mem (GB)": "mean",
            "Unused Mem (GB)": "mean",
            "Unused Mem (%)": "mean",
            "Elapsed (h)": "mean",
            "CPU (%)": "mean",
        })

        # Add a job count column named "Job ID" for consistency
        grouped_dj["Job ID"] = grp.size()

        grouped_dj = grouped_dj.reset_index()

        # ---- Filter users with Unused Mem (%) >= 0
        dj_agg = grouped_dj[grouped_dj["Unused Mem (%)"] >= 0].copy()

        # ---- Compute unused core-hour metric (GB * hr * job count)
        dj_agg["Unused MemxHour (GB.hr)"] = (
            dj_agg["Unused Mem (GB)"] * dj_agg["Elapsed (h)"] * dj_agg["Job ID"]
        )

        dj_agg["Unused CPU-Hour"] = (
            (100 - dj_agg["CPU (%)"]) / 100 * dj_agg["Elapsed (h)"] * dj_agg["Job ID"]
        )

        dj_agg = dj_agg.rename(columns={"Job ID": "Job Count"})

        avg_utilized_mem_cpu = (self.dask_jobs["Used Mem (GB)"] / self.dask_jobs["NCPUs"]).mean()
        avg_requested_mem_cpu = (self.dask_jobs["Req Mem (GB)"] / self.dask_jobs["NCPUs"]).mean()

        console.print()
        mem_cpu_text = Text()
        mem_cpu_text.append("Memory per CPU: ", style="bold")
        mem_cpu_text.append(f"{avg_utilized_mem_cpu:.2f}", style="green")
        mem_cpu_text.append(" GB used  /  ", style="dim")
        mem_cpu_text.append(f"{avg_requested_mem_cpu:.2f}", style="yellow")
        mem_cpu_text.append(" GB requested", style="dim")
        console.print(mem_cpu_text)

        # ---- Display nicely formatted summary
        pd.options.display.float_format = "{:.2f}".format
        if sort_var == "mem":
            sort_variable = "Unused MemxHour (GB.hr)"
        elif sort_var == "cpu":
            sort_variable = "Unused CPU-Hour"
        else:
            sort_variable = "Unused MemxHour (GB.hr)"

        sorted_df = dj_agg.sort_values(by=[sort_variable], ascending=False)

        csg_table = Table(title="All User Report", box=ROUNDED, show_header=True, header_style="bold cyan")
        for col in sorted_df.columns:
            csg_table.add_column(col, justify="right" if sorted_df[col].dtype in ['float64', 'int64'] else "left")
        for _, row in sorted_df.iterrows():
            csg_table.add_row(*[f"{v:.2f}" if isinstance(v, float) else str(v) for v in row])
        console.print(csg_table)

        # ---- Save to CSV if requested
        if save_csv:
            dj_agg.to_csv(report, index=False)
