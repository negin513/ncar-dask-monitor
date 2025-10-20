import os
import unittest
import pandas as pd
from unittest.mock import MagicMock, patch
from ncar_dask_monitor.report_generator import compute_summary_stats, bin_summary, JobsSummary


class TestReportGenerator(unittest.TestCase):
    def setUp(self):
        self.data = {
            "Req Mem (GB)": [2.0, 4.0, 8.0, 16.0, 32.0],
            "Used Mem(GB)": [1.0, 3.0, 3.0, 4.0, 5.0],
            "Unused Mem (GB)": [ 1.0, 1.0, 5.0, 12.0, 27.0],
            "Elapsed (h)": [1.0, 2.0, 3.0, 4.0, 5.0],
            "Job Name": [
                "job1",
                "job2",
                "job3",
                "dask-worker.1",
                "dask-worker.2",
            ],
        }
        self.df = pd.DataFrame(self.data)
        self.df['Unused Mem (%)']= (self.df['Unused Mem (GB)']/self.df['Req Mem (GB)'])*100

    def test_compute_summary_stats(self):
        expected_result_dict = {
            "Used Mem(GB)": {
                #"count": 5.0,
                "mean": 3.2,
                'median': 3.0,
                "min": 1.0,
                "max": 5.0,
            }
        }
        result_dict = compute_summary_stats(self.df, "Used Mem(GB)")
        self.assertEqual(expected_result_dict, result_dict)

    def test_bin_summary(self):
        with patch("builtins.print") as mock_print:
            bin_summary(self.df, "Unused Mem (%)", [0, 50, 100], ["<50%", ">=50%"])
            mock_print.assert_called_once_with(
                "Unused Mem (%) Jobs %\n         >=50% 80.00%\n          <50% 20.00%"
            )

class TestJobsSummary(unittest.TestCase):
    def setUp(self):
        test_dir = os.path.dirname(os.path.abspath(__file__))
        self.csv_file = os.path.join(test_dir, 'qhist.log')


    def test_JobsSummary_read_all_jobs(self):
        jobs_summary = JobsSummary(self.csv_file)
        jobs_summary._read_all_jobs()
        self.assertIsNotNone(jobs_summary.dask_jobs)

    def test_JobsSummary_dask_user_report(self):
        jobs_summary = JobsSummary(self.csv_file)
        jobs_summary._read_all_jobs()
        with patch("builtins.print") as mock_print:
            jobs_summary.dask_user_report(table=True)
            self.assertEqual(mock_print.call_count, 3)
            jobs_summary.dask_user_report(table=False)
            self.assertEqual(mock_print.call_count, 33)

    def test_JobsSummary_dask_csg_report(self):
        jobs_summary = JobsSummary(self.csv_file)
        jobs_summary._read_all_jobs()
        with patch("builtins.print") as mock_print:
            jobs_summary.dask_csg_report("report.csv", save_csv=False)
            self.assertEqual(mock_print.call_count, 2)
            jobs_summary.dask_csg_report("report.csv", save_csv=True)
            self.assertEqual(mock_print.call_count, 4)
            os.remove ("report.csv")
