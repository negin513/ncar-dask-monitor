import unittest
import argparse

from unittest.mock import MagicMock, patch
from datetime import datetime, timedelta

from dask_mem_usage import parse_arguments, validate_dates, run_qhist


class TestDaskMemUsage(unittest.TestCase):
    """
    Test the dask_mem_usage module functions.
    """

    def test_parse_arguments(self):
        """
        Test the parse_arguments function.
        """
        with patch("argparse.ArgumentParser.parse_args") as mock_parse_args:
            mock_parse_args.return_value = MagicMock(
                start_date="20220301",
                end_date="20220302",
                user="testuser",
                filename="log.txt",
                verbose=True,
            )

            args = parse_arguments()
            self.assertEqual(args.start_date, "20220301")
            self.assertEqual(args.end_date, "20220302")
            self.assertEqual(args.user, "testuser")
            self.assertEqual(args.filename, "log.txt")
            self.assertEqual(args.verbose, True)

    def test_missing_start_date_and_days(self):
        """
        Test the validate_dates function when start_date and days are missing.
        """
        args = argparse.Namespace(start_date=None, days=None, end_date=None)
        parser = argparse.ArgumentParser()
        parser.error = MagicMock()

        validate_dates(args, parser)

        parser.error.assert_called_once_with(
            "Either --start-date or -d/-days option must be provided."
        )

    def test_missing_end_date(self):
        """
        Test the validate_dates function when end_date is missing.
        """
        args = argparse.Namespace(start_date="20230101", days=None, end_date=None)
        parser = argparse.ArgumentParser()
        parser.error = MagicMock()

        validate_dates(args, parser)

        parser.error.assert_called_once_with(
            "End date is required if start date is provided."
        )

    def test_end_date_less_than_start_date(self):
        """
        Test the validate_dates function when end_date is less than start_date.
        """
        args = argparse.Namespace(start_date="20230110", days=None, end_date="20230101")
        parser = argparse.ArgumentParser()
        parser.error = MagicMock()

        validate_dates(args, parser)

        parser.error.assert_called_once_with(
            "End date must be greater than start date."
        )

    def test_valid_dates_with_days(self):
        """
        Test the validate_dates function with days.
        """
        args = argparse.Namespace(start_date=None, days=7, end_date=None)
        parser = argparse.ArgumentParser()
        parser.error = MagicMock()

        validate_dates(args, parser)

        self.assertIsNotNone(args.start_date)
        self.assertIsNotNone(args.end_date)
        self.assertNotEqual(args.start_date, args.end_date)

    def test_validate_dates_days(self):
        """
        Test the validate_dates function with valid dates and days.
        """
        args = MagicMock(start_date=None, end_date=None, days=3)
        parser = MagicMock()
        validate_dates(args, parser)
        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=3)).strftime("%Y%m%d")
        self.assertEqual(args.start_date, start_date)
        self.assertEqual(args.end_date, end_date)

    def test_run_qhist(self):
        """
        Test the run_qhist function.
        """
        with patch("dask_mem_usage.QhistRunner") as mock_qhist_runner, patch(
            "dask_mem_usage.JobsSummary"
        ) as mock_jobs_summary:

            mock_qhist_runner.return_value.run_shell_code = MagicMock()
            mock_jobs_summary.return_value.dask_user_report = MagicMock()

            args = MagicMock(
                start_date="20220301",
                end_date="20220302",
                filename="log.txt",
                user="testuser",
            )
            run_qhist(args)

            mock_qhist_runner.assert_called_once_with(
                "20220301", "20220302", "log.txt", "testuser"
            )
            mock_qhist_runner.return_value.run_shell_code.assert_called_once()
            mock_jobs_summary.assert_called_once_with("log.txt")
            mock_jobs_summary.return_value.dask_user_report.assert_called_once()


if __name__ == "__main__":
    unittest.main()
