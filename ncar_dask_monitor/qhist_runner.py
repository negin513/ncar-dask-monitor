import sys
import subprocess


class QhistRunner:
    """
    Run qhist commands for a given user and date range, saving output to a file.

    Attributes:
        start_date (str): The start date of the date range to extract.
        end_date (str): The end date of the date range to extract.
-       filename (str): The name of the file to extract data from.
        username (str): The username for which to extract qhist data.
    """

    def __init__(self, start_date, end_date, filename, username):
        self.start_date = start_date
        self.end_date = end_date
        self.filename = filename
        self.username = username

    def _create_command(self):
        """
        Construct the qhist command string.

        Returns:
            str: The constructed qhist command.
        """
        qformat = (
            "'id,user,queue,numnodes,numcpus,reqmem,memory,start,end,"
            "elapsed,walltime,waittime,name,avgcpu,resources,status'"
        )

        base = f"qhist --format={qformat} -p {self.start_date}-{self.end_date}"

        if self.username and self.username != "all":
            command = f"{base} -u {self.username} -c > {self.filename}"
        else:
            command = f"{base} -c > {self.filename}"

        return command

    def run_shell_code(self, verbose=False):
        """
        Run the qhist command and capture stdout/stderr safely.

        Args:
            verbose (bool): If True, print the command and its output.  
        
        Returns:
            str: The standard output from the command execution.
        """
        command = self._create_command()
        if verbose:
            print(">>", command)

        process = subprocess.Popen(
            command,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        stdout, stderr = process.communicate()

        if process.returncode != 0:
            print(f"Error running qhist:\n{stderr.strip()}")
            sys.exit(1) 

        return stdout.strip()
