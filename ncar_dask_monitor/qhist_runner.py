import sys
import subprocess

class QhistRunner:
    """
    A class that runs shell commands to extract data from a file.

    Attributes:
        start_date (str): The start date of the date range to extract.
        end_date (str): The end date of the date range to extract.
        filename (str): The name of the file to extract data from.
    """
    def __init__(self, start_date, end_date, filename, username):
        """
        Initializes a ShellRunner object.

        Args:
            start_date (str): The start date of the date range to extract.
            end_date (str): The end date of the date range to extract.
            filename (str): The name of the file to extract data from.
        """
        self.start_date = start_date
        self.end_date = end_date
        self.filename = filename
        self.username = username

    def _create_command(self):
        """
        Creates a shell command for running qhist

        Returns:
            str: The shell command.
        """
        #Job ID,Queue,Nodes,NCPUs,NGPUs,Req Mem (GB),Used Mem(GB),Job Submit,Job Start,Job End,Walltime (h),Exit Status,Job Name
        qformat = "'user,queue,numnodes,numcpus,reqmem,memory,start,end,elapsed,walltime,waittime,name,avgcpu,resources,status'"

        if self.username and self.username != 'all':
            command = "qhist --format="+qformat,\
                        " -p " + self.start_date+'-'+self.end_date, \
                        " -u " + self.username, \
                        " -c ", " >& " + self.filename

        else:
            command = "qhist --format="+qformat+ \
                        " -p " + self.start_date+'-'+self.end_date, \
                        " -c ", " >& " + self.filename

        command = ''.join(str(i) for i in command)
        return command

    def run_shell_code(self, verbose=False):
        """
        Runs a shell command to extract data from a file.

        Args:
            verbose (bool, optional): Whether to display the qhist command.

        Returns:
            str: The result of the shell command.
        """
        command = self._create_command()
        if verbose:
            print ('>> ', command)
        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
        stdout, stderr = process.communicate()
        if stderr:
            error_msg = f"Error: {stderr.strip()}"
            print(error_msg)
            sys.exit(1)  # Exits the script with an error code
        else:
            return f"Result: {stdout.strip()}"
