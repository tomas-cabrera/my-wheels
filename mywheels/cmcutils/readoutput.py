"""! @brief This module loads the standard CMC output files into a pandas DataFrame."""

###############################################################################

import numpy as np
import pandas as pd

###############################################################################

class _output_file:
    """! The parent class for the individual files. """
    def __init__(self, fname, header_line_no=0, colon_aliases=[]):
        """! Read the data.

        @param  fname           The path to the output file.
        @param  header_line_no  The line to read the columns from.
        @param  colon_aliases   List-like of character to replace with colons (e.g. ["."] for dyn.dat files)

        """

        # Get the column names
        columns = self._get_column_names(fname, header_line_no, colon_aliases)
        print(columns)

        # TODO: Add functionality to select columns

        # Load data
        self.df = pd.read_csv(
            fname,
            names=columns,
            delimiter=" ",
            skiprows=header_line_no+1,
        )

    def _get_column_names(self, fname, header_line_no, colon_aliases=[]):
        """! Parses the specified line in the file to get the header names.

        @return columns     The column names.

        """

        # Read the header line
        with open(fname) as file:
            for li, l in enumerate(file):
                if li == header_line_no:
                    columns = l
                    break

        # Split at whitespace
        columns = columns.split()

        # Keep everything after the colon (using aliases if needed)
        for ca in colon_aliases:
            columns = [c.replace(ca, ":") for c in columns]
        columns = [c.split(":")[1] for c in columns]

        return columns
