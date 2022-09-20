"""! @brief This module loads the standard CMC output files into a pandas DataFrame."""

###############################################################################

import inspect
import numpy as np
import pandas as pd

###############################################################################


class _output_file:
    """! The parent class for the individual files. """

    def __init__(
        self, fname, header_line_no=0, colon_aliases=[], pd_kwargs={},
    ):
        """! Read the data.

        @param  fname           The path to the output file.
        @param  header_line_no  The line to read the columns from.
        @param  colon_aliases   List-like of character to replace with colons (e.g. ["."] for dyn.dat files)
        @param  pd_kwargs       Dictionary of keyword arguments to pass to pd.read_csv when loading data.
                                Merges the specified dict with the default to preserve defaults.

        """

        # Get the column names
        names = self._get_column_names(fname, header_line_no, colon_aliases)

        # Merge specified pd_kwargs dist with default
        pd_kwarg_defaults = {
            "names": names,
            "delimiter": " ",
            "skiprows": header_line_no + 1,
        }
        pd_kwargs = {**pd_kwarg_defaults, **pd_kwargs}

        # Load data, passing usecols to select particular columns
        self.df = pd.read_csv(fname, **pd_kwargs,)

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
