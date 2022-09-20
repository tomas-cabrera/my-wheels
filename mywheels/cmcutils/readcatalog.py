"""! @brief This module concerns reading data from a catalog of CMC models."""

###############################################################################

import os
import numpy as np
import pandas as pd

import mywheels.cmcutils.readoutput as readoutput

###############################################################################


class CMCCatalog:
    """! The class for a catalog of CMC models. """

    def __init__(self, path, extension="02", extension_replacement="02"):
        """! Initialize the catalog. 

        @param  path                        Path to the catalog.
                                            Should be a path to a directory that contains a folder for each of the models.
        @param  extension                   File extension on names; will be stripped, but is expected at end of all names.
        @param  extension_replacement       What to replace the extensions with.
                                            Removes extension by default.
                                            Note that default of "02" here and for "extension" is a dirty way of keeping Kremer+20-like models.

        """

        # Store path
        self.path = path

        # Get list of folder names
        fnames = os.listdir(path)

        # Make catalog dataframe, starting with fnames
        fnames = pd.DataFrame([n for n in fnames if extension in n], columns=["fname"])

        # Prep and parse names
        params = [n.replace(extension, extension_replacement) for n in fnames.fname]
        params = pd.DataFrame([self._parse_model_name(n) for n in params])
        self.df = pd.concat([fnames, params], axis=1)

    def _parse_model_name(self, model_name, cat_type="Kremer+20"):
        """! Parse model names.  Currently only the base one is implemented.

        @param  model_name      Name of model
        @param  cat_type        Name format.
                                Default is "Kremer+20", i.e. the "N[_v2]_rv_rg_Z" convention from the respective catalog.

        @return model_params    Dictionary of model parameters.

        """

        # For names like the Kremer+20 catalog:
        if cat_type == "Kremer+20":
            # Strip "_v2"
            model_name = model_name.replace("_v2", "")

            # Split and parse
            model_params = dict(zip(["N", "rv", "rg", "Z"], model_name.split("_")))
            for k in model_params.keys():
                model_params[k] = float(model_params[k].replace(k, ""))
        else:
            raise Exception("cat_type '%s' not implemented." % cat_type)

        return model_params
