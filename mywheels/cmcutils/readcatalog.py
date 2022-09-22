"""! @brief This module concerns reading data from a catalog of CMC models."""

###############################################################################

import os
import numpy as np
import pandas as pd

import mywheels.cmcutils.readoutput as readoutput

###############################################################################


class CMCCatalog:
    """! The class for a catalog of CMC models. """

    def __init__(self, path, extension="02", mp_nprocs=1):
        """! Initialize the catalog. 

        @param  path            Path to the catalog.
                                Should be a path to a directory that contains a folder for each of the models.
        @param  extension       File extension on names; will be stripped, but is expected at end of all names.
                                Note that default of "02" here a dirty way of keeping Kremer+20-like models.
        @param  mp_nprocs       Number of processes to use for parallel tasks.

        """

        # Store path
        self.path = path

        # Get list of folder names
        fnames = os.listdir(path)

        # Make catalog dataframe, starting with fnames
        self.df = pd.DataFrame(index=[n for n in fnames if n.endswith(extension)])

    def parse_names(self, replace={"_v2": ""}, cat_type="Kremer+20"):
        """! Function to extract model parameters from names.

        @param  replace     Dictionary where the keys will be replaced with the values whenever they appear in the model name.
                            Default removes any "_v2"s in the model names.
        @param  cat_type        Name format.
                                Default is "Kremer+20", i.e. the "N[_v2]_rv_rg_Z" convention from the respective catalog.

        """

        # Replace strings
        fnames = self.df.index
        for item in replace.items():
            fnames = [n.replace(*item) for n in fnames]

        # Extract params
        params = [self._parse_model_name(n, cat_type=cat_type) for n in fnames]
        params = pd.DataFrame(params, index=self.df.index)

        # Join params to dataframe
        self.df = self.df.join(params)

    def _parse_model_name(self, model_name, cat_type="Kremer+20"):
        """! Parse model names.  Currently only the base one is implemented.

        @param  model_name      Name of model
        
        @return model_params    Dictionary of model parameters.

        """

        # For names like the Kremer+20 catalog:
        if cat_type == "Kremer+20":
            # Split and parse
            model_params = dict(zip(["N", "rv", "rg", "Z"], model_name.split("_")))
            for k in model_params.keys():
                if k == "N":
                    model_params[k] = int(float(model_params[k].replace(k, "")))
                else:
                    model_params[k] = float(model_params[k].replace(k, ""))
        else:
            raise Exception("cat_type '%s' not implemented." % cat_type)

        return model_params
