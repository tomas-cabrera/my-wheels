"""! @brief A module to combine data from several catalogs """

###############################################################################

import numpy as np
import pandas as pd

###############################################################################


class GCCatalog:
    def __init__(self, baumgardt_path, harris_path):
        """! Basic things for now."""

        # Load cleaned Baumgardt data
        df_baumgardt = pd.read_csv(
            baumgardt_path,
            usecols=["Cluster", "Mass", "rc", "rh,m", "R_GC"],
            delim_whitespace=True,
        )

        # Load cleaned Harris data (for metallicities)
        # "cleaned" = ID renamed to Cluster, names edited to match Baumgardt & Holger, Z=-100 if there's no metallicity measurement (these clusters are also the ones with wt=0 i.e. no stellar metallicities have been measured)
        df_harris = pd.read_csv(
            harris_path, usecols=["Cluster", "[Fe/H]"], delim_whitespace=True
        )
        # drop rows with [Fe/H]=-100
        df_harris = df_harris[df_harris["[Fe/H]"] != -100.0]

        # Merge the two datasets
        self.df = pd.merge(df_baumgardt, df_harris, how="inner", on="Cluster")

        # Set cluster name as index
        self.df.set_index("Cluster", inplace=True)

    def match_to_cmc_models(
        self, cmc_path, cmc_kwargs={}, dyn_kwargs={},
    ):
        """! Given a path to a CMC directory, finds the matching CMC models for the GCs."""

        # Save cmc_path
        self.cmc_path = cmc_path

        # Load cmc catalog, and dyn.dat data
        import mywheels.cmcutils.readcatalog as cmccat

        cmc_models = cmccat.CMCCatalog(cmc_path, mp_nprocs=4, **cmc_kwargs)
        cmc_models.df = cmc_models.df.iloc[:8]
        cmc_models.add_dat_timesteps(
            "initial.dyn.dat",
            tnum=3,
            dat_kwargs={
                "pd_kwargs": {"usecols": ["M", "rc_spitzer", "r_h"]},
                "convert_units": {"M": "msun", "rc_spitzer": "pc", "r_h": "pc"},
            },
            **dyn_kwargs,
        )
        cmc_models.parse_names()

        # Calculate columns to match
        self.df["logM"] = np.log10(self.df["Mass"])
        cmc_models.df["logM"] = np.log10(cmc_models.df["M"])
        self.df["rc/rh"] = self.df["rc"] / self.df["rh,m"]
        cmc_models.df["rc/rh"] = cmc_models.df["rc_spitzer"] / cmc_models.df["r_h"]

        # TODO: Can split MW GCs into Rg x Z blocks (3 x 3 = 9 blocks), and can process each block wholesale
        #       Probably just hardcode bins (e.g. [Fe/H]: <-1.5, -1.5 \le x < -0.5, and -0.5 \le x)
        # Also can calculate distances wholesale, and then filter by Rg x Z (but ~9x the computation)
