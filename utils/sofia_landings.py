"""
This file includes all functions used for calculating the landings for stocks in SOFIA data

These functions are implemented in ./main/sofia_landings.py
"""

import pandas as pd
import numpy as np
import os

def convert_status_to_list(status):
    if not isinstance(status, str) and np.isnan(status):
        return [status]

    separators = [",", "/", "-"]

    sep = next((sep for sep in separators if sep in status), None)

    if status == "OF":
        return ["O", "F"]

    if sep:
        status_list = [s.strip()[0] for s in status.split(sep)]
        return status_list
    elif isinstance(status, str):
        return [status.strip()]

    return [status]

def get_proxy_name(sn, scientific_names):
    if pd.isna(sn):
        return sn
    if sn in scientific_names:
        return sn
    elif "sp." in sn:
        return sn.replace("sp.", "spp")
    elif "Species" in sn and sn.replace("Species", "spp") in scientific_names:
        return sn.replace("Species", "spp")

    if sn.split(" ")[0] + " spp" in scientific_names:
        return sn.split(" ")[0] + " spp"

    return np.nan

def normalize_landings(sofia, years, key=["Area", "ASFIS Scientific Name"]):
    sofia["n"] = sofia.groupby(key)[key[0]].transform("count")

    sofia[years] = sofia[years].div(sofia["n"], axis=0)
    
    sofia = sofia.drop(columns="n")
    
    return sofia
    