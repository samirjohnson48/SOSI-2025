"""
This file includes all functions used for collection of species landings 
from the FAO Fishstat database

These functions are implemented in ./main/fishstat_landings.py
"""

import pandas as pd
import numpy as np
import json


def format_fishstat(fishstat, code_to_scientific=[], year_start=1950, year_end=2022):
    fishstat = fishstat.drop(columns=["Unit"])

    rename_dict = {f"[{year}]": year for year in range(year_start, year_end + 1)}
    rename_dict["Country (ISO3 code)"] = "ISO3"
    rename_dict["ASFIS species (3-alpha code)"] = "Alpha3_Code"
    rename_dict["FAO major fishing area (Code)"] = "Area"
    rename_dict["Unit (Name)"] = "Unit"
    fishstat = fishstat.rename(columns=rename_dict)

    if code_to_scientific:
        fishstat["ASFIS Scientific Name"] = fishstat["Alpha3_Code"].map(
            code_to_scientific
        )

    return fishstat


def compute_species_landings(
    row, fishstat, area_map, year_start=1950, year_end=2021, key="ASFIS Scientific Name"
):
    scientific_name, area, loc = row[key], row["Area"], row["Location"]
    years = list(range(year_start, year_end + 1))

    if row["Area"] == "48,58,88":
        area_str = loc.split(".")[0]
        areas = [int(area_str)] if area_str.isdigit() else [48, 58, 88]
    elif row["Area"] == "Salmon":
        areas = [67]
    elif isinstance(area, int):
        areas = [area]
    else:
        # Handle the categorical areas separately
        areas = area_map[area][loc]

    area_mask = fishstat["Area"].isin(areas)

    # Create mask for scientific name
    # Handle species listed by commas
    fishstat_sn = fishstat["ASFIS Scientific Name"].unique()
    if ", " in scientific_name and scientific_name not in fishstat_sn:
        scientific_names = [
            sn for sn in scientific_name.split(", ") if sn in fishstat_sn
        ]
        sn_mask = fishstat["ASFIS Scientific Name"].isin(scientific_names)
    else:
        sn_mask = fishstat["ASFIS Scientific Name"] == scientific_name

    if sum(sn_mask) == 0:
        # If no matching scientific names, return missing values
        return pd.Series([np.nan] * len(years), index=years)

    # Return dictionary of Area to landings for sharks covering more than one FAO area
    if row["Area"] == "Sharks":
        if len(areas) == 1:
            area_mask = fishstat["Area"] == areas[0]
            return fishstat[area_mask & sn_mask][years].sum()
        else:
            cap_series = pd.Series(index=years, dtype=object)

            for year in years:
                cap_dict = {}
                for area in areas:
                    area_mask_shark = fishstat["Area"] == area
                    cap_dict[area] = fishstat[area_mask_shark & sn_mask][year].sum()
                cap_series[year] = json.dumps(cap_dict)

            return cap_series

    return fishstat[area_mask & sn_mask][years].sum()
