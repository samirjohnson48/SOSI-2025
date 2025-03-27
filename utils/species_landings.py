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


def expand_sg_stocks(species_landings, special_groups, location_to_area):
    sl = species_landings.copy()

    def retrieve_areas(row, lta=location_to_area):
        area = row["Area"]

        if area not in special_groups:
            return [area]

        loc = row["Location"]

        try:
            areas = lta[area].get(loc, [])

            if not areas:
                print(
                    f"Location {loc} not found in location_to_area map under Area {area}."
                )

            return areas
        except KeyError:
            msg = f"Special group {area} not found in location_to_area map"

            raise KeyError(msg)

    sl["FAO Area"] = sl[["Area", "Location"]].apply(retrieve_areas, axis=1)

    sl = sl.explode("FAO Area").reset_index(drop=True)

    return sl


def compute_species_landings(
    row, fishstat, area_map, year_start=1950, year_end=2021, key="ASFIS Scientific Name"
):
    scientific_name, area = row[key], row["FAO Area"]
    years = list(range(year_start, year_end + 1))

    # area_mask = fishstat["Area"].isin(areas)
    area_mask = fishstat["Area"] == area

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

    # If no matching scientific names, return missing values
    if sum(sn_mask) == 0:
        return pd.Series([np.nan] * len(years), index=years)

    return fishstat[area_mask & sn_mask][years].sum()


def substitute_landings(species_landings, fishstat, subs, years):
    sl = species_landings.copy()

    for sub in subs:
        area = sub[0]
        stocks = sub[1]
        sub_stocks = sub[2]
        n_stocks = len(stocks)

        sl_area_mask = sl["Area"] == area
        sl_stocks_mask = sl["ASFIS Scientific Name"].isin(stocks)

        fs_area_mask = fishstat["Area"] == area
        fs_stocks_mask = fishstat["ASFIS Scientific Name"].isin(sub_stocks)

        landings = fishstat[fs_area_mask & fs_stocks_mask][years].sum() / n_stocks

        sl.loc[sl_area_mask & sl_stocks_mask, years] = landings.values

    return sl
