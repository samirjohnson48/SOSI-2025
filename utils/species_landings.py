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


def explode_stocks(species_landings, key="FAO Areas"):
    sl = species_landings.copy()

    sl[key] = sl[key].astype(str)

    sl["FAO Area"] = sl[key].apply(lambda x: x.split(", "))

    sl = sl.explode("FAO Area").reset_index(drop=True)

    sl["FAO Area"] = sl["FAO Area"].apply(
        lambda x: int(x) if x.isdigit() else print(f"{x} cannot be cast to type int")
    )

    sl = sl.drop(columns=key)

    return sl


def compute_species_landings(
    row,
    fishstat,
    mult_sns=[],
    year_start=1950,
    year_end=2021,
    key="ASFIS Scientific Name",
):
    scientific_name, area = row[key], row["FAO Area"]
    years = list(range(year_start, year_end + 1))

    area_mask = fishstat["Area"] == area

    # Create mask for scientific name
    # Handle species listed by commas
    fishstat_sn = fishstat["ASFIS Scientific Name"].unique()
    if ", " in scientific_name and scientific_name not in fishstat_sn:
        scientific_names = [
            sn for sn in scientific_name.split(", ") if sn in fishstat_sn
        ]
        sn_mask = fishstat["ASFIS Scientific Name"].isin(scientific_names)
    elif scientific_name in mult_sns:
        sn_mask = fishstat["Alpha3_Code"] == row["Alpha3_Code"]
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

        sl_area_mask = sl["FAO Area"] == area
        sl_stocks_mask = sl["ASFIS Scientific Name"].isin(stocks)

        fs_area_mask = fishstat["Area"] == area
        fs_stocks_mask = fishstat["ASFIS Scientific Name"].isin(sub_stocks)

        landings = fishstat[fs_area_mask & fs_stocks_mask][years].sum() / n_stocks

        sl.loc[sl_area_mask & sl_stocks_mask, years] = landings.values

    return sl


def add_species_landings(species_landings, fishstat, spl, years):
    sl = species_landings.copy()

    for species_to_add, species_info in spl.items():
        # Area is the area of the species
        # species is the list of species where the extra landings are added
        # distribution is the weights over the list of species for the extra landings
        if len(species_info) == 3:
            area, species, distribution = species_info
        elif len(species_info) == 2:
            # If no distribution provided, use uniform distribution
            area, species = species_info

            # Make sure species is list if single species is given
            if not isinstance(species, list):
                species = [species]

            n = len(species)
            distribution = list(np.ones(n) / n)

        # Normalize distribution if not done already
        s = sum(distribution)
        if s != 1:
            distribution = [d / s for d in distribution]

        species_mask = fishstat["ASFIS Scientific Name"] == species_to_add
        area_mask = fishstat["Area"] == area

        landings_to_add = fishstat[species_mask & area_mask][years].sum()

        for sp, w in zip(species, distribution):
            lta = landings_to_add * w

            sl_species_mask = sl["ASFIS Scientific Name"] == sp
            sl_area_mask = sl["FAO Area"] == area

            sl.loc[sl_species_mask & sl_area_mask, years] += lta

    return sl


def compute_missing_landings(
    species_landings,
    stock_assessments,
    fishstat,
    analysis_groups,
    nei_to_isscaap,
    year=2021,
    key=2021,
    pk=["FAO Area", "ASFIS Scientific Name"],
):
    sa = explode_stocks(stock_assessments)
    sl = pd.merge(species_landings, sa, on=pk)

    for ag in analysis_groups:
        # Get stock landings for the analysis group
        ag_mask = sl["Analysis Group"] == ag
        df = sl[ag_mask].reset_index(drop=True)

        areas = get_numbers_from_string(ag)
        cap = fishstat[fishstat["Area"].isin(areas)]

        # Check if there are any stocks with no landings in area
        no_l_mask = (df[key] == 0) | (df[key].isna())
        if sum(no_l_mask) == 0:
            continue

        has_l_tot = df[~no_l_mask]

        for nei, isscaaps in nei_to_isscaap.items():
            isscaap_mask = df["ISSCAAP Code"].isin(isscaaps)

            if sum(no_l_mask & isscaap_mask) == 0:
                continue

            # Define stocks to reassign landings to
            reassign_mask = no_l_mask | (df["ASFIS Name"] == nei)

            if "Proxy Name" in df.columns:
                reassign_mask = reassign_mask | (df["Proxy Name"] == nei)

            # If the NEI Species is already reported in the Area
            # Use the landings to redistribute
            if nei in has_l_tot["ASFIS Name"].unique():
                factor = 1
            else:  # Otherwise, use portion of NEI landings
                n_no_l = sum(no_l_mask & isscaap_mask)
                n_has_l = max(sum(~no_l_mask & isscaap_mask), 1)

                factor = min(n_no_l / n_has_l, 1)

            if nei == "Marine fishes NEI":
                print(f"{ag} factor: {factor}")

            # Get total landings of NEI species in area for 2021
            nei_l = cap[(cap["ASFIS Name"] == nei)][year].sum()

            no_l = df[reassign_mask & isscaap_mask]
            has_l = df[~reassign_mask & isscaap_mask]

            # Calculate the weights of U, M, O stocks based on proportion for stocks with landings
            has_l_counts = has_l["Status"].value_counts()
            u_t, m_t, o_t = (
                has_l_counts.get("U", 0),
                has_l_counts.get("M", 0),
                has_l_counts.get("O", 0),
            )

            # Get value counts across status for stocks with no landings
            no_l_counts = no_l["Status"].value_counts()
            u, m, o = (
                no_l_counts.get("U", 0),
                no_l_counts.get("M", 0),
                no_l_counts.get("O", 0),
            )

            # If distribution gives zero for any status but there are members of that status without landings,
            # increase count by one across statuses
            if (
                any(x == 0 and y > 0 for x, y in zip([u_t, m_t, o_t], [u, m, o]))
                or sum([u_t, m_t, o_t]) == 0
            ):
                u_t, m_t, o_t = u_t + 1, m_t + 1, o_t + 1

            t_arr = np.array([u_t, m_t, o_t])
            u_w, m_w, o_w = t_arr / np.sum(t_arr)

            # Assign total landings for categories U, M, O for stocks with no landings
            u_l, m_l, o_l = u_w * nei_l, m_w * nei_l, o_w * nei_l

            # Assign stock landings for stocks with no landings
            # (or reassign landings for NEI species so not to double count)
            u_mask, m_mask, o_mask = (
                sl["Status"] == "U",
                sl["Status"] == "M",
                sl["Status"] == "O",
            )
            sl_no_l_mask = (sl[key] == 0) | (sl[key].isna())
            nei_mask = sl["ASFIS Name"] == nei
            if "Proxy Name" in sl.columns:
                nei_mask = nei_mask | (sl["Proxy Name"] == nei)
            sl_isscaap_mask = sl["ISSCAAP Code"].isin(isscaaps)

            base_mask = ag_mask & ((sl_no_l_mask & sl_isscaap_mask) | nei_mask)

            if u > 0:
                sl.loc[
                    base_mask & u_mask,
                    key,
                ] = (
                    u_l / u * factor
                )
                sl.loc[
                    base_mask & u_mask,
                    "Proxy Species",
                ] = nei
            if m > 0:
                sl.loc[
                    base_mask & m_mask,
                    key,
                ] = (
                    m_l / m * factor
                )
                sl.loc[
                    base_mask & m_mask,
                    "Proxy Species",
                ] = nei
            if o > 0:
                sl.loc[
                    base_mask & o_mask,
                    key,
                ] = (
                    o_l / o * factor
                )
                sl.loc[
                    base_mask & o_mask,
                    "Proxy Species",
                ] = nei

    return sl
