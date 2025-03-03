"""

"""

import pandas as pd
import numpy as np


def compute_num_stocks(stock_landings, group_key=["Area", "ASFIS Scientific Name"]):
    sl = stock_landings.copy()
    sl["Num Stocks"] = sl.groupby(group_key)[group_key[0]].transform("count")

    return sl


def compute_landings(
    row, species_landings="Species Landings 2021", weight="Normalized Weight"
):
    if row["Area"] == "Tuna":
        return row[species_landings]
    elif row["Num Stocks"] == 1 and not pd.isna(row[species_landings]):
        return row[species_landings]
    elif not pd.isna(row[species_landings]) and not pd.isna(row[weight]):
        return row[species_landings] * row[weight]
    # elif np.isnan(row[species_landings]) and not pd.isna(row["Weight 1"]):
    #     return row["Weight 1"]
    return np.nan


def use_proxy_landings(
    stock_landings,
    proxy_landings,
    primary_key=["Area", "ASFIS Scientific Name", "Location"],
    landings_key="Stock Landings 2021",
    proxy_landings_key="Proxy Species Landings",
    proxy_species_key="Proxy Species",
):
    merge = pd.merge(stock_landings, proxy_landings, on=primary_key, how="left")

    no_landings_mask = (merge[landings_key].isna()) | (merge[landings_key] == 0)
    has_proxy_mask = merge[proxy_landings_key].notna()

    combined_mask = no_landings_mask & has_proxy_mask

    if combined_mask.any():
        merge.loc[combined_mask, landings_key] = merge.loc[
            combined_mask, proxy_landings_key
        ].astype(float)

    merge["Proxy Species"] = np.where(combined_mask, merge[proxy_species_key], np.nan)

    return merge


def compute_missing_landings(
    stock_landings, fishstat, areas, isscaap_to_nei, year=2021
):
    sl = stock_landings.copy()

    for area in areas:
        # Get stock landings for the area
        area_mask = sl["Area"] == area
        df = sl[area_mask]
        cap = fishstat[fishstat["Area"] == area]

        # Check if there are any stocks with no landings in area
        no_landings_mask = (df["Stock Landings 2021"] == 0) | (
            df["Stock Landings 2021"].isna()
        )
        if sum(no_landings_mask) == 0:
            continue

        # Get the list of NEI species whose landings will be used
        proxy_species = (
            df[no_landings_mask]["ISSCAAP Code"].map(isscaap_to_nei).unique()
        )
        proxy_species = proxy_species[pd.notna(proxy_species)]

        # Don't include seals since they are taken out in calculations
        seals_mask = df["ISSCAAP Code"] == 63
        no_l = df[
            (no_landings_mask | (df["ASFIS Name"].isin(proxy_species))) & ~seals_mask
        ]

        # Get total landings of Marine Fishes NEI in area for 2021
        nei = cap[(cap["ASFIS Name"].isin(proxy_species))][year].sum()

        # Calculate the weights of U, M, O stocks based on proportion for stocks with landings
        has_l = df[df["Stock Landings 2021"] > 0]
        weights = has_l["Status"].value_counts(normalize=True)
        u_w, m_w, o_w = weights.get("U", 0), weights.get("M", 0), weights.get("O", 0)

        # Assign total landings for categories U, M, O for stocks with no landings
        u_l, m_l, o_l = u_w * nei, m_w * nei, o_w * nei

        # Get value counts across status for stocks with no landings
        no_l_counts = no_l["Status"].value_counts()
        u, m, o = (
            no_l_counts.get("U", 0),
            no_l_counts.get("M", 0),
            no_l_counts.get("O", 0),
        )

        # Assign stock landings for stocks with no landings
        # (or reassign landings for Marine Fishes NEI so not to double count)
        u_mask, m_mask, o_mask = (
            sl["Status"] == "U",
            sl["Status"] == "M",
            sl["Status"] == "O",
        )
        no_l_mask = (sl["Stock Landings 2021"] == 0) | (
            sl["Stock Landings 2021"].isna()
        )
        nei_mask = sl["ASFIS Name"].isin(proxy_species)
        no_seals_mask = ~(sl["ISSCAAP Code"] == 63)

        if u > 0:
            sl.loc[
                (area_mask) & (u_mask) & (no_l_mask | nei_mask) & (no_seals_mask),
                "Stock Landings 2021",
            ] = (
                u_l / u
            )
            sl.loc[
                (area_mask) & (u_mask) & (no_l_mask | nei_mask) & (no_seals_mask),
                "Proxy Species",
            ] = "NEI"
        if m > 0:
            sl.loc[
                (area_mask) & (m_mask) & (no_l_mask | nei_mask) & (no_seals_mask),
                "Stock Landings 2021",
            ] = (
                m_l / m
            )
            sl.loc[
                (area_mask) & (m_mask) & (no_l_mask | nei_mask) & (no_seals_mask),
                "Proxy Species",
            ] = "NEI"
        if o > 0:
            sl.loc[
                (area_mask) & (o_mask) & (no_l_mask | nei_mask) & (no_seals_mask),
                "Stock Landings 2021",
            ] = (
                o_l / o
            )
            sl.loc[
                (area_mask) & (o_mask) & (no_l_mask | nei_mask) & (no_seals_mask),
                "Proxy Species",
            ] = "NEI"

    return sl
