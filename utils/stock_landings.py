"""

"""

import pandas as pd
import numpy as np
import json
import re


def compute_num_stocks(stock_landings, group_key=["FAO Area", "ASFIS Scientific Name"]):
    sl = stock_landings.copy()
    sl["Num Stocks"] = sl.groupby(group_key)[group_key[0]].transform("count")

    return sl


def compute_landings(
    row, species_landings="Species Landings 2021", weight="Normalized Weight"
):
    if pd.notna(row[species_landings]) and pd.notna(row[weight]):
        return row[species_landings] * row[weight]
    elif row["Num Stocks"] == 1 and pd.notna(row[species_landings]):
        return row[species_landings]

    return np.nan


def use_proxy_landings(
    stock_landings,
    proxy_landings,
    primary_key=["ASFIS Scientific Name", "Location"],
    landings_key="Stock Landings 2021",
    proxy_landings_key="Proxy Stock Landings",
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


def get_numbers_from_string(text):
    numbers_str = re.findall(r"\d+", text)
    numbers_int = [int(num) for num in numbers_str]
    return numbers_int


def compute_missing_species_landings(
    species_map,
    species_landings,
    mlr=0.25,
    nei_factor=4,
):
    sp_map = species_map.copy()

    factor_dict = {}

    spl_grouped = species_landings.groupby("FAO Area")[2021]

    for name, group in spl_grouped:
        no_l_mask = group.isna() | (group == 0)

        factor = nei_factor * len(group) / sum(no_l_mask)

        factor_dict[name] = factor

    mzz_mask = sp_map["ASFIS Class or Phylum"] == "MZZ"

    sp_map.loc[mzz_mask, "Factor Class or Phylum"] = sp_map.loc[
        mzz_mask, "FAO Area"
    ].map(factor_dict)

    spl = pd.merge(
        species_landings,
        sp_map,
        on=["FAO Area", "ASFIS Scientific Name"],
        how="left",
        indicator=True,
    )

    # Make sure not to double count landings for missing species if proxy is already reported in area
    for level in ["genus", "family", "order", "Class or Phylum"]:
        method = f"ASFIS {level}"

        og_mask = spl["_merge"] == "left_only"

        pairs1 = set(zip(spl[og_mask]["Alpha3_Code"], spl[og_mask]["FAO Area"]))
        pairs2 = set(zip(spl[~og_mask][method], spl[~og_mask]["FAO Area"]))

        matches = pairs1.intersection(pairs2)
        matches = [m for m in matches if pd.notna(m[0])]

        mask1 = spl.apply(
            lambda row: (row[method], row["FAO Area"]) in matches,
            axis=1,
        )  # Rows from method which are reported in area

        mask2 = spl.apply(
            lambda row: (row["Alpha3_Code"], row["FAO Area"]) in matches,
            axis=1,
        )  # Rows reported in area which are in method

        landings_col1 = f"ASFIS {level} Landings"
        landings_col2 = 2021

        spl.loc[mask1, landings_col1] *= mlr
        spl.loc[mask2, landings_col2] *= 1 - mlr

        miss_mask = spl["_merge"] == "both"

    spl[2021] = spl[2021].fillna(0)

    for level in ["genus", "family", "order", "Class or Phylum"]:
        landings_col = f"ASFIS {level} Landings"
        factor_col = f"Factor {level}"

        spl.loc[miss_mask, 2021] += spl.loc[miss_mask, landings_col] / spl.loc[
            miss_mask, factor_col
        ].fillna(1)

    cols_to_keep = ["FAO Area", "ASFIS Scientific Name", 2021]

    spl = spl[cols_to_keep]

    return spl


def compute_missing_landings_v1(
    stock_landings,
    fishstat,
    analysis_groups,
    nei_to_isscaap,
    year=2021,
    key="Stock Landings 2021",
):
    sl = stock_landings.copy()

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

            # If NEI species is reported in area, make sure we use all of the landings
            # This means, if any of u, m, o == 0, the weights should be renormalized
            if nei in has_l_tot["ASFIS Name"].unique():
                u_w, m_w, o_w = [
                    (
                        w
                        / sum(w_ for w_, s in zip([u_w, m_w, o_w], [u, m, o]) if s > 0)
                        if s > 0
                        else 0
                    )
                    for w, s in zip([u_w, m_w, o_w], [u, m, o])
                ]

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


def compute_missing_landings_v2(
    species_map,
    method,
    fishstat,
    stock_assessments,
    species_landings,
    weights,
    default_landings,
    missing_landings_ratio=1 / 3,
):
    fs = (
        fishstat.groupby(["Area", "Alpha3_Code"])[2021]
        .sum()
        .reset_index()
        .rename(columns={"Area": "FAO Area"})
    )

    species_map = species_map.rename(
        columns={
            "Area": "FAO Area",
            "ASFIS species (Scientific name)": "ASFIS Scientific Name",
        }
    )[["FAO Area", "ASFIS Scientific Name", method]]

    cols = list(species_map.columns)

    col = "Species Landings 2021"

    species_map = species_map.rename(columns={method: "Alpha3_Code"})

    species_map = pd.merge(species_map, fs, on=["FAO Area", "Alpha3_Code"], how="left")

    species_map = species_map.rename(columns={"Alpha3_Code": method, 2021: col})

    # Normalize the landings across the number of same code in area
    species_map["N"] = species_map.groupby(["FAO Area", method])["FAO Area"].transform(
        "size"
    )

    species_map[col] /= species_map["N"]

    species_map = species_map.drop(columns="N")

    cols.append(col)

    species_map = species_map[cols]

    # Use the default landings to fill in the rest which are not mapped
    # Default landings computed from v1 of compute missing landings

    sl_grouped = (
        default_landings.groupby(["FAO Area", "ASFIS Scientific Name"])[
            "Stock Landings 2021"
        ]
        .sum()
        .reset_index()
    ).rename(columns={"Stock Landings 2021": "Default Landings"})

    species_map = pd.merge(
        species_map, sl_grouped, on=["FAO Area", "ASFIS Scientific Name"], how="left"
    )

    species_map = species_map[cols + ["Default Landings"]]

    col_fill = "Default Landings"
    no_l_mask = species_map[col].isna() | (species_map[col] == 0)
    species_map.loc[no_l_mask, col] = species_map.loc[no_l_mask, col_fill]

    spl_mod = pd.merge(
        species_landings,
        species_map,
        on=["FAO Area", "ASFIS Scientific Name"],
        how="left",
    )

    # Combine the missing landings with the mapped landings
    # If proxy species in method appears in assessment in same area
    # we must split the landings accordingly so not to double count

    col_fill = 2021

    og_mask = spl_mod[col].isna()
    spl_mod["Flag"] = og_mask

    pairs1 = set(zip(spl_mod[og_mask]["Alpha3_Code"], spl_mod[og_mask]["FAO Area"]))
    pairs2 = set(zip(spl_mod[~og_mask][method], spl_mod[~og_mask]["FAO Area"]))

    matches = pairs1.intersection(pairs2)
    matches = [m for m in matches if pd.notna(m[0])]

    mask1 = spl_mod.apply(
        lambda row: (row[method], row["FAO Area"]) in matches,
        axis=1,
    )  # Rows from method which are reported in area

    mask2 = spl_mod.apply(
        lambda row: (row["Alpha3_Code"], row["FAO Area"]) in matches,
        axis=1,
    )  # Rows reported in area which are in method

    spl_mod.loc[:, col] = spl_mod[col].fillna(spl_mod[col_fill])

    spl_mod.loc[mask1, col] *= missing_landings_ratio
    spl_mod.loc[mask2, col] *= 1 - missing_landings_ratio

    cols_to_keep = ["FAO Area", "Alpha3_Code", "ASFIS Scientific Name", col]

    sl_mod = pd.merge(
        spl_mod,
        weights,
        on=["FAO Area", "ASFIS Scientific Name"],
    )

    sl_mod = pd.merge(
        sl_mod,
        stock_assessments,
        on=["ASFIS Scientific Name", "Location"],
        suffixes=("", "_x"),
    )

    sl_mod = compute_num_stocks(sl_mod)

    stock_col = "Stock Landings 2021"

    sl_mod[stock_col] = sl_mod.apply(compute_landings, args=(col,), axis=1)

    cols_to_keep = ["FAO Area", "ASFIS Scientific Name", "Location", stock_col]

    sl_mod = sl_mod[cols_to_keep]

    return sl_mod
