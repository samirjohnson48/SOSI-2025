"""

"""

import pandas as pd
import numpy as np
import json
from tqdm import tqdm
from openpyxl import load_workbook
from functools import reduce

from utils.species_landings import compute_species_landings


def round_excel_file(filename, decimal_places=2, lt_one=False):
    try:
        book = load_workbook(filename)
    except FileNotFoundError:
        print(f"Error: File '{filename}' not found.")
        return

    for sheet_name in book.sheetnames:
        sheet = book[sheet_name]

        for col in sheet.columns:
            for i, cell in enumerate(col):
                if i == 0:
                    continue
                if cell.data_type == "n":
                    if isinstance(cell.value, int):
                        cell.number_format = "#,##0"
                    else:
                        if (
                            lt_one
                            and isinstance(cell.value, float)
                            and 0 < cell.value < 1
                        ):
                            cell.number_format = "< 1"
                        elif decimal_places == 0:
                            cell.number_format = "#,##0"
                        elif isinstance(cell.value, float) and 0 < cell.value < 1:
                            val_str = str(cell.value)
                            zeros = (
                                len(val_str.split(".")[1])
                                - len(val_str.split(".")[1].lstrip("0"))
                                if "." in val_str
                                else 0
                            )
                            cell.number_format = "#,##0." + "0" * (
                                zeros + decimal_places
                            )
                        else:
                            cell.number_format = "#,##0." + "0" * decimal_places

    book.save(filename)


def add_footnote(df, footnote_text, multi_index=False):
    if multi_index:
        levels = len(df.columns[0])
        footnote_df = pd.DataFrame(
            {tuple("" for _ in range(levels)): [footnote_text]}, index=["Footnote"]
        )
    else:
        footnote_df = pd.DataFrame({"": [footnote_text]}, index=["Footnote"])

    df_with_footnote = pd.concat([df, footnote_df])

    return df_with_footnote


def compute_count_for_group(df, group_col="Area", count_col="Tier"):
    counts = df.groupby(group_col)[count_col].value_counts().unstack(fill_value=0)

    total = counts.sum(numeric_only=True)

    counts.loc["Global"] = total

    rename_cols = {col: f"{count_col} {col}" for col in counts.columns}

    counts = counts.rename(columns=rename_cols)

    counts["Total"] = counts.sum(axis=1)

    return counts


def compute_status_by_number(data, group):
    grouped = (
        data.groupby(group)
        .agg(
            **{
                "No. of stocks": ("Status", "size"),
                "No. of U": ("Status", lambda x: (x == "U").sum()),
                "No. of MSF": ("Status", lambda x: (x == "M").sum()),
                "No. of O": ("Status", lambda x: (x == "O").sum()),
                "No. of Sustainable": (
                    "Status",
                    lambda x: ((x == "U") | (x == "M")).sum(),
                ),
                "No. of Unsustainable": ("Status", lambda x: (x == "O").sum()),
                "U (%)": ("Status", lambda x: (x == "U").mean() * 100),
                "MSF (%)": ("Status", lambda x: (x == "M").mean() * 100),
                "O (%)": ("Status", lambda x: (x == "O").mean() * 100),
                "Sustainable (%)": (
                    "Status",
                    lambda x: ((x == "M") | (x == "U")).mean() * 100,
                ),
                "Unsustainable (%)": ("Status", lambda x: (x == "O").mean() * 100),
            }
        )
        .reset_index()
    )

    # Set percentages to NaN when denominator (No. of stocks) is 0
    for col in ["U (%)", "MSF (%)", "O (%)", "Sustainable (%)", "Unsustainable (%)"]:
        grouped[col] = grouped.apply(
            lambda row: np.nan if row["No. of stocks"] == 0 else row[col], axis=1
        )

    # Add a final row with total values
    total_stocks = data.shape[0]
    totals = pd.DataFrame(
        {
            group: ["Global"],
            "No. of stocks": [total_stocks],
            "No. of U": [(data["Status"] == "U").sum()],
            "No. of MSF": [(data["Status"] == "M").sum()],
            "No. of O": [(data["Status"] == "O").sum()],
            "No. of Sustainable": [data["Status"].isin(["U", "M"]).sum()],
            "No. of Unsustainable": [(data["Status"] == "O").sum()],
            "U (%)": [
                (
                    np.nan
                    if total_stocks == 0
                    else ((data["Status"] == "U").sum() / total_stocks) * 100
                )
            ],
            "MSF (%)": [
                (
                    np.nan
                    if total_stocks == 0
                    else ((data["Status"] == "M").sum() / total_stocks) * 100
                )
            ],
            "O (%)": [
                (
                    np.nan
                    if total_stocks == 0
                    else ((data["Status"] == "O").sum() / total_stocks) * 100
                )
            ],
            "Sustainable (%)": [
                (
                    np.nan
                    if total_stocks == 0
                    else (data["Status"].isin(["U", "M"]).sum() / total_stocks) * 100
                )
            ],
            "Unsustainable (%)": [
                (
                    np.nan
                    if total_stocks == 0
                    else ((data["Status"] == "O").sum() / total_stocks) * 100
                )
            ],
        }
    )

    return pd.concat([grouped, totals], ignore_index=True)


def compare_status_by_number(update, previous):
    comparison = pd.merge(
        update, previous, on="Area", how="left", suffixes=("_update", "_previous")
    )

    new_columns = []

    for col in comparison.columns:
        if col == "Area":
            new_columns.append(("", col))  # Keeping Area as a separate category
        elif col.endswith("_previous"):
            new_columns.append(
                ("Previous SoSI Categories", col.replace("_previous", ""))
            )
        elif col.endswith("_update"):
            new_columns.append(("Updated SoSI Categories", col.replace("_update", "")))

    comparison.columns = pd.MultiIndex.from_tuples(new_columns)

    return comparison


def compute_summary_of_stocks(data, group="Tier"):
    if group == "Tier":
        data = data[data["Tier"].isin([1, 2, 3])]

    assessed_data_mask = data["Status"].isin(["U", "M", "O"])
    numeric_isscaap_mask = data["ISSCAAP Code"].apply(
        lambda x: isinstance(x, (int, float))
    )

    summary = (
        data.groupby(group)
        .agg(
            {
                group: "size",
                "Status": lambda x: x.isin(["U", "M", "O"]).sum(),
                "ASFIS Scientific Name": lambda x: x[assessed_data_mask].nunique(),
                "ISSCAAP Code": lambda x: x[
                    assessed_data_mask & numeric_isscaap_mask
                ].nunique(),
            }
        )
        .rename(
            columns={
                group: "Total stocks",
                "Status": "Total assessed stocks",
                "ASFIS Scientific Name": "Total ASFIS species (from total assessed stocks)",
                "ISSCAAP Code": "Total ISSCAAP Groups (from total assessed stocks)",
            }
        )
    )

    summary.loc["Global"] = summary.sum()
    summary.loc["Global", "Total ASFIS species (from total assessed stocks)"] = (
        data.loc[assessed_data_mask, "ASFIS Scientific Name"].nunique()
    )
    summary.loc["Global", "Total ISSCAAP Groups (from total assessed stocks)"] = (
        data.loc[assessed_data_mask & numeric_isscaap_mask, "ISSCAAP Code"].nunique()
    )

    return summary


def compute_summary_area_by_tier(data):
    tier_summaries = {}

    for tier in [1, 2, 3, "Total"]:
        tier_mask = (
            data["Tier"] == tier
            if tier != "Total"
            else pd.Series(True, index=data.index)
        )

        df = data[tier_mask].copy()

        sos = compute_summary_of_stocks(df, group="Area")

        rename_dict = {
            "Total assessed stocks": "No. of stocks",
            "Total ASFIS species (from total assessed stocks)": "No. of ASFIS species",
            "Total ISSCAAP Groups (from total assessed stocks)": "No. of ISSCAAP groups",
        }

        sos = sos.rename(columns=rename_dict).drop(columns=["Total stocks"])

        sbn = (
            compute_status_by_number(df, "Area")
            .set_index("Area")
            .drop(columns="No. of stocks")
        )

        comb = pd.merge(sos, sbn, left_index=True, right_index=True)

        key = f"Tier {tier}" if tier != "Total" else "Total"
        tier_summaries[key] = comb

    return tier_summaries


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


def compute_species_status_by_number(data, species_list, fishstat):
    data = data[data["ASFIS Scientific Name"].isin(species_list)]
    group = (
        data.groupby(["ASFIS Scientific Name", "Status"]).size().unstack(fill_value=0)
    )
    global_totals = group.sum(axis=0)
    global_totals.name = "Global"
    group = pd.concat([group, global_totals.to_frame().T])
    total_counts = group.sum(axis=1)
    percentages = group.div(total_counts, axis=0) * 100
    # landings = data[["ASFIS Scientific Name", 2021]].rename(columns={"ASFIS Scientific Name": "Species"}).groupby("Species").sum()
    landings = (
        fishstat[fishstat["ASFIS Scientific Name"].isin(species_list)]
        .groupby("ASFIS Scientific Name")[2021]
        .sum()
        .sort_values(ascending=False)
    )
    landings = (
        landings.to_frame()
        .reset_index()
        .rename(columns={"ASFIS Scientific Name": "Species"})
        .set_index("Species")
    )
    result = pd.concat(
        [group, percentages, landings], axis=1, keys=["Counts", "%", "Landings"]
    )
    result.columns.names = ["Metric", "Status"]
    result = result.rename_axis("Species").reset_index()
    result.sort_values(("Landings", 2021), ascending=False, inplace=True)
    result.loc[result[("Species", "")] == "Global", ("Landings", 2021)] = result[
        ("Landings", 2021)
    ].sum()

    result[("Landings", 2021)] /= 1e6
    result = result.rename(columns={2021: "2021 (Mt)"}, level=1)

    return result


def convert_sg_landings_long(
    special_group, species_landings, year_start=1950, year_end=2021
):
    sl_sg_mask = species_landings["Area"] == special_group
    sl_sg = species_landings[sl_sg_mask].copy()
    sl_wo_sg = species_landings[~sl_sg_mask].copy()

    sg_list = sl_sg["ASFIS Scientific Name"].unique()

    area_sg_mask = sl_wo_sg["ASFIS Scientific Name"].isin(sg_list)
    sl_area_sg = sl_wo_sg[area_sg_mask]
    sl_area_sg_set = set(zip(sl_area_sg["Area"], sl_area_sg["ASFIS Scientific Name"]))

    def convert_sg_landings(row, years, sg_set=sl_area_sg_set):
        sn = row["ASFIS Scientific Name"]

        sl_tot_dict = {}

        for year in years:
            if isinstance(row[year], (int, float)):
                sl_tot_dict[year] = row[year]
            else:
                sl_dict = json.loads(row[year])

                sl_tot = sum(
                    sl_dict[area]
                    for area in sl_dict.keys()
                    if (int(area), sn) not in sg_set
                )

                sl_tot_dict[year] = sl_tot

        return pd.Series(sl_tot_dict)

    years = list(range(year_start, year_end + 1))

    sl_sg[years] = sl_sg.apply(convert_sg_landings, args=(years,), axis=1)

    sl = (
        pd.concat([sl_wo_sg, sl_sg])
        .reset_index(drop=True)
        .sort_values(["Area", "ASFIS Scientific Name", "Location"])
    )

    return sl


def compute_total_area_landings(
    area,
    fishstat,
    species_landings,
    special_groups=["Salmon", "Sharks", "Tuna"],
    isscaap_to_remove=[],
    year_start=1950,
    year_end=2021,
    special_groups_to_convert=[],
):
    # Convert special groups with landings saved as dictionary back to numeric
    if area in special_groups_to_convert:
        sl = convert_sg_landings_long(area, species_landings, year_start, year_end)
    else:
        sl = species_landings.copy()

    # Define special groups masks to either take out special group landings from FAO Areas
    # or calculate landings for special group categories

    sg_masks = {}

    for sg in special_groups:
        sg_masks[sg] = {}

        sl_mask = sl["Area"] == sg
        sg_list = sl[sl_mask]["ASFIS Scientific Name"].unique()

        sg_area_mask = (
            fishstat["Area"] == 67
            if sg == "Salmon"
            else pd.Series(True, index=fishstat.index)
        )

        sg_mask_cap = fishstat["ASFIS Scientific Name"].isin(sg_list) & sg_area_mask

        sg_masks[sg]["fishstat"] = sg_mask_cap

        sg_mask_sl = sl["ASFIS Scientific Name"].isin(sg_list)

        if sg == "Salmon":
            sg_mask_sl = sg_mask_sl & sl["Area"] == 67

        sg_masks[sg]["sl"] = sg_mask_sl

    if area in special_groups:
        cap = fishstat[sg_masks[area]["fishstat"]]

        years = list(range(year_start, year_end + 1))

        total_cap = cap[years].sum()

        # Remove landings from sharks which are reported in FAO Areas in assessment

        if area == "Sharks":
            numeric_areas = [
                area
                for area in sl["Area"].unique()
                if isinstance(area, int) or area == "48,58,88"
            ]
            numeric_areas_mask = sl["Area"].isin(numeric_areas)

            sl_sg_cap = sl[sg_masks[area]["sl"] & numeric_areas_mask]

            sl_sg_cap = sl_sg_cap.drop_duplicates(
                subset=["Area", "ASFIS Scientific Name"]
            )

            total_cap -= sl_sg_cap[years].sum()

            total_area = sl[sl["Area"] == area][years].sum()

            total_cap = total_cap.combine(total_area, max)

        return total_cap

    if area == "48,58,88":
        area_list = [48, 58, 88]
    else:
        area_list = [area]

    area_mask_cap = fishstat["Area"].isin(area_list)

    isscaap_mask_cap = ~fishstat["ISSCAAP Code"].isin(isscaap_to_remove)

    all_sg_mask_cap = pd.Series(False, index=area_mask_cap.index)

    for sg, masks_dict in sg_masks.items():
        all_sg_mask_cap = all_sg_mask_cap | masks_dict["fishstat"]

    cap = fishstat[area_mask_cap & isscaap_mask_cap & ~all_sg_mask_cap]

    # Add special group landings back to cap which appear reported in FAO Area in assessment

    sl_area_mask = sl["Area"] == area

    for sg in special_groups:
        sg_in_area = sl[sl_area_mask & sg_masks[sg]["sl"]]
        sg_in_area = sg_in_area.drop_duplicates(subset="ASFIS Scientific Name")

        if not sg_in_area.empty:
            cap = pd.concat([cap, sg_in_area])

    # Add landings from assessed stocks in ISSCAAP Groups which have been removed
    sl_isscaap_mask = sl["ISSCAAP Code"].isin(isscaap_to_remove)

    lta = sl[sl_area_mask & sl_isscaap_mask]

    if not lta.empty:
        cap = pd.concat([cap, lta])

    years = list(range(year_start, year_end + 1))

    total_cap = cap[years].sum()

    return total_cap


def compute_total_aquaculture_landings(
    area,
    aquaculture,
    species_landings,
    special_groups=["Salmon", "Sharks", "Tuna"],
    isscaap_to_remove=[],
    year_start=1950,
    year_end=2021,
):
    sg_masks = {}

    for sg in special_groups:
        sl_mask = species_landings["Area"] == sg
        sg_list = species_landings[sl_mask]["ASFIS Scientific Name"].unique()

        sg_area_mask = (
            aquaculture["Area"] == 67
            if sg == "Salmon"
            else pd.Series(True, index=aquaculture.index)
        )

        sg_mask_aqua = aquaculture["ASFIS Scientific Name"].isin(sg_list) & sg_area_mask

        sg_masks[sg] = sg_mask_aqua

    years = list(range(year_start, year_end + 1))

    if area in special_groups:
        aqua = aquaculture[sg_masks[area]]

        total_aqua = aqua[years].sum()

        return total_aqua

    if area == "48,58,88":
        area_list = [48, 58, 88]
    else:
        area_list = [area]

    area_mask = aquaculture["Area"].isin(area_list)

    isscaap_mask = ~aquaculture["ISSCAAP Code"].isin(isscaap_to_remove)

    all_sg_mask = pd.Series(False, index=area_mask.index)

    for sg, mask in sg_masks.items():
        all_sg_mask = all_sg_mask | mask

    aqua = aquaculture[area_mask & isscaap_mask & ~all_sg_mask]

    total_aqua = aqua[years].sum()

    return total_aqua


def compute_appendix_landings(
    species_landings,
    fishstat,
    aquaculture,
    isscaap_to_remove,
    isscaap_code_to_name,
    scientific_names,
    location_to_area,
    iso3_to_name,
    year_start=1950,
    year_end=2021,
    last_decade_year=2010,
):
    # Convert Shark species landings from dictionary back to total number
    # Exclude landings from sharks in FAO areas
    agg_dict = {
        "Status": "first",
        "Tier": "first",
        "ASFIS Name": "first",
        "ISSCAAP Code": "first"
    }
    for year in range(year_start, year_end+1):
        agg_dict[year] = "sum"
    
    sl = species_landings.groupby(["Area", "ASFIS Scientific Name", "Location"]).agg(agg_dict).reset_index()

    # Group the Status and Uncertainty by tier
    def aggregate_status_by_tier(group, status_vals=["U", "M", "O"]):
        tier_data = []
        for tier in [1, 2, 3]:
            tier_group = group[group["Tier"] == tier]
            status_counts = tier_group["Status"].value_counts().to_dict()
            row = {"Tier": f"Tier {tier}"}
            for status in status_vals:
                row[status] = status_counts.get(status, np.nan)
            tier_data.append(row)
        return pd.DataFrame(tier_data)

    aggregated_status = (
        sl.groupby(["Area", "ASFIS Name", "ASFIS Scientific Name"])
        .apply(aggregate_status_by_tier)
        .reset_index()
    )

    # Group the rest of the columns
    aggregated_species = (
        sl.groupby(["Area", "ASFIS Name", "ASFIS Scientific Name"]).agg(
            {
                "Location": list,
                "ISSCAAP Code": "first",
                **{year: ["first", "sum"] for year in range(year_start, year_end + 1)},
            }
        )
    ).reset_index()

    aggregated_species.columns = [
        f"{col[0]}_{col[1]}" if col[1] and isinstance(col[0], int) else col[0]
        for col in aggregated_species.columns
    ]

    # Retrieve the most activate countries for each species for the given area(s)
    def most_active_countries(row, country_key="ISO3", year=2021):
        species, area = row["ASFIS Scientific Name"], row["Area"]

        if species not in scientific_names:
            return np.nan

        if isinstance(area, int):
            area_list = [area]
        elif area == "Salmon":
            area_list = [67]
        elif area == "48,58,88":
            area_list = [48, 58, 88]
        else:
            locs = row["Location"]
            area_map = location_to_area.get(row["Area"], {})
            area_list = []
            for loc in locs:
                area_list += area_map.get(loc, [])

        if ", " in species:
            species_list = species.split(", ")
            species_mask = fishstat["ASFIS Scientific Name"].isin(species_list)
        else:
            species_mask = fishstat["ASFIS Scientific Name"] == species

        area_mask = fishstat["Area"].isin(area_list)
        cap = fishstat[species_mask & area_mask][[country_key, year]]

        cap_countries = (
            cap.groupby(country_key)
            .sum()
            .sort_values(year, ascending=False)
            .reset_index()
        )
        cap_countries = cap_countries[cap_countries[year] > 0]
        cap_countries["Country"] = cap_countries[country_key].map(iso3_to_name)

        return ", ".join(cap_countries["Country"].values[:5])

    tqdm.pandas(desc="Retrieving Most Active Countries in 2021")

    aggregated_species["Most Active Countries in 2021"] = aggregated_species[
        ["ASFIS Scientific Name", "Area", "Location"]
    ].progress_apply(most_active_countries, axis=1)

    # Merge the groupings
    species_landings_dec = pd.merge(
        aggregated_species,
        aggregated_status,
        on=["Area", "ASFIS Name", "ASFIS Scientific Name"],
    )

    for year in range(year_start, year_end + 1):
        # Total landings are sum for species in "Tuna", "Sharks" areas
        # since same species correspond to different areas
        species_landings_dec[year] = species_landings_dec.apply(
            lambda row: (
                row[f"{year}_sum"]
                if row["Area"] in ["Tuna", "Sharks"]
                else row[f"{year}_first"]
            ),
            axis=1,
        )
        species_landings_dec.drop(
            columns=[f"{year}_first", f"{year}_sum"], inplace=True
        )

    # Report in kilotonnes
    for year in range(year_start, year_end + 1):
        species_landings_dec[year] /= 1e3

    # Create the decade columns for the appendix sheet
    def create_decade_cols(
        data,
        year_start=year_start,
        year_end=year_end,
        last_decade_year=last_decade_year,
    ):
        d = data.copy()
        for start in range(year_start, last_decade_year + 1, 10):
            end = start + 9

            if isinstance(data, pd.DataFrame):
                d[f"{start}-{end}"] = data.loc[:, range(start, end + 1)].mean(axis=1)
            elif isinstance(data, pd.Series):
                d[f"{start}-{end}"] = data.loc[start:end].mean()

        return d

    species_landings_dec = create_decade_cols(species_landings_dec)

    # Remove duplicate values in columns not in Tier, U, M, O
    def manually_group_df(df, check_col, group_cols):
        result = df.copy()
        for i in range(len(df) - 1):
            if df.loc[i, check_col] == df.loc[i + 1, check_col]:
                result.loc[i + 1, group_cols] = np.nan

        return result

    check_col = "ASFIS Scientific Name"
    tier_cols = ["Tier", "U", "M", "O"]
    group_cols = [
        col
        for col in species_landings_dec
        if col not in tier_cols + ["Area", "ISSCAAP Code"]
    ]

    species_landings_dec = manually_group_df(
        species_landings_dec, check_col, group_cols
    )

    # Reorder columns
    columns_order = [
        "Area",
        "ISSCAAP Code",
        "ASFIS Name",
        "ASFIS Scientific Name",
        "Most Active Countries in 2021",
    ]
    columns_order += sorted(
        [
            col
            for col in species_landings_dec.columns
            if isinstance(col, str) and "-" in col
        ]
    )
    columns_order += [
        col for col in species_landings_dec.columns if isinstance(col, int)
    ]
    columns_order += tier_cols
    species_landings_dec = species_landings_dec[columns_order]

    # Retrieve numeric columns
    def get_numeric_cols(cols):
        return [
            col
            for col in cols
            if isinstance(col, (float, int))
            or (isinstance(col, str) and col[0].isdigit())
        ]

    # Take out specificed ISSCAAP groups from aquaculture data
    aqua_isscaap_mask = ~aquaculture["ISSCAAP Code"].isin(isscaap_to_remove)
    aquaculture = aquaculture[aqua_isscaap_mask]

    # Create decade columns for aquaculture
    aquaculture = create_decade_cols(aquaculture)

    # Build the appendix landings sheets
    # Data with decade columns
    summaries_w_dec = {}
    # Data with individual years
    summaries_w_year = {}

    for area in tqdm(
        species_landings_dec["Area"].unique(), desc="Creating Appendix Sheets"
    ):
        # Total assessed landings in area
        area_landings = species_landings_dec[species_landings_dec["Area"] == area].drop(
            columns="Area"
        )

        # Create total rows for each ISSCAAP group
        isscaap_total = area_landings.groupby("ISSCAAP Code").sum().reset_index()
        isscaap_total["ASFIS Name"] = isscaap_total["ISSCAAP Code"].apply(
            lambda x: (
                str(int(x)) + f" - {isscaap_code_to_name.get(x, " ")}"
                if isinstance(x, (int, float))
                else x
            )
        )
        isscaap_total.loc[
            :,
            [
                "ASFIS Scientific Name",
                "Most Active Countries in 2021",
            ]
            + tier_cols,
        ] = np.nan

        isscaap_grouped = (
            area_landings.groupby("ISSCAAP Code")[area_landings.columns]
            .apply(
                lambda group: pd.concat(
                    [
                        group,
                        isscaap_total[
                            isscaap_total["ISSCAAP Code"]
                            == group["ISSCAAP Code"].iloc[0]
                        ],
                    ],
                    ignore_index=True,
                ),
            )
            .reset_index(drop=True)
        )

        isscaap_grouped = isscaap_grouped[
            [col for col in isscaap_grouped.columns if col not in tier_cols] + tier_cols
        ]

        total_area = area_landings[get_numeric_cols(area_landings.columns)].sum()

        total_cap = (
            compute_total_area_landings(
                area,
                fishstat,
                species_landings,
                isscaap_to_remove=isscaap_to_remove,
                # special_groups_to_convert=["Sharks"],
            )
            / 1e3
        )

        total_cap = create_decade_cols(total_cap)

        diff_cap = total_cap - total_area

        total_aqua = (
            compute_total_aquaculture_landings(
                area, aquaculture, species_landings, isscaap_to_remove=isscaap_to_remove
            )
            / 1e3
        )

        total_aqua = create_decade_cols(total_aqua)

        total_production = total_cap + total_aqua

        total_area = total_area.to_frame().T
        total_area["ASFIS Name"] = "Total selected species groups"
        total_cap = total_cap.to_frame().T
        total_cap["ASFIS Name"] = "Total marine capture"
        diff_cap = diff_cap.to_frame().T
        diff_cap["ASFIS Name"] = "Total other species groups"
        total_aqua = total_aqua.to_frame().T
        total_aqua["ASFIS Name"] = "Total aquaculture"
        total_production = total_production.to_frame().T
        total_production["ASFIS Name"] = "Total production"

        area_summary = pd.concat(
            [
                isscaap_grouped,
                total_area,
                diff_cap,
                total_cap,
                total_aqua,
                total_production,
            ]
        ).reset_index(drop=True)

        def reverse_forward_fill(df, column_name):
            df_modified = df.copy()
            groups = (
                df_modified[column_name] != df_modified[column_name].shift()
            ).cumsum()

            def transform_group(group):
                first_value = group[column_name].iloc[0]
                group[column_name] = [first_value] + [np.nan] * (len(group) - 1)
                return group

            df_modified = df_modified.groupby(groups).apply(transform_group)

            return df_modified.reset_index(drop=True)

        area_summary = reverse_forward_fill(area_summary, "ISSCAAP Code")

        area_summary_dec = area_summary.drop(
            columns=list(range(year_start, last_decade_year + 10))
        )

        dec_cols = [
            f"{start}-{start+9}"
            for start in range(year_start, last_decade_year + 1, 10)
        ]
        area_summary_years = area_summary.drop(columns=dec_cols)

        summaries_w_dec[area] = area_summary_dec
        summaries_w_year[area] = area_summary_years

    return summaries_w_dec, summaries_w_year


def compute_sg_area_landings(
    stock_weights, species_landings, special_group, location_to_area
):
    sl_sg_mask = species_landings["Area"] == special_group
    sl = species_landings[sl_sg_mask][
        ["ASFIS Scientific Name", "Location", "Status", "Tier", 2021]
    ].copy()

    sw_sg_mask = stock_weights["Area"] == special_group
    sw = stock_weights[sw_sg_mask][
        ["ASFIS Scientific Name", "Location", "Normalized Weight"]
    ].copy()

    sg_landings = pd.merge(sl, sw, on=["ASFIS Scientific Name", "Location"])

    def compute_sg_landings(row):
        if isinstance(row["Normalized Weight"], (int, float)) and isinstance(
            row[2021], (int, float)
        ):
            areas = location_to_area[special_group][row["Location"]]

            landings = [row["Normalized Weight"] * row[2021]]

            return pd.Series([areas, landings], index=["Area", "Stock Landings 2021"])

        w_dict = json.loads(row["Normalized Weight"])
        l_dict = json.loads(row[2021])

        areas, landings = [], []

        for area, area_landings in l_dict.items():
            weight = w_dict.get(area, 0)

            l = area_landings * weight

            areas.append(int(area))
            landings.append(l)

        return pd.Series([areas, landings], index=["Area", "Stock Landings 2021"])

    sg_landings[["Area", "Stock Landings 2021"]] = sg_landings.apply(
        compute_sg_landings, axis=1
    )

    sg_landings = sg_landings.explode(["Area", "Stock Landings 2021"])

    sg_landings = sg_landings[
        [
            "Area",
            "ASFIS Scientific Name",
            "Location",
            "Status",
            "Tier",
            "Stock Landings 2021",
        ]
    ]

    mask_485888 = sg_landings["Area"].isin([48, 58, 88])

    sg_landings.loc[mask_485888, "Area"] = "48,58,88"

    return sg_landings


def compute_weighted_percentages(
    stock_landings,
    fishstat=None,
    key="Area",
    location_to_area={},
    add_salmon=False,
    shark_area_landings=pd.DataFrame(),
    year=2021,
    landings_key="Stock Landings 2021",
):
    data = stock_landings.copy()

    def add_special_group_landings(data, special_group, lta, fs=fishstat):
        df = data.copy()

        sn = "ASFIS Scientific Name"
        sg_in_areas = pd.DataFrame()
        for idx, row in data[data["Area"] == special_group].iterrows():
            areas = lta[row["Location"]]

            if ", " in row[sn]:
                sn_mask = fs[sn].isin(row[sn].split(", "))
            else:
                sn_mask = fs[sn] == row[sn]

            for area in areas:
                sg_capture = fs[(fs["Area"] == area) & sn_mask][year].sum()
                if sg_capture > 0:
                    sg_in_area = pd.DataFrame(
                        {
                            "Area": area,
                            "ASFIS Scientific Name": row[sn],
                            "Status": row["Status"],
                            landings_key: sg_capture,
                        },
                        index=[len(sg_in_areas)],
                    )
                    sg_in_areas = pd.concat([sg_in_areas, sg_in_area])

        sg_in_areas = sg_in_areas.drop_duplicates(
            subset=["Area", "ASFIS Scientific Name", "Status"]
        )

        # Add the area specific tuna rows, and remove the Tuna category
        df = df[~(df["Area"] == special_group)]
        df = pd.concat([df, sg_in_areas]).reset_index(drop=True)

        return df

    for special_group, lta in location_to_area.items():
        # Add the special group stocks back into the areas from which they came
        # One tuna assessment corresponds to multiple
        # assessments added back into the area with the same status
        # as original assessment and landing specific to that area
        if special_group != "Deep Sea":
            data = add_special_group_landings(data, special_group, lta, fishstat)

    if add_salmon:
        salmon_mask = data["Area"] == "Salmon"
        data.loc[salmon_mask, "Area"] = 67

    if not shark_area_landings.empty:
        sharks_mask = data["Area"] == "Sharks"
        data = data[~sharks_mask]

        data = pd.concat([data, shark_area_landings])

    # Group by key and Status to aggregate data
    group = data.groupby([key, "Status"])[landings_key].sum().unstack(fill_value=0)

    # Add a "Global" aggregation row
    global_totals = group.sum(axis=0)
    global_totals.name = "Global"
    group = pd.concat([group, global_totals.to_frame().T])

    # Calculate total landings per group
    total_landings = group.sum(axis=1).to_frame(name="Total Landings (Mt)")

    # Ensure required columns exist before computations
    for col in ["M", "U", "O"]:
        if col not in group.columns:
            group[col] = 0  # Add missing columns to avoid KeyErrors

    # Compute total sustainable and unsustainable landings
    total_landings["Sustainable (Mt)"] = (group["M"] + group["U"]) / 1e6
    total_landings["Unsustainable (Mt)"] = group["O"] / 1e6
    total_landings["MSF (Mt)"] = group["M"] / 1e6
    total_landings["U (Mt)"] = group["U"] / 1e6
    total_landings["O (Mt)"] = group["O"] / 1e6

    # Ensure no division by zero
    wp = group.div(group.sum(axis=1).replace(0, 1), axis=0) * 100

    # Compute weighted percentages
    wp["Sustainable (%)"] = wp["M"] + wp["U"]
    wp["Unsustainable (%)"] = wp["O"]
    wp.rename(columns={"U": "U (%)", "M": "MSF (%)", "O": "O (%)"}, inplace=True)

    if key == "Area" and "48,58,88" not in total_landings.index:
        new_row = pd.DataFrame(
            {
                "Total Landings (Mt)": 0,
                "Sustainable (Mt)": 0,
                "Unsustainable (Mt)": 0,
                "MSF (Mt)": 0,
                "U (Mt)": 0,
                "O (Mt)": 0,
            },
            index=["48,58,88"],
        )
        total_landings = pd.concat([total_landings, new_row])

    # Organize and rename columns
    total_landings = total_landings[
        ["U (Mt)", "MSF (Mt)", "O (Mt)", "Sustainable (Mt)", "Unsustainable (Mt)"]
    ]
    wp = wp[["U (%)", "MSF (%)", "O (%)", "Sustainable (%)", "Unsustainable (%)"]]

    # Combine totals and percentages
    result = pd.concat(
        [total_landings, wp],
        axis=1,
        keys=["Total Landings", "Weighted % by Landings"],
    )

    result.index.name = key

    return result


def get_weighted_percentages_and_total_landings(
    weighted_percentages,
    appendix_landings={},
    tuna_landings=pd.DataFrame(),
    fishstat=pd.DataFrame(),
    isscaap_to_remove=[],
    areas=[],
    year=2021,
    special_groups=True,
):
    total_landings = {"Global": 0}

    # If we are including special groups, we use appendix landings
    if appendix_landings:
        for area, df in appendix_landings.items():
            # Check if tunas have been taken out as separate category
            if not tuna_landings.empty and area == "Tuna":
                continue

            if isinstance(area, str) and area.isdigit():
                area = int(area)

            tot = (
                df.loc[df["ASFIS Name"] == "Total marine capture", 2021].values[0] / 1e3
            )  # Convert to Mt

            if not tuna_landings.empty:
                tl_area_mask = (
                    tuna_landings["Area"].isin([48, 58, 88])
                    if area == "48,58,88"
                    else tuna_landings["Area"] == area
                )
                tl = tuna_landings.loc[tl_area_mask, year].values / 1e6  # Convert to Mt
                if tl:
                    tot += tl

            total_landings[area] = tot
            total_landings["Global"] += tot
    elif (
        not fishstat.empty
    ):  # Otherwise use fishstat data to get totals for area without special groups
        isscaap_mask = ~fishstat["ISSCAAP Code"].isin(isscaap_to_remove)
        fs = fishstat[isscaap_mask].copy()

        areas_mask = fs["Area"].isin(areas)
        fs = fs[areas_mask]

        mask_485888 = fs["Area"].isin([48, 58, 88])
        fs["Area"] = fs["Area"].astype(object)
        fs.loc[mask_485888, "Area"] = "48,58,88"

        fs_grouped = fs.groupby("Area")[2021]

        for area, group in fs_grouped:
            tot = group.sum() / 1e6  # Convert to Mt

            total_landings[area] = tot
            total_landings["Global"] += tot

    total_landings_df = pd.DataFrame(total_landings, index=[0]).T
    total_landings_df.columns = pd.MultiIndex.from_tuples([("", "Total Landings (Mt)")])

    w = weighted_percentages.copy()

    w[("", "Total Assessed Landings (Mt)")] = (
        w[("Total Landings", "Sustainable (Mt)")]
        + w[("Total Landings", "Unsustainable (Mt)")]
    )

    w.drop(
        columns=[
            ("Total Landings", "Sustainable (Mt)"),
            ("Total Landings", "Unsustainable (Mt)"),
            ("Total Landings", "U (Mt)"),
            ("Total Landings", "MSF (Mt)"),
            ("Total Landings", "O (Mt)"),
        ],
        inplace=True,
    )

    result = pd.merge(
        w, total_landings_df, left_index=True, right_index=True, how="left"
    )

    if not special_groups:
        sg_mask = result.index.isin(["Deep Sea", "Salmon", "Sharks", "Tuna"])

        result = result[~sg_mask]

    result = result[
        [("", "Total Landings (Mt)"), ("", "Total Assessed Landings (Mt)")]
        + [col for col in result.columns if col[0] == "Weighted % by Landings"]
    ]

    return result


def get_weighted_percentages_by_tier_and_area(stock_landings, total_landings):
    areas = stock_landings["Area"].unique()
    areas_df = pd.DataFrame()

    tl_cols = [("", "", "Total Landings (Mt)")]

    def wp_tier(stock_landings, area=None):
        if area:
            area_mask = stock_landings["Area"] == area
        else:
            area_mask = pd.Series(True, index=stock_landings.index)
            area = "Global"

        d = compute_weighted_percentages(stock_landings[area_mask], key="Tier")

        d2_cols = (
            [("", "", "Area")]
            + tl_cols
            + [
                (f"Tier {i}", col[0], col[1])
                for i in d.index
                if isinstance(i, int)
                for col in d.columns
            ]
        )
        d2 = pd.DataFrame(columns=pd.MultiIndex.from_tuples(d2_cols))

        d2.loc[0, ("", "", "Area")] = area

        d2.loc[0, tl_cols] = total_landings.loc[
            area, [(col[0], col[2]) for col in tl_cols]
        ].values

        d2 = d2.rename(
            columns={"Total Landings (Mt)": "Total Landings in Area (Mt)"}, level=2
        )

        for i in range(1, 4):
            cols = [col for col in d2.columns if col[0] == f"Tier {i}"]
            if i in d.index:
                d2.loc[0, cols] = d.loc[i].values
                d2[(f"Tier {i}", "", "Total Landings (Mt)")] = (
                    d2.loc[0, (f"Tier {i}", "Total Landings", "Sustainable (Mt)")]
                    + d2.loc[0, (f"Tier {i}", "Total Landings", "Unsustainable (Mt)")]
                )
            else:
                d2.loc[0, cols] = [0] * len(cols)
                d2[(f"Tier {i}", "", "Total Landings (Mt)")] = 0

        cols_to_drop = [
            "U (Mt)",
            "MSF (Mt)",
            "O (Mt)",
            "Sustainable (Mt)",
            "Unsustainable (Mt)",
        ]
        d2 = d2.drop(
            columns=[
                (f"Tier {i}", "Total Landings", col)
                for i in range(1, 4)
                if i in d.index
                for col in cols_to_drop
            ]
        )
        tier1_cols = [
            col
            for col in d2.columns
            if col[0] == "Tier 1" and col[1] == "Weighted % by Landings"
        ]
        tier2_cols = [
            col
            for col in d2.columns
            if col[0] == "Tier 2" and col[1] == "Weighted % by Landings"
        ]
        tier3_cols = [
            col
            for col in d2.columns
            if col[0] == "Tier 3" and col[1] == "Weighted % by Landings"
        ]
        col_sort = (
            [("", "", "Area"), ("", "", "Total Landings in Area (Mt)")]
            + [("Tier 1", "", "Total Landings (Mt)")]
            + tier1_cols
            + [("Tier 2", "", "Total Landings (Mt)")]
            + tier2_cols
            + [("Tier 3", "", "Total Landings (Mt)")]
            + tier3_cols
        )

        return d2[col_sort]

    for area in areas:
        area_df = wp_tier(stock_landings, area)
        areas_df = pd.concat([areas_df, area_df])

    global_df = wp_tier(stock_landings)

    areas_df = pd.concat([areas_df, global_df])

    return areas_df


def remove_isscaap_fishstat(
    fishstat, stock_landings, isscaap_to_remove, landings_key, year
):
    sl, fs = stock_landings.copy(), fishstat.copy()
    fs_isscaap_mask = ~fs["ISSCAAP Code"].isin(isscaap_to_remove)
    sl_isscaap_mask = sl["ISSCAAP Code"].isin(isscaap_to_remove)

    # Take out ISSCAAP Groups from Fishstat
    fs = fs[fs_isscaap_mask]

    # Add landings back to Fishstat from Stock Landings which are in ISSCAAP to remove
    lta = sl[sl_isscaap_mask][
        ["Area", "ASFIS Scientific Name", "Location", "ISSCAAP Code"] + [landings_key]
    ]

    # Convert Areas 48,58,88 back to original area
    lta_southern_mask = lta["Area"] == "48,58,88"

    def loc_to_area_southern(loc):
        # Default to Area 48 if area cannot be found
        # Areas 48,58,88 are aggregated anyways in compute_percent_coverage
        if pd.isna(loc):
            return 48

        area = loc.split(".")[0]

        return int(area) if area.isdigit() else 48

    lta.loc[lta_southern_mask, "Area"] = lta.loc[lta_southern_mask, "Location"].apply(
        loc_to_area_southern
    )

    lta = lta.drop(columns="Location")
    lta = lta.rename(columns={landings_key: year})

    if not lta.empty:
        fs = pd.concat([fs, lta])

    return fs


def compute_sg_long(
    stock_landings, fishstat, species_landings, weights, location_to_area
):
    # Create tuna df
    tuna_mask = stock_landings["Area"] == "Tuna"
    tuna_df = stock_landings[tuna_mask][
        ["ASFIS Scientific Name", "Tier", "Location"]
    ].copy()

    tuna_df["Area"] = tuna_df["Location"].map(location_to_area["Tuna"])

    tuna_df = tuna_df.explode("Area")

    tuna_df[2021] = tuna_df.apply(
        compute_species_landings,
        args=(
            fishstat,
            {},
            2021,
            2021,
            "ASFIS Scientific Name",
        ),
        axis=1,
    )

    tuna_df = tuna_df.rename(columns={2021: "Stock Landings 2021"})
    tuna_df = tuna_df[["Area", "ASFIS Scientific Name", "Tier", "Stock Landings 2021"]]

    # Create salmon df
    salmon_mask = stock_landings["Area"] == "Salmon"
    salmon_df = stock_landings[salmon_mask].copy()
    salmon_df.loc[:, "Area"] = 67
    salmon_df = salmon_df.rename(columns={2021: "Stock Landings 2021"})
    salmon_df = salmon_df[
        ["Area", "ASFIS Scientific Name", "Tier", "Stock Landings 2021"]
    ]

    # Create sharks df
    w_sharks_mask = weights["Area"] == "Sharks"
    sharks_weights = weights[w_sharks_mask].copy()

    sl_sharks_mask = species_landings["Area"] == "Sharks"
    sharks_sl = species_landings[sl_sharks_mask].copy()

    primary_key = ["Area", "ASFIS Scientific Name", "Location"]

    sharks_comb = pd.merge(sharks_sl, sharks_weights, on=primary_key)[
        ["ASFIS Scientific Name", "Tier", "Location", 2021, "Normalized Weight"]
    ]

    sharks_dict = {
        "Area": [],
        "ASFIS Scientific Name": [],
        "Tier": [],
        "Stock Landings 2021": [],
    }

    for idx, row in sharks_comb.iterrows():
        if isinstance(row[2021], float):
            area = location_to_area["Sharks"][row["Location"]][0]

            sharks_dict["Area"].append(area)
            sharks_dict["ASFIS Scientific Name"].append(row["ASFIS Scientific Name"])
            sharks_dict["Tier"].append(row["Tier"])
            sharks_dict["Stock Landings 2021"].append(row[2021])

        elif isinstance(row[2021], str):
            w = json.loads(row["Normalized Weight"])
            l = json.loads(row[2021])

            for area_str, landings in l.items():
                sl = w[area_str] * landings
                area = int(area_str)

                sharks_dict["Area"].append(area)
                sharks_dict["ASFIS Scientific Name"].append(
                    row["ASFIS Scientific Name"]
                )
                sharks_dict["Tier"].append(row["Tier"])
                sharks_dict["Stock Landings 2021"].append(sl)

    sharks_df = pd.DataFrame(sharks_dict)

    return [tuna_df, salmon_df, sharks_df]


def compute_percent_coverage(
    stock_landings,
    species_landings,
    fishstat,
    isscaap_to_remove,
    landings_key="Stock Landings 2021",
    tier=None,
    year=2021,
):
    total_cov, total_area_l = 0, 0
    pc_dict = {}
    
    areas = stock_landings["FAO Area"].unique()

    for area in areas:
        tier_mask = stock_landings["Tier"] == tier if tier else pd.Series(True, index=stock_landings.index)

        area_mask = stock_landings["FAO Area"] == area

        cov = stock_landings[tier_mask & area_mask][landings_key].sum()

        area_l = compute_total_area_landings(
            area,
            fishstat,
            species_landings,
            isscaap_to_remove=isscaap_to_remove,
            special_groups=[],
        )[year]
        
        pc_dict[area] = 100 * cov / area_l

        total_cov += cov
        total_area_l += area_l

    pc_dict["Global"] = 100 * total_cov / total_area_l
    
    pc = pd.DataFrame(pc_dict, index=["Coverage (%)"]).T.reset_index(names="Area")

    return pc


def compute_percent_coverage_tiers(
    stock_landings,
    species_landings,
    fishstat,
    isscaap_to_remove,
):
    pc_tiers = pd.DataFrame()

    for tier in [1, 2, 3]:
        pc_tier = compute_percent_coverage(
            stock_landings,
            species_landings,
            fishstat,
            isscaap_to_remove,
            tier=tier,
        )

        pc_tier = pc_tier.rename(columns={"Coverage (%)": f"Coverage (%) Tier {tier}"})

        if pc_tiers.empty:
            pc_tiers = pc_tier.copy()
        else:
            pc_tiers = pd.merge(pc_tiers, pc_tier, on="Area")

    pc_tiers = pc_tiers.set_index("Area")

    pc_tiers["Coverage (%) Total"] = pc_tiers.sum(axis=1)

    return pc_tiers.reset_index()


def compare_weighted_percentages(previous, update):
    cols = [
        ("Weighted % by Landings", "U (%)"),
        ("Weighted % by Landings", "MSF (%)"),
        ("Weighted % by Landings", "O (%)"),
        ("Weighted % by Landings", "Sustainable (%)"),
        ("Weighted % by Landings", "Unsustainable (%)"),
    ]

    comparison_df = pd.concat(
        [
            update[cols].rename(
                columns={"Weighted % by Landings": "Updated SoSI Categories"}
            ),
            previous[cols].rename(
                columns={"Weighted % by Landings": "Previous SoSI Categories"}
            ),
        ],
        axis=1,
    )

    comparison_df = comparison_df.reset_index().rename(columns={"index": "Area"})

    return comparison_df.set_index("Area")


def compute_species_weighted_percentages(stock_landings, species_list):
    species_mask = stock_landings["ASFIS Scientific Name"].isin(species_list)
    species_data = stock_landings[species_mask]

    group = (
        species_data.groupby(["ASFIS Scientific Name", "Status"])["Stock Landings 2021"]
        .sum()
        .unstack(fill_value=0)
    )

    global_totals = group.sum(axis=0)
    global_totals.name = "Global"
    group = pd.concat([group, global_totals.to_frame().T])

    total_landings = group.sum(axis=1)
    weighted_percentages = group.div(total_landings, axis=0) * 100

    result = pd.concat(
        [group, weighted_percentages],
        axis=1,
        keys=["Total Landings", "Weighted % by Landings"],
    )

    result.columns.names = ["Metric", "Status"]
    result = result.rename_axis("Species").reset_index()

    for status in ["M", "O", "U"]:
        result[("Total Landings", status)] /= 1e6
    result = result.rename(columns={"Total Landings": "Total Landings (Mt)"}, level=0)

    return result


def compute_top_species_by_area(
    areas, stock_assessments, stock_landings, fishstat, n=10, year=2021
):
    top_species_dfs = {}

    for area in areas:
        if isinstance(area, int):
            area_list = [area]
        elif "," in area:
            area_list = [int(a) for a in area.split(",")]
        else:
            print(f"Area {area} is not a FAO Major Fishing Area")
            return

        fs_area_mask = fishstat["Area"].isin(area_list)
        sa_area_mask = stock_assessments["Area"] == area

        species_in_area = stock_assessments[sa_area_mask][
            "ASFIS Scientific Name"
        ].unique()
        species_mask = fishstat["ASFIS Scientific Name"].isin(species_in_area) & (
            fishstat["ASFIS Scientific Name"] != "Actinopterygii"
        )

        top_species = (
            fishstat[fs_area_mask & species_mask]
            .groupby("ASFIS Scientific Name")[year]
            .sum()
            .nlargest(n)
            .reset_index()
        )
        top_species[2021] /= 1e3
        top_species = top_species.rename(
            columns={2021: "Landings 2021 (in thousand tonnes, live weight equivalent)"}
        )

        top_species_list = list(top_species["ASFIS Scientific Name"])

        sa_top_species_mask = stock_assessments["ASFIS Scientific Name"].isin(
            top_species_list
        )

        sbn = compute_status_by_number(
            stock_assessments[sa_top_species_mask & sa_area_mask],
            "ASFIS Scientific Name",
        )
        sbn.loc[sbn["ASFIS Scientific Name"] == "Global", "ASFIS Scientific Name"] = (
            "Total"
        )

        sl_area_mask = stock_landings["Area"] == area
        sl_top_species_mask = stock_landings["ASFIS Scientific Name"].isin(
            top_species_list
        )

        sbl = compute_weighted_percentages(
            stock_landings[sl_area_mask & sl_top_species_mask],
            key="ASFIS Scientific Name",
        )
        sbl.columns = [col[1].replace("(Mt)", "(Kt)") for col in sbl.columns]
        sbl.loc[:, [col for col in sbl.columns if "(Kt)" in col]] *= 1e3
        sbl = sbl.reset_index()

        comb = pd.merge(top_species, sbn, on="ASFIS Scientific Name")
        comb = pd.merge(
            comb,
            sbl,
            on="ASFIS Scientific Name",
            suffixes=(" by Number", " by Landings"),
        )

        top_species_dfs[area] = comb

    return top_species_dfs
