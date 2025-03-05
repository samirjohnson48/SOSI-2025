"""

"""

import pandas as pd
import numpy as np
from openpyxl import load_workbook
from functools import reduce


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


def compute_status_by_number(data, group):
    grouped = (
        data.groupby(group)
        .agg(
            **{
                "No. of stocks": (group, "size"),
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

    # Add a final row with total values
    totals = pd.DataFrame(
        {
            group: ["Global"],
            "No. of stocks": [data.shape[0]],
            "No. of U": [(data["Status"] == "U").sum()],
            "No. of MSF": [(data["Status"] == "M").sum()],
            "No. of O": [(data["Status"] == "O").sum()],
            "No. of Sustainable": [data["Status"].isin(["U", "M"]).sum()],
            "No. of Unsustainable": [(data["Status"] == "O").sum()],
            "U (%)": [((data["Status"] == "U").sum() / data.shape[0]) * 100],
            "MSF (%)": [((data["Status"] == "M").sum() / data.shape[0]) * 100],
            "O (%)": [((data["Status"] == "O").sum() / data.shape[0]) * 100],
            "Sustainable (%)": [
                (
                    ((data["Status"] == "M") | (data["Status"] == "U")).sum()
                    / data.shape[0]
                )
                * 100
            ],
            "Unsustainable (%)": [
                ((data["Status"] == "O").sum() / data.shape[0]) * 100
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
        data.groupby("Tier")
        .agg(
            {
                "Tier": "size",
                "Status": lambda x: x.isin(["U", "M", "O"]).sum(),
                "ASFIS Scientific Name": lambda x: x[assessed_data_mask].nunique(),
                "ISSCAAP Code": lambda x: x[
                    assessed_data_mask & numeric_isscaap_mask
                ].nunique(),
            }
        )
        .rename(
            columns={
                "Tier": "Total stocks",
                "Status": "Total assessed stocks",
                "ASFIS Scientific Name": "Total ASFIS species (from total assessed stocks)",
                "ISSCAAP Code": "Total ISSCAAP Groups (from total assessed stocks)",
            }
        )
    )

    # Add a Total row
    summary.loc["Total"] = summary.sum()
    summary.loc["Total", "Total ASFIS species (from total assessed stocks)"] = data.loc[
        assessed_data_mask, "ASFIS Scientific Name"
    ].nunique()
    summary.loc["Total", "Total ISSCAAP Groups (from total assessed stocks)"] = (
        data.loc[assessed_data_mask & numeric_isscaap_mask, "ISSCAAP Code"].nunique()
    )

    return summary


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
    species_landings_dec = species_landings.copy()

    # Standardize the uncertainty
    species_landings_dec["Uncertainty"] = species_landings_dec["Uncertainty"].apply(
        lambda x: (
            x[0].upper()
            if isinstance(x, str) and x[0].upper() in ["L", "M", "H"]
            else "X"
        )
    )

    # Group the data by species within each area
    species_landings_dec = (
        species_landings_dec.groupby(
            ["Area", "ASFIS Name", "ASFIS Scientific Name"]
        ).agg(
            {
                "Location": list,
                "Status": list,
                "Uncertainty": list,
                "ISSCAAP Code": "first",
                **{year: ["first", "sum"] for year in range(1950, year_end + 1)},
            }
        )
    ).reset_index()

    species_landings_dec.columns = [
        f"{col[0]}_{col[1]}" if col[1] and isinstance(col[0], int) else col[0]
        for col in species_landings_dec.columns
    ]

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
    def create_decade_cols(data, year_start=1950, year_end=2021, last_decade_year=2010):
        d = data.copy()
        for start in range(year_start, last_decade_year + 1, 10):
            end = start + 9
            d[f"{start}-{end}"] = data.loc[:, range(start, end + 1)].mean(axis=1)
        d[f"2020-{year_end}"] = data.loc[
            :, range(last_decade_year + 10, year_end + 1)
        ].mean(axis=1)
        return d

    species_landings_dec = create_decade_cols(species_landings_dec)

    # Report Status, Uncertainty as list per species
    species_landings_dec["Status"] = species_landings_dec["Status"].apply(
        lambda x: ", ".join(x)
    )
    species_landings_dec["Uncertainty"] = species_landings_dec["Uncertainty"].apply(
        lambda x: ", ".join(x)
    )

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

    species_landings_dec["Most Active Countries in 2021"] = species_landings_dec[
        ["ASFIS Scientific Name", "Area", "Location"]
    ].apply(most_active_countries, axis=1)

    species_landings_dec = species_landings_dec.drop(columns=["Location"])

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
    columns_order += ["Status", "Uncertainty"]
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

    # Define the landings to be added back in
    # i.e. assessed stocks which are included in the ISSCAAP groups removed
    sldec_isscaap_mask = species_landings_dec["ISSCAAP Code"].isin(isscaap_to_remove)
    landings_to_add = species_landings_dec[sldec_isscaap_mask]

    # Build the appendix landings sheets
    # Data with decade columns
    summaries_w_dec = {}
    # Data with individual years
    summaries_w_year = {}

    for area in species_landings_dec["Area"].unique():
        # Cannot calculate total landings for Deep Sea so we skip this category
        if area == "Deep Sea":
            continue

        # Total assessed landings in area
        area_landings = species_landings_dec[species_landings_dec["Area"] == area].drop(
            columns="Area"
        )

        # Create total rows for each ISSCAAP group
        isscaap_total = (
            area_landings.groupby("ISSCAAP Code")
            .filter(lambda x: len(x) > 1)
            .groupby("ISSCAAP Code")
            .sum()
            .reset_index()
        )
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
                "Status",
                "Uncertainty",
                "Most Active Countries in 2021",
            ],
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
            [
                col
                for col in isscaap_grouped.columns
                if col not in ["Status", "Uncertainty"]
            ]
            + ["Status", "Uncertainty"]
        ]

        total_area = area_landings[get_numeric_cols(area_landings.columns)].sum()

        if area == "48,58,88":
            area_list = [48, 58, 88]
        else:
            area_list = [area]

        # Define Tuna, Shark, and Salmon masks
        # These will be taken out of numerical areas so as to not double count

        # Tunas
        sl_tuna_mask = species_landings["Area"] == "Tuna"
        tuna_list = species_landings[sl_tuna_mask]["ASFIS Scientific Name"].unique()
        tuna_mask_cap = fishstat["ASFIS Scientific Name"].isin(tuna_list)
        tuna_mask_aqua = aquaculture["ISSCAAP Code"] == 36

        # Sharks
        sl_sharks_mask = species_landings["Area"] == "Sharks"
        sharks_list = species_landings[sl_sharks_mask]["ASFIS Scientific Name"].unique()
        sharks_mask_cap = fishstat["ASFIS Scientific Name"].isin(sharks_list)
        sharks_mask_aqua = aquaculture["ISSCAAP Code"] == 38

        # Salmon
        sl_salmon_mask = species_landings["Area"] == "Salmon"
        salmon_list = species_landings[sl_salmon_mask]["ASFIS Scientific Name"].unique()
        salmon_mask_cap = fishstat["ASFIS Scientific Name"].isin(salmon_list) & (
            fishstat["Area"] == 67
        )
        salmon_mask_aqua = (aquaculture["ISSCAAP Code"] == 23) & (
            aquaculture["Area"] == 67
        )

        # Masks for numerical areas
        area_mask_cap = fishstat["Area"].isin(area_list)
        area_mask_aqua = aquaculture["Area"].isin(area_list)
        isscaap_mask_cap = ~fishstat["ISSCAAP Code"].isin(isscaap_to_remove)
        isscaap_mask_aqua = ~aquaculture["ISSCAAP Code"].isin(isscaap_to_remove)

        # Define total landings for the areas
        if area == "Tuna":
            cap = fishstat[tuna_mask_cap]
            aqua = aquaculture[tuna_mask_aqua]
        elif area == "Sharks":
            cap = fishstat[sharks_mask_cap]
            aqua = aquaculture[sharks_mask_aqua]
        elif area == "Salmon":
            cap = fishstat[salmon_mask_cap]
            aqua = aquaculture[salmon_mask_aqua]
        else:
            # Take out Tuna, Sharks, and Salmon from numerical areas
            cap = fishstat[
                area_mask_cap
                & isscaap_mask_cap
                & ~tuna_mask_cap
                & ~sharks_mask_cap
                & ~salmon_mask_cap
            ]
            # Add tuna and sharks which were listed in numerical areas back to total capture
            sl_area_mask = species_landings["Area"] == area

            tuna_mask = species_landings["ASFIS Scientific Name"].isin(tuna_list)
            tuna_in_area = species_landings[sl_area_mask & tuna_mask].drop_duplicates(
                subset="ASFIS Scientific Name"
            )

            if not tuna_in_area.empty:
                cap = pd.concat([cap, tuna_in_area])

            sharks_mask = species_landings["ASFIS Scientific Name"].isin(sharks_list)
            sharks_in_area = species_landings[
                sl_area_mask & sharks_mask
            ].drop_duplicates(subset="ASFIS Scientific Name")

            if not sharks_in_area.empty:
                cap = pd.concat([cap, sharks_in_area])

            aqua = aquaculture[
                area_mask_aqua
                & isscaap_mask_aqua
                & ~tuna_mask_aqua
                & ~sharks_mask_aqua
                & ~salmon_mask_aqua
            ]

        cap = create_decade_cols(cap)
        cap = cap.drop(columns=["Alpha3_Code"])

        total_cap = cap[get_numeric_cols(cap.columns)].sum() / 1e3

        if area in landings_to_add["Area"].unique():
            additional_landings = landings_to_add[landings_to_add["Area"] == area][
                get_numeric_cols(landings_to_add.columns)
            ].sum()
            total_cap = total_cap.add(additional_landings, fill_value=0)

        diff_cap = total_cap - total_area

        total_aqua = aqua[get_numeric_cols(aqua.columns)].sum() / 1e3

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

        area_summary_dec = area_summary.drop(
            columns=list(range(year_start, last_decade_year + 1))
        )

        dec_cols = [
            f"{start}-{start+9}" for start in range(year_start, last_decade_year, 10)
        ] + [f"{last_decade_year+10}-{year_end}"]
        area_summary_years = area_summary.drop(columns=dec_cols)

        summaries_w_dec[area] = area_summary_dec
        summaries_w_year[area] = area_summary_years

    return summaries_w_dec, summaries_w_year


def compute_weighted_percentages(
    stock_landings,
    fishstat=None,
    key="Area",
    tuna_location_to_area={},
    year=2021,
    landings_key="Stock Landings 2021",
):
    data = stock_landings.copy()

    if tuna_location_to_area and key == "Area":
        # Add the Tuna back into the areas from which they came
        # One tuna assessment corresponds to multiple
        # assessments added back into the area with the same status
        # as original assessment and landing specific to that area
        sn = "ASFIS Scientific Name"
        tuna_in_areas = pd.DataFrame()
        for idx, row in data[data["Area"] == "Tuna"].iterrows():
            areas = tuna_location_to_area[row["Location"]]

            for area in areas:
                tuna_capture = fishstat[
                    (fishstat["Area"] == area) & (fishstat[sn] == row[sn])
                ][year].sum()
                if tuna_capture > 0:
                    tuna_in_area = pd.DataFrame(
                        {
                            "Area": area,
                            "ASFIS Scientific Name": row[sn],
                            "Status": row["Status"],
                            landings_key: tuna_capture,
                        },
                        index=[len(tuna_in_areas)],
                    )
                    tuna_in_areas = pd.concat([tuna_in_areas, tuna_in_area])

        # Add the area specific tuna rows, and remove the Tuna category
        data = data[~(data["Area"] == "Tuna")]
        data = pd.concat([data, tuna_in_areas]).reset_index(drop=True)

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

    # Returns the tunas added back into areas if applicable
    if tuna_location_to_area and key == "Area":
        return result, tuna_in_areas

    return result


def get_weighted_percentages_and_total_landings(
    weighted_percentages, appendix_landings, tuna_landings=pd.DataFrame(), year=2021
):
    total_landings = {"Global": 0}

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

    result = result[
        [("", "Total Landings (Mt)"), ("", "Total Assessed Landings (Mt)")]
        + [col for col in result.columns if col[0] == "Weighted % by Landings"]
    ]

    return result


def get_weighted_percentages_by_tier_and_area(stock_landings, total_landings):
    areas = [a for a in stock_landings["Area"].unique() if a != "Deep Sea"]
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

    fs = pd.concat([fs, lta])

    return fs


def compute_percent_coverage(
    stock_landings,
    fishstat,
    areas,
    isscaap_to_remove,
    assessment="Update",
    sn_key="ASFIS Scientific Name",
    landings_key="Stock Landings 2021",
    tier=None,
    extra_stocks_map={},
    year=2021,
    location_to_area={},
):
    if tier:
        if tier == "Missing":
            sl = stock_landings[stock_landings["Tier"].isna()]
        else:
            sl = stock_landings[stock_landings["Tier"] == tier]
    else:
        sl = stock_landings.copy()

    percent_coverage = {}

    # Remove ISSCAAP groups from Fishstat data for correct total landings per area
    fs = remove_isscaap_fishstat(
        fishstat, stock_landings, isscaap_to_remove, landings_key, year
    )

    for area in areas:
        coverage = sl[sl["Area"] == area][landings_key].sum()

        fs_area_mask = fs["Area"] == area

        # See if additional unassessed stocks need to be added to coverage
        extra_stocks_total = 0
        extra_stocks_added = []
        if assessment in extra_stocks_map and area in extra_stocks_map[assessment]:
            extra_stocks_tiers = extra_stocks_map[assessment][area]

            for t, extra_stocks in extra_stocks_tiers.items():
                if not tier or t == tier:
                    extra_stocks_mask = fs["ASFIS Scientific Name"].isin(extra_stocks)

                    extra_stocks_coverage = fs[extra_stocks_mask & fs_area_mask][
                        year
                    ].sum()

                    coverage += extra_stocks_coverage
                    extra_stocks_total += extra_stocks_coverage

                    extra_stocks_added += list(extra_stocks)

        # Check if tuna landings need to be added back into area
        tuna_total = 0
        for idx, row in sl[sl["Area"] == "Tuna"].iterrows():
            # Make sure not to double count stocks
            if row[sn_key] not in extra_stocks_added:
                tuna_areas = location_to_area["Tuna"][row["Location"]]

                tuna_mask = fs["ASFIS Scientific Name"] == row[sn_key]

                if area in tuna_areas:
                    tuna_coverage = fs[tuna_mask & fs_area_mask][year].sum()
                    coverage += tuna_coverage
                    tuna_total += tuna_coverage
                elif area == "48,58,88":  # Check for Tunas in areas 48,58,88
                    tuna_s_areas = [a for a in [48, 58, 88] if a in tuna_areas]
                    if tuna_s_areas:
                        tuna_s_mask = fs["Area"].isin(tuna_s_areas)
                        tuna_coverage = fs[tuna_mask & tuna_s_mask][year].sum()
                        coverage += tuna_coverage
                        tuna_total += tuna_coverage

        # Add salmon to Area 67
        if area == 67 and "Salmon" in sl["Area"].unique():
            salmon_coverage = sl[(sl["Area"] == "Salmon")][landings_key].sum()
            coverage += salmon_coverage

        # Check if sharks need to be added to area
        sharks_total = 0
        for idx, row in sl[sl["Area"] == "Sharks"].iterrows():
            # Make sure not to double count stocks
            if row[sn_key] not in extra_stocks_added:
                sharks_areas = location_to_area["Sharks"][row["Location"]]

                sharks_mask = fs["ASFIS Scientific Name"] == row[sn_key]

                if area in sharks_areas:
                    sharks_coverage = fs[sharks_mask & fs_area_mask][year].sum()
                    coverage += sharks_coverage
                    sharks_total += sharks_coverage
                elif area == "48,58,88":  # Check for sharks in areas 48,58,88
                    sharks_s_areas = [a for a in [48, 58, 88] if a in sharks_areas]
                    if sharks_s_areas:
                        sharks_s_mask = fs["Area"].isin(sharks_s_areas)
                        sharks_coverage = fs[tuna_mask & sharks_s_mask][year].sum()
                        coverage += sharks_coverage
                        sharks_total += sharks_coverage

        # Calculate area's total landings
        total_area_mask = (
            fs["Area"].isin([48, 58, 88]) if area == "48,58,88" else fs["Area"] == area
        )
        total_landings = fs[total_area_mask][year].sum()

        # Add to global total
        if "Global" not in percent_coverage:
            percent_coverage["Global"] = {}
            percent_coverage["Global"]["Coverage"] = coverage
            percent_coverage["Global"]["Total Landings"] = total_landings
        else:
            percent_coverage["Global"]["Coverage"] += coverage
            percent_coverage["Global"]["Total Landings"] += total_landings

        percent_coverage[area] = coverage / total_landings * 100

    percent_coverage["Global"] = (
        percent_coverage["Global"]["Coverage"]
        / percent_coverage["Global"]["Total Landings"]
        * 100
    )
    return pd.DataFrame(
        percent_coverage.items(), columns=["Area", f"Coverage (%) {assessment}"]
    )


def compute_percent_coverage_tiers(
    stock_landings,
    fishstat,
    areas,
    isscaap_to_remove,
    extra_stocks_map={},
    location_to_area={},
):
    tiers = [1, 2, 3, "Missing"]
    pc_tiers = []
    for tier in tiers:
        pc_tier = compute_percent_coverage(
            stock_landings,
            fishstat,
            areas,
            isscaap_to_remove,
            tier=tier,
            extra_stocks_map=extra_stocks_map,
            location_to_area=location_to_area,
        )
        rename_col = f"Tier {tier}" if isinstance(tier, int) else "No Tier"
        pc_tier = pc_tier.rename(columns={"Coverage (%) Update": rename_col})
        pc_tiers.append(pc_tier)

    pc_update = reduce(lambda left, right: pd.merge(left, right, on="Area"), pc_tiers)

    pc_update["Total"] = (
        pc_update["Tier 1"]
        + pc_update["Tier 2"]
        + pc_update["Tier 3"]
        + pc_update["No Tier"]
    )

    tuples = [("", "Area")] + [
        ("Coverage (%)", col) for col in pc_update.columns if col != "Area"
    ]
    pc_update.columns = pd.MultiIndex.from_tuples(tuples)

    return pc_update


def compare_weighted_percentages(previous, update, coverage_comparison):
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

    coverage_comparison = coverage_comparison[sorted(coverage_comparison.columns)]
    coverage_comparison.columns = pd.MultiIndex.from_tuples(
        [
            ("Area", ""),
            ("Previous SoSI Categories", "Coverage (%)"),
            ("Updated SoSI Categories", "Coverage (%)"),
        ]
    )

    merged_df = pd.merge(comparison_df, coverage_comparison, on=["Area"], how="inner")
    merged_df = merged_df[
        [
            ("Area", ""),
            ("Updated SoSI Categories", "Coverage (%)"),
            ("Updated SoSI Categories", "U (%)"),
            ("Updated SoSI Categories", "MSF (%)"),
            ("Updated SoSI Categories", "O (%)"),
            ("Updated SoSI Categories", "Sustainable (%)"),
            ("Updated SoSI Categories", "Unsustainable (%)"),
            ("Previous SoSI Categories", "Coverage (%)"),
            ("Previous SoSI Categories", "U (%)"),
            ("Previous SoSI Categories", "MSF (%)"),
            ("Previous SoSI Categories", "O (%)"),
            ("Previous SoSI Categories", "Sustainable (%)"),
            ("Previous SoSI Categories", "Unsustainable (%)"),
        ]
    ]

    return merged_df.set_index("Area")


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
