"""

"""

import os
import pandas as pd
import numpy as np
import json

# Change directory for import
import sys

sys.path.append(os.path.dirname(os.getcwd()))

from utils.aggregate_tables import *
from utils.stock_assessments import get_asfis_mappings, read_stock_data
from utils.species_landings import format_fishstat


def main():
    # Define directories for input and output files
    parent_dir = os.path.dirname(os.getcwd())
    input_dir = os.path.join(parent_dir, "input")
    clean_data_dir = os.path.join(parent_dir, os.path.join("output", "clean_data"))
    output_dir = os.path.join(parent_dir, os.path.join("output", "aggregate_tables"))
    
    os.make_dirs(output_dir, exist_ok=True)

    # Retrieve ASFIS mappings
    asfis_mapping = get_asfis_mappings(input_dir, "ASFIS_sp_2024.csv")
    asfis = asfis_mapping["ASFIS"]
    scientific_to_isscaap = asfis_mapping["ASFIS Scientific Name to ISSCAAP Code"]
    scientific_names = asfis_mapping["ASFIS Scientific Names"]

    # -- Tables based on number --
    print("Computing tables based on number...")
    # Retrieve stock lists (assessed/unassessed and only assessed stocks)
    stock_assessments_full = pd.read_excel(
        os.path.join(clean_data_dir, "stock_assessments_w_unassessed.xlsx")
    )
    stock_assessments = pd.read_excel(
        os.path.join(clean_data_dir, "stock_assessments.xlsx")
    )

    # Compute Status by Number grouped by Area and Tier
    sbn_area = compute_status_by_number(stock_assessments, "Area")
    sbn_tier = compute_status_by_number(stock_assessments, "Tier")

    sofia_indices = {
        "Area 21": (46, 0, 0),
        "Area27": (40, 0, 0),
        "Area 31": (51, 0, 0),
        "Area34": (71, 0, 0),
        "Area37": (60, 0, 0),
        "Area41": (62, 0, 0),
        "Area47": (44, 0, 0),
        "Area51": (52, 0, 0),
        "Area57": (64, 0, 0),
        "Area 61": (46, 0, 0),
        "Area67": (41, 0, 0),
        "Area71": (63, 0, 0),
        "Area77": (33, 0, 0),
        "area81v2": (38, 0, 0),
        "Area87": (31, 0, 0),
        "Tunas_HilarioISSF": (19, 0, 0),
    }

    sofia_sheets = sofia_indices.keys()
    sofia_sheet_to_area = {
        sheet: "".join([char for char in sheet if char.isdigit()])
        for sheet in sofia_sheets
    }
    sofia_sheet_to_area = {
        sheet: int(area) if area.isdigit() else area
        for sheet, area in sofia_sheet_to_area.items()
    }
    sofia_sheet_to_area["area81v2"] = 81
    sofia_sheet_to_area["Tunas_HilarioISSF"] = "Tuna"

    sofia_file_path = os.path.join(
        input_dir, "sofia2024v2Oct31woTunasFinalchcecksMarch2024.xlsx"
    )
    sbn_sofia_dict = read_stock_data(
        sofia_file_path, sofia_indices, desc="SOFIA Sheets"
    )

    # Reformat SOFIA status by number
    for sheet, df in sbn_sofia_dict.items():
        sbn_sofia_dict[sheet]["Area"] = sofia_sheet_to_area[sheet]
        sbn_sofia_dict[sheet] = df[
            ["Area", "Overfished", "Fully Fished ", "Under fished"]
        ]
        sbn_sofia_dict[sheet] = sbn_sofia_dict[sheet].rename(
            columns={
                "Overfished": "No. of O",
                "Fully Fished ": "No. of MSF",
                "Under fished": "No. of U",
            }
        )
        sbn_sofia_dict[sheet]["No. of Sustainable"] = (
            sbn_sofia_dict[sheet]["No. of U"] + sbn_sofia_dict[sheet]["No. of MSF"]
        )
        sbn_sofia_dict[sheet]["No. of Unsustainable"] = sbn_sofia_dict[sheet][
            "No. of O"
        ]
        sbn_sofia_dict[sheet]["No. of stocks"] = (
            sbn_sofia_dict[sheet]["No. of Sustainable"]
            + sbn_sofia_dict[sheet]["No. of Unsustainable"]
        )

    sbn_sofia = pd.DataFrame()

    for sheet, df in sbn_sofia_dict.items():
        if sbn_sofia.empty:
            sbn_sofia = df.copy()
        else:
            sbn_sofia = pd.concat([sbn_sofia, df])

    sbn_sofia = pd.concat(
        [sbn_sofia, pd.DataFrame({"Area": "Global"}, index=[len(sbn_sofia)])]
    )

    cols_to_sum = [
        "No. of stocks",
        "No. of U",
        "No. of MSF",
        "No. of O",
        "No. of Sustainable",
        "No. of Unsustainable",
    ]
    sbn_sofia.loc[sbn_sofia["Area"] == "Global", cols_to_sum] = (
        sbn_sofia[cols_to_sum].sum().values
    )

    pct_cols = []
    for col in cols_to_sum:
        sbn_sofia[col] = sbn_sofia[col].astype(int)
        if col != "No. of stocks":
            pct_col = col.replace("No. of ", "") + " (%)"
            pct_cols.append(pct_col)
            sbn_sofia[pct_col] = (sbn_sofia[col] / sbn_sofia["No. of stocks"]) * 100

    sbn_col_order = ["Area"] + cols_to_sum + pct_cols
    sbn_sofia = sbn_sofia[sbn_col_order]

    # Compare status by number for the two methods
    sbn_comp = compare_status_by_number(sbn_area, sbn_sofia)

    # Save status by number files
    sbn_area_fp = os.path.join(output_dir, "status_by_number_area.xlsx")
    sbn_area.to_excel(sbn_area_fp, index=False)
    round_excel_file(sbn_area_fp)

    sbn_tier_fp = os.path.join(output_dir, "status_by_number_tier.xlsx")
    sbn_tier.to_excel(sbn_tier_fp, index=False)
    round_excel_file(sbn_tier_fp)

    sbn_comp_fp = os.path.join(output_dir, "compare_status_by_number.xlsx")
    sbn_comp.to_excel(sbn_comp_fp)
    round_excel_file(sbn_comp_fp)

    # Create summary of stocks and save file
    sos = compute_summary_of_stocks(stock_assessments_full)
    sos_fp = os.path.join(output_dir, "summary_of_stocks.xlsx")
    sos.to_excel(sos_fp, index=False)

    # Save same aggregations for individual areas
    for area in stock_assessments["Area"].unique():
        sbna = sbn_area[sbn_area["Area"] == area]
        sbnt = compute_status_by_number(
            stock_assessments[stock_assessments["Area"] == area], "Tier"
        )
        sosa = compute_summary_of_stocks(
            stock_assessments_full[stock_assessments_full["Area"] == area]
        )
        cbn = sbn_comp[sbn_comp[("", "Area")] == area]

        area_summary_fp = os.path.join(
            output_dir, os.path.join("Area Statistics", f"area_{area}_summary.xlsx")
        )
        with pd.ExcelWriter(area_summary_fp) as writer:
            sbna.to_excel(writer, sheet_name="Status by Number")
            sbnt.to_excel(writer, sheet_name="Status by Tier")
            sosa.to_excel(writer, sheet_name="Summary of Stocks")

            if area in sbn_comp[("", "Area")].unique():
                cbn.to_excel(writer, sheet_name="Comparison by Number")

        round_excel_file(area_summary_fp)

    # -- Tables based on fishstat landings --
    print("Computing tables based on Fishstat data...")
    # Retrieve fishstat data from input folder
    fishstat = pd.read_csv(os.path.join(input_dir, "global_capture_production.csv"))

    # Format fishstat data
    mappings = get_asfis_mappings(input_dir, "ASFIS_sp_2024.csv")
    asfis = mappings["ASFIS"]
    code_to_scientific = dict(zip(asfis["Alpha3_Code"], asfis["Scientific_Name"]))

    fishstat = format_fishstat(fishstat, code_to_scientific)

    # Drop 2022 data from fishstat
    fishstat = fishstat.drop(columns=2022)

    # Compute status by number for top ten species globally and save file
    top_ten_species = [
        "Engraulis ringens",
        "Gadus chalcogrammus",
        "Katsuwonus pelamis",
        "Clupea harengus",
        "Thunnus albacares",
        "Micromesistius poutassou",
        "Sardina pilchardus",
        "Scomber japonicus",
        "Gadus morhua",
        "Sardinops sagax",
    ]
    sbn_top10_species = compute_species_status_by_number(
        stock_assessments, top_ten_species, fishstat
    )
    sbn_top10_fp = os.path.join(output_dir, "top10_species_status_by_number.xlsx")
    sbn_top10_species.to_excel(sbn_top10_fp)
    round_excel_file(sbn_top10_fp)

    # -- Tables based on species landings --
    print("Computing tables based on species landings...")
    # Build appendix landings tables
    # Retrieve species landings
    species_landings = pd.read_excel(
        os.path.join(clean_data_dir, "species_landings.xlsx")
    )

    # Define ISSCAAP Codes to remove from appendix
    # (unless they appear in assessment, then they are added back in)
    isscaap_to_remove = [46, 61, 62, 63, 64, 71, 72, 73, 74, 81, 82, 83, 91, 92, 93, 94]

    # Add ISSCAAP Code, ASFIS Name, Status, and Uncertainty to species landings
    primary_key = ["Area", "ASFIS Scientific Name", "Location"]
    species_landings = pd.merge(species_landings, stock_assessments, on=primary_key)

    # Add ISSCAAP Code to capture data
    fishstat["ISSCAAP Code"] = fishstat["ASFIS Scientific Name"].map(
        scientific_to_isscaap
    )

    # Retrieve aquaculture landings and reformat
    aquaculture = pd.read_csv(
        os.path.join(input_dir, "global_aquaculture_production.csv")
    )
    aquaculture = format_fishstat(aquaculture)
    aquaculture = aquaculture.rename(
        columns={
            "ASFIS species (Name)": "ASFIS Scientific Name",
            "ASFIS species (Code)": "ISSCAAP Code",
        }
    )
    aquaculture = aquaculture.drop(columns=2022)

    # Create ISSCAAP Group code to name map
    isscaap_code_to_name = dict(
        zip(asfis["ISSCAAP_Group_Code"], asfis["ISSCAAP_Group_Name_EN"])
    )

    # Retrieve country data and create ISO3 to name map
    country_codes = pd.read_excel(
        os.path.join(input_dir, "NOCS.xlsx"), sheet_name="Codes"
    )
    iso3_to_name = dict(zip(country_codes["ISO3"], country_codes["LIST NAME"]))
    iso3_to_name["EAZ"] = "Zanzibar"

    # Retrieve location to area mappings for Tuna, Deep Sea, Sharks
    with open(os.path.join(input_dir, "location_to_area.json"), "r") as file:
        location_to_area = json.load(file)

    # Compute appendix landings
    appendix_decs, appendix_years = compute_appendix_landings(
        species_landings,
        fishstat,
        aquaculture,
        isscaap_to_remove,
        isscaap_code_to_name,
        scientific_names,
        location_to_area,
        iso3_to_name,
    )

    # Save appendix landings files
    # By decade
    appendix_decs_fp = os.path.join(output_dir, "appendix_landings_decades.xlsx")

    with pd.ExcelWriter(appendix_decs_fp) as writer:
        for area, summary in appendix_decs.items():
            summary.to_excel(writer, sheet_name=str(area), index=False)

    round_excel_file(
        appendix_decs_fp,
        decimal_places=0,
        lt_one=True,
    )

    # By year
    appendix_years_fp = os.path.join(output_dir, "appendix_landings_years.xlsx")

    with pd.ExcelWriter(appendix_years_fp) as writer:
        for area, summary in appendix_years.items():
            summary.to_excel(writer, sheet_name=str(area), index=False)

    round_excel_file(
        appendix_years_fp,
        decimal_places=0,
        lt_one=True,
    )

    # -- Tables based on stock landings --
    print("Computing tables based on stock landings...")
    # Retrieve stock landings
    stock_landings = pd.read_excel(os.path.join(clean_data_dir, "stock_landings.xlsx"))

    # Merge with stock assessments for Status, Tier, etc.
    stock_landings = pd.merge(stock_landings, stock_assessments, on=primary_key)

    # Take out seals from stock landing aggregations since they are reported by number
    seals_mask = stock_landings["ISSCAAP Code"] == 63
    stock_landings = stock_landings[~seals_mask]

    # Compute percent coverage for updated assessment
    # Add unassessed stocks to Area 71 for coverage
    area71_extras = pd.read_excel(
        os.path.join(input_dir, "updated_assessment_overview.xlsx")
    )
    area71_extras = area71_extras.rename(
        columns={
            "More appropriate ASFIS Scientific Name": "Check",
            "Scientific name ASFIS": "ASFIS Scientific Name",
        }
    )
    area71_extras_mask = area71_extras["Check"] == "to ignore"
    area71_extras = area71_extras[area71_extras_mask]

    area71_tier1_mask = area71_extras["Tier"] == 1
    area71_no_tier_mask = area71_extras["Tier"].isna()

    extra_stocks_map = {
        "Update": {
            71: {
                "Tier 1": area71_extras[area71_tier1_mask][
                    "ASFIS Scientific Name"
                ].values,
                "Missing": area71_extras[area71_no_tier_mask][
                    "ASFIS Scientific Name"
                ].values,
            }
        }
    }
    areas = [
        area
        for area in stock_landings["Area"].unique()
        if isinstance(area, int) or area == "48,58,88"
    ]
    pc = compute_percent_coverage(
        stock_landings,
        fishstat,
        areas,
        assessment="Update",
        location_to_area=location_to_area,
        extra_stocks_map=extra_stocks_map,
    )

    # Compute percent coverage across tiers
    pc_tiers = compute_percent_coverage_tiers(
        stock_landings,
        fishstat,
        areas,
        extra_stocks_map=extra_stocks_map,
        location_to_area=location_to_area,
    )
    # Retrieve SOFIA with landings
    sofia_landings = pd.read_excel(os.path.join(clean_data_dir, "sofia_landings.xlsx"))
    sofia_landings.loc[sofia_landings["Area"].isin([48, 58, 88]), "Area"] = "48,58,88"

    # Compute percent coverage for SOFIA data
    pc_sofia = compute_percent_coverage(
        sofia_landings,
        fishstat,
        areas,
        assessment="Previous",
        key="Proxy",
        landings_key=2021,
        location_to_area=location_to_area,
    )

    # Compute and save the comparison of percent coverage
    pc_comp = pd.merge(pc_sofia, pc, on="Area")

    # Add footnote to table describing process of computation
    pc_footnote = (
        "NOTE: Percent coverages are performed across FAO major fishing areas to be consistent with Fishstatj. \n"
        + "Thus, landings from areas such as 'Salmon', 'Tuna', 'Deep Sea', and 'Sharks' are added back into the FAO major fishing area \n"
        + "from where they were reported."
    )

    pc_comp_fp = os.path.join(output_dir, "percent_coverage_comparison.xlsx")
    add_footnote(pc_comp, pc_footnote).to_excel(pc_comp_fp, index=False)
    round_excel_file(pc_comp_fp)

    # Save percent coverage across tiers
    pc_tiers_fp = os.path.join(output_dir, "percent_coverage_tiers.xlsx")
    add_footnote(pc_tiers, pc_footnote, multi_index=True).to_excel(pc_tiers_fp)
    round_excel_file(pc_tiers_fp)

    # Compute weighted percentages with and w/o Tunas category
    wp_area = compute_weighted_percentages(stock_landings)
    wp_wo_tuna = compute_weighted_percentages(
        stock_landings,
        fishstat=fishstat,
        tuna_location_to_area=location_to_area["Tuna"],
    )

    # Compute weighted percentages for SOFIA
    # Retrieve SOFIA assessments with landings
    sofia_landings = pd.read_excel(os.path.join(clean_data_dir, "sofia_landings.xlsx"))

    # Set stocks in Areas 48,58,88 to Area 48,58,88
    mask_485888 = sofia_landings["Area"].isin([48, 58, 88])
    sofia_landings.loc[mask_485888, "Area"] = "48,58,88"

    # Get assessed stocks from SOFIA data
    sofia_assessed_mask = sofia_landings["Status"].isin(["U", "F", "O"])
    sofia_landings_assessed = sofia_landings[sofia_assessed_mask]
    sofia_landings_assessed["Status"] = sofia_landings_assessed["Status"].apply(
        lambda x: {"F": "M"}.get(x, x)
    )
    sofia_landings_assessed = sofia_landings_assessed.rename(
        columns={2021: "Stock Landings 2021"}
    )
    sofia_landings_assessed = sofia_landings_assessed[
        ["Area", "ASFIS Scientific Name", "Status", "Stock Landings 2021"]
    ]

    wp_sofia = compute_weighted_percentages(sofia_landings_assessed)

    # Compare the weighted percentages for the two assessments
    wp_comp = compare_weighted_percentages(wp_sofia, wp_area, pc_comp)

    # Save the updated weighted percentages w/Tuna area separate
    wp_area_fp = os.path.join(output_dir, "status_by_landings_area.xlsx")
    wp_area.to_excel(wp_area_fp)
    round_excel_file(wp_area_fp)

    # Save the updated weighted percentages w/o Tuna separate
    wp_wo_tuna_fp = os.path.join(output_dir, "status_by_landings_area_wo_tuna.xlsx")
    wp_wo_tuna.to_excel(wp_wo_tuna_fp)
    round_excel_file(wp_wo_tuna_fp)

    # Save the comparison by landings of updated and previous method
    wp_comp_fp = os.path.join(output_dir, "comparison_by_landings.xlsx")
    # Add percent coverage footnote
    add_footnote(wp_comp, pc_footnote, multi_index=True).to_excel(wp_comp_fp)
    round_excel_file(wp_comp_fp)

    # Compute weighted percentages across tiers
    wp_tiers = compute_weighted_percentages(stock_landings, key="Tier")

    # Save weighted percentages across tiers
    wp_tiers_fp = os.path.join(output_dir, "status_by_landings_tier.xlsx")
    wp_tiers.to_excel(wp_tiers_fp)
    round_excel_file(wp_tiers_fp)

    # Get weighted percentages and total landings
    wp_totl = get_weighted_percentages_and_total_landings(wp_area, appendix_decs)

    # Save wp w/totals w/footnote explaining why assessed landings / total landings * 100
    # per area does not correspond to the percent coverage (doesn't account for addition of Tuna landings, etc.)
    sbl_footnote = (
        "Note: Percent coverage in this sheet (Total Assessed Landings / Total Landings * 100) does not reflect reported percent coverage. \n"
        + "For percent coverage, the landings of 'Salmon', 'Tuna', 'Deep Sea' and 'Sharks' are incorporated in the FAO major fishing areas \n"
        + "from which their landings are reported. Thus, percent coverage calculated from this table will appear lower than reported elsewhere."
    )
    wp_totl_fp = os.path.join(output_dir, "status_by_landings_w_totals_area.xlsx")
    add_footnote(wp_totl, sbl_footnote, multi_index=True).to_excel(wp_totl_fp)
    round_excel_file(wp_totl_fp)

    # Get same but w/o tunas as separate area
    wp_totl_wo_tuna = get_weighted_percentages_and_total_landings(
        wp_wo_tuna, appendix_decs
    )
    wp_totl_wo_tuna_fp = os.path.join(
        output_dir, "status_by_landings_w_totals_wo_tuna.xlsx"
    )
    add_footnote(wp_totl_wo_tuna, sbl_footnote, multi_index=True).to_excel(
        wp_totl_wo_tuna_fp
    )
    round_excel_file(wp_totl_wo_tuna_fp)

    # Compute weighted percentages across tiers per area
    wp_tiers_area = get_weighted_percentages_by_tier_and_area(stock_landings, wp_totl)

    # Save
    wp_tiers_area_fp = os.path.join(output_dir, "status_by_landings_tier_and_area.xlsx")
    wp_tiers_area.to_excel(wp_tiers_area_fp)
    round_excel_file(wp_tiers_area_fp)

    # Save aggregations for areas individually
    for area in stock_landings["Area"].unique():
        wp = wp_area.loc[area]
        wpt = compute_weighted_percentages(
            stock_landings[stock_landings["Area"] == area], "Tier"
        )
        cbl = wp_comp.loc[area] if area in wp_comp.index else None

        area_summary_fp = os.path.join(
            output_dir, os.path.join("Area Statistics", f"area_{area}_summary.xlsx")
        )
        with pd.ExcelWriter(
            area_summary_fp,
            engine="openpyxl",
            mode="a",
            if_sheet_exists="replace",
        ) as writer:
            wp.to_excel(writer, sheet_name="Status by Landings (Area)")
            wpt.to_excel(writer, sheet_name="Status by Landings (Tier)")

            if area in wp_comp.index:
                cbl.to_excel(writer, sheet_name="Comparison by Landings")

        round_excel_file(area_summary_fp)

    # Compute weighted percentages for top ten species globally
    wp_top10 = compute_species_weighted_percentages(stock_landings, top_ten_species)

    # Save
    wp_top10_fp = os.path.join(output_dir, "weight_percentages_top10species.xlsx")
    wp_top10.to_excel(wp_top10_fp)
    round_excel_file(wp_top10_fp)

    print(f"All files saved to {output_dir}")


if __name__ == "__main__":
    main()
