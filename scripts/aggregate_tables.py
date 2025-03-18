"""

"""

import os
import pandas as pd
import numpy as np
import json

from utils.aggregate_tables import *
from utils.stock_assessments import get_asfis_mappings, read_stock_data
from utils.species_landings import format_fishstat


def main():
    # Define directories for input and output files
    if os.path.basename(os.getcwd()) == "SOSI-2025":
        parent_dir = os.getcwd()
    elif os.path.basename(os.path.dirname(os.getcwd())) == "SOSI-2025":
        parent_dir = os.path.dirname(os.getcwd())
    else:
        raise FileNotFoundError("SOSI-2025 folder could not be found.")

    input_dir = os.path.join(parent_dir, "input")
    clean_data_dir = os.path.join(parent_dir, os.path.join("output", "clean_data"))
    output_dir = os.path.join(parent_dir, os.path.join("output", "aggregate_tables"))

    os.makedirs(output_dir, exist_ok=True)

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
    
    vc_tier_area = compute_count_for_group(stock_assessments, group_col="Area", count_col="Tier")

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
    
    # Save value counts for tier across areas
    vc_tier_area_fp = os.path.join(output_dir, "value_counts_tier_by_area.xlsx")
    vc_tier_area.to_excel(vc_tier_area_fp)

    # Create summary of stocks and save file
    sos = compute_summary_of_stocks(stock_assessments_full)
    sos_fp = os.path.join(output_dir, "summary_of_stocks.xlsx")
    sos.to_excel(sos_fp)

    # Save same aggregations for individual areas
    area_summary_dir = os.path.join(output_dir, "Area Statistics")
    os.makedirs(area_summary_dir, exist_ok=True)

    for area in stock_assessments["Area"].unique():
        sbna = sbn_area[sbn_area["Area"] == area]
        sbnt = compute_status_by_number(
            stock_assessments[stock_assessments["Area"] == area], "Tier"
        )
        sosa = compute_summary_of_stocks(
            stock_assessments_full[stock_assessments_full["Area"] == area]
        )
        cbn = sbn_comp[sbn_comp[("", "Area")] == area]

        area_summary_fp = os.path.join(area_summary_dir, f"area_{area}_summary.xlsx")

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

    # Only keep data from FAO major fishing areas in analysis
    numerical_areas = [
        area for area in stock_assessments["Area"].unique() if isinstance(area, int)
    ] + [48, 58, 88]

    fishstat_area_mask = fishstat["Area"].isin(numerical_areas)
    fishstat = fishstat[fishstat_area_mask]

    # Add ISSCAAP Code to capture data
    fishstat["ISSCAAP Code"] = fishstat["ASFIS Scientific Name"].map(
        scientific_to_isscaap
    )
    
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

    # Add ISSCAAP Code, ASFIS Name, Status, and Uncertainty to species landings
    primary_key = ["Area", "ASFIS Scientific Name", "Location"]
    species_landings = pd.merge(species_landings, stock_assessments, on=primary_key)

    # Take out seals since they are reported by number
    no_seals_mask = ~(species_landings["ISSCAAP Code"] == 63)
    species_landings = species_landings[no_seals_mask]

    # Retrieve aquaculture landings and reformat
    aquaculture = pd.read_csv(
        os.path.join(input_dir, "global_aquaculture_production.csv")
    )
    aquaculture = format_fishstat(aquaculture)
    aquaculture = aquaculture.rename(
        columns={
            "ASFIS species (Name)": "ASFIS Name",
            "ASFIS species (Code)": "ISSCAAP Code",
            "ASFIS species (Scientific name)": "ASFIS Scientific Name"
        }
    )
    aquaculture = aquaculture.drop(columns=2022)

    # Only keep data for relevant areas
    ac_area_mask = aquaculture["Area"].isin(numerical_areas)

    aquaculture = aquaculture[ac_area_mask]

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

    # Define ISSCAAP Codes to remove from appendix landings,
    # and later the percent coverage calculations as well.
    # (unless they appear in assessment, then they are added back in to total area landings)
    isscaap_to_remove = [61, 62, 63, 64, 71, 72, 73, 74, 81, 82, 83, 91, 92, 93, 94]
    
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
    
    cols = []
    for tier in [1,2,3]:
        cols += [f"Tier {tier} Status", f"Tier {tier} Uncertainty"]
    
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
    
    # Define areas for percent coverage reporting
    areas = [
        area
        for area in stock_landings["Area"].unique()
        if isinstance(area, int) or area == "48,58,88"
    ]
    
    # Get area specific shark and deep sea landings for percent coverage calculations
    stock_weights = pd.read_excel(os.path.join(clean_data_dir, "stock_weights.xlsx"))
    
    shark_area_landings = compute_sg_area_landings(stock_weights, species_landings, "Sharks", location_to_area)
    
    pc = compute_percent_coverage(
        stock_landings,
        fishstat,
        areas,
        isscaap_to_remove,
        assessment="Update",
        location_to_area=location_to_area,
        shark_area_landings=shark_area_landings,
    )

    # Compute percent coverage across tiers
    pc_tiers = compute_percent_coverage_tiers(
        stock_landings,
        fishstat,
        areas,
        isscaap_to_remove,
        location_to_area=location_to_area,
        shark_area_landings=shark_area_landings,
    )
    # Retrieve SOFIA with landings
    sofia_landings = pd.read_excel(os.path.join(clean_data_dir, "sofia_landings.xlsx"))

    # Combine areas 48,58,88
    sofia_southern_mask = sofia_landings["Area"].isin([48, 58, 88])
    sofia_landings.loc[sofia_southern_mask, "Area"] = "48,58,88"

    # Add ISSCAAP Code to SOFIA data
    sofia_landings["ISSCAAP Code"] = sofia_landings["Proxy"].map(scientific_to_isscaap)

    # Compute percent coverage for SOFIA data
    pc_sofia = compute_percent_coverage(
        sofia_landings,
        fishstat,
        areas,
        isscaap_to_remove,
        assessment="Previous",
        sn_key="Proxy",
        landings_key=2021,
        location_to_area=location_to_area,
    )

    # Compute and save the comparison of percent coverage
    pc_comp = pd.merge(pc_sofia, pc, on="Area")

    # Add footnote to table describing process of computation
    pc_footnote = (
        "NOTE: Percent coverages are performed across FAO major fishing areas to be consistent with Fishstatj. \n"
        + "Thus, landings from areas such as 'Salmon', 'Tuna', 'Deep Sea', and 'Sharks' are added back into the FAO major fishing area from where they were reported. \n"
        + f"Percent coverage calculations do not include landings from ISSCAAP codes {", ".join([str(i) for i in isscaap_to_remove])}, \n"
        + "except for stocks from these groups which are included in the assessment."
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
        location_to_area={"Tuna": location_to_area["Tuna"]},
    )   
    
    wp_wo_sg = compute_weighted_percentages(
        stock_landings,
        fishstat=fishstat,
        location_to_area={"Tuna": location_to_area["Tuna"]},
        add_salmon=True,
        shark_area_landings=shark_area_landings,
    )
    
    # Compute weighted percentages for SOFIA
    # Get assessed stocks from SOFIA data
    sofia_landings_assessed = sofia_landings.rename(columns={"Status": "Status Old"})
    sofia_landings_assessed["Status"] = sofia_landings_assessed["Status Old"].apply(
        lambda x: {"F": "M"}.get(x, x)
    )
    sofia_assessed_mask = sofia_landings_assessed["Status"].isin(["U", "M", "O"])
    sofia_landings_assessed = sofia_landings_assessed[sofia_assessed_mask]

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
    sbl_footnote1 = (
        "Note: Percent coverage in this sheet does not reflect reported percent coverage. For the reported percent coverage, \n"
        + "the landings of 'Deep Sea', 'Salmon', 'Tuna', and 'Sharks' are incorporated in the FAO major fishing areas \n"
        + "from which their landings are reported. Thus, percent coverage calculated from this table will slightly different than reported elsewhere. \n"
        + f"Area landings exclude landings from ISSCAAP codes {", ".join([str(i) for i in isscaap_to_remove])}, \n"
        + "except for stocks which have been incorporated in assessment."
    )
    wp_totl_fp = os.path.join(output_dir, "status_by_landings_w_totals_area.xlsx")
    add_footnote(wp_totl, sbl_footnote1, multi_index=True).to_excel(wp_totl_fp)
    round_excel_file(wp_totl_fp)

    # Get same but w/o tunas as separate area
    # Compute tuna landings to add back into total landings for each area
    tuna_mask = stock_assessments["Area"] == "Tuna"
    tuna_list = stock_assessments[tuna_mask]["ASFIS Scientific Name"].unique()
    fs_tuna_mask = fishstat["ASFIS Scientific Name"].isin(tuna_list)
    fs_tuna = fishstat[fs_tuna_mask].groupby("Area")[2021].sum().reset_index()
    
    sbl_footnote2 = (
        "Note: Percent coverage in this sheet does not reflect reported percent coverage. For the reported percent coverage, \n"
        + "the landings of 'Deep Sea', 'Salmon', and 'Sharks' are incorporated in the FAO major fishing areas \n"
        + "from which their landings are reported. Thus, percent coverage calculated from this table will slightly different than reported elsewhere. \n"
        + f"Area landings exclude landings from ISSCAAP codes {", ".join([str(i) for i in isscaap_to_remove])}, \n"
        + "except for stocks which have been incorporated in assessment. \n"
        + "Tuna status/landings have been incorporated into FAO area weighted percentages, so these will appear different \n"
        + "compared to tables with Tuna category separated."
    )
    wp_totl_wo_tuna = get_weighted_percentages_and_total_landings(
        wp_wo_tuna, appendix_decs, tuna_landings=fs_tuna
    )
    wp_totl_wo_tuna_fp = os.path.join(
        output_dir, "status_by_landings_w_totals_wo_tuna.xlsx"
    )
    add_footnote(wp_totl_wo_tuna, sbl_footnote2, multi_index=True).to_excel(
        wp_totl_wo_tuna_fp
    )
    round_excel_file(wp_totl_wo_tuna_fp)
    
    # Get the same but with special groups (only numerical areas)
    # Use fishstat directly to compute total landings in area
    sbl_footnote3 = ( 
        f"Note: Area landings exclude landings from ISSCAAP codes {", ".join([str(i) for i in isscaap_to_remove])}, \n"
        + "except for stocks which have been incorporated in assessment. \n"
        + "'Deep Sea', 'Salmon', 'Sharks', and 'Tuna' status/landings have been incorporated into FAO area weighted percentages, \n"
        + "so these will appear different compared to tables with 'Deep Sea', 'Salmon', 'Sharks', and 'Tuna' categories separated."
    )
    wp_totl_wo_sg = get_weighted_percentages_and_total_landings(
        wp_wo_sg,
        fishstat=fishstat,
        isscaap_to_remove=isscaap_to_remove,
        areas=numerical_areas,
        special_groups=False
    )
    wp_totl_wo_sg_fp = os.path.join(
        output_dir, "status_by_landings_w_totals_fao_areas.xlsx"
    )
    add_footnote(wp_totl_wo_sg, sbl_footnote3, multi_index=True).to_excel(
        wp_totl_wo_sg_fp
    )
    round_excel_file(wp_totl_wo_sg_fp)
    

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

