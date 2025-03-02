"""
This file creates full table of all assessed stocks (and unassessed stocks)

Inputs
    - ./input/updated_assessment_overview.xlsx: collection of spreadsheets of stock assessments for each area
    - ./input/ASFIS_sp_2024.csv: list of ASFIS species in 2024
    - ./input/ASFIS_changes_2024.xlsx: list of name updates for ASFIS species in 2024
    - ./input/deep_sea_name_map.json: mapping from name in data to ASFIS Scientific Name for Deep Sea sheet
    - ./input/corrected_scientific_names.json: mapping of corrected ASFIS Scientific Names for the report
    - ./input/overview_2025-02-04.xlsx: a corrected stock assessments sheet with changes made to Angolan stocks in Area 47

Outputs
    - ./output/clean_data/stock_assessments_w_unassessed.xlsx: complete list of assessed and unassessed stocks from sheets
    - ./output/clean_data/stock_assessments.xlsx: complete list of assessed stocks (Status is 'U', 'M' or 'O')

Output schema (primary key = [Area, ASFIS Scientific Name, Location]):
    - Area: The group of stocks which are found in separate sheets from input
        Most of the time, this is an FAO major fishing area (21, 27, etc.)
        However, this can include other types of aggregations, such as 
        Salmon, Tuna, Deep Sea, and Sharks.
    - ISSCAAP Code: ISSCAAP Code corresponding to the corresponding to the species of the stock
    - ASFIS Name: The current ASFIS common name corresponding to the species of the stock
    - ASFIS Scientific Name: The current ASFIS Scientific Name pertaining to the species of the stock
    - Location: The reported location of the stock
    - Tier: The tier of the assessment (1, 2, or 3)
    - Status: The status of the assessment, standardized to be U, M, O, or NaN 
            (Underfished, Maximally Sustainably Fished, Overfished, or Unknown, resp.)
    - Uncertainty: The uncertainty of the assessment, standardized to be L, M, or H (low, medium, high, resp.)
"""

# Silence the Pandas future warnings on output
import warnings
warnings.simplefilter(action="ignore", category=FutureWarning)

import os
import pandas as pd
import numpy as np
import json

# Change directory for import
import sys
sys.path.append(os.path.dirname(os.getcwd()))

from utils.data_cleanup_and_validation import *


def main():
    # Define directories for input and output files
    parent_dir = os.path.dirname(os.getcwd())
    input_dir = os.path.join(parent_dir, "input")
    output_dir = os.path.join(parent_dir, os.path.join("output", "clean_data"))

    # Read in the ASFIS mappings (between common name, scientific name, and ISSCAAP group)
    print("Creating mappings...")
    mappings = get_asfis_mappings(
        input_dir, "ASFIS_sp_2024.csv", "ASFIS_changes_2024.xlsx"
    )
    
    scientific_update = mappings["ASFIS Scientific Name Update"]
    scientific_to_name = mappings["ASFIS Scientific Name to ASFIS Name"]
    scientific_to_isscaap = mappings["ASFIS Scientific Name to ISSCAAP Code"]
    scientific_names = mappings["ASFIS Scientific Names"]

    # Read in Deep Sea ASFIS Name to Scientific Name mapping
    # This is for the names which can't be mapped (based on misspellings and such)
    with open(os.path.join(input_dir, "deep_sea_name_map.json"), "r") as file:
        deep_sea_name_map = json.load(file)

    # Read in original data
    # Define the indices to be kept within each sheet
    # NOTE: Indices are (skiprows, start, end)
    # The start, end indices to be passed within function
    # will be two less than those listed in excel
    # skiprows indicates how many rows to skip before finding header
    # e.g. sheet "87" has data from lines 11-109 and header in first row in excel,
    # so we list indices (0, 9, 107) in dictionary to pass in function
    # To start from beginning of sheet, simply pass 0 as first index
    sheet_indices = {
        "21": (0, 0, 171),
        "27": (0, 0, 206),
        "Area31Jeremy": (0, 0, 118),
        "Area34Luca": (0, 0, 152),
        "37": (0, 0, 128),
        "41": (0, 0, 81),
        "47": (0, 0, 82),
        "51fromAbwoTuna": (0, 0, 503),
        "57": (0, 0, 312),
        "61": (0, 0, 97),
        "67": (0, 0, 200),
        "Area67woSalmon": (0, 0, 120),
        "Area67SalmonRishiguessSalmononX": (0, 0, 84),
        "71fromABwoTuna": (0, 0, 297),
        "77": (0, 0, 96),
        "81": (0, 0, 167),
        "87": (0, 9, 107),
        "48,58,88": (0, 0, 24),
        "Tuna": (0, 0, 22),
        "Sharks": (1, 0, 45),
        "Deep Sea": (0, 0, 92),
    }
    print("Reading in stock assessments...")
    overview = read_stock_data(
        os.path.join(input_dir, "updated_assessment_overview.xlsx"), sheet_indices
    )

    print("Standardizing data...")
    # Standardize the column names
    col_names = [
        "Area",
        "ASFIS Scientific Name",
        "Location",
        "Tier",
        "ISSCAAP Code",
        "Uncertainty",
        "ASFIS Name",
        "Status",
    ]
    area, sn, loc, tier, code, unc, name, st = col_names
    columns_map = {
        "27": {"Code": code, "Name in Data": name},
        "Area31Jeremy": {
            "Code": code,
            "Uncertainty (H,L,M)": unc,
        },
        "Area34Luca": {
            "Scientific name": sn,
            "Location - in online document": loc,
            "Tier (3 levels)": tier,
            "ISSCAAP Group Code": code,
            "ASFIS Stocks \nCommon name (species name)": name,
            "Uncertainty (H,L,M)": unc,
            "Status (3 levels)": st,
        },
        "37": {
            "Scientific name": sn,
            "Tier (3 levels)": tier,
            "ISSCAAP Group Code": code,
            "ASFIS Stocks \nCommon name (species name)": name,
            "Uncertainty (H,L,M) \nsee below": unc,
            "Status (3 levels)": st,
        },
        "41": {
            "Scientific Name": sn,
            "Code": code,
            "Uncertainty (H,L,M) \nsee below": unc,
        },
        "47": {
            "Scientific Name": sn,
            "Country": loc,
            "ISSCAAP Group Code": code,
            "Uncertainty\n(high, medium, low)": unc,
        },
        "51fromAbwoTuna": {
            "Scientific name in Data": sn,
            "Tier (3 levels)": tier,
            "ISSCAAP Group Code": code,
            "Uncertainty (High,Medium, Low)": unc,
            "Status (3 levels)": st,
        },
        "57": {"Scientific Name": sn},
        "61": {
            "Tier (3 levels)": tier,
            "Code": code,
            "Uncertainty (H, M, L)": unc,
            "Assessment": st,
        },
        "Area67SalmonRishiguessSalmononX": {"Code": code},
        "Area67woSalmon": {
            "Unnamed: 2": code,
            "Unnamed: 5": name,
            "Unnamed: 6": sn,
            "Unnamed: 7": loc,
            "Unnamed: 8": tier,
            "Unnamed: 0": "org lin no",
        },
        "67": {"Code": code},
        "71fromABwoTuna": {
            "Scientific name ASFIS": sn,
            "Code": code,
            "Common name ASFIS Name": name,
        },
        "77": {"Species": sn, "Stocks": name},
        "81": {
            "Scientific name": sn,
            "Location (stock)": loc,
            "Tier (3 levels)": tier,
            "ISSCAAP Group Code": code,
            "ASFIS Stocks \nCommon name (species name)": name,
            "Uncertainty (H,L,M)": unc,
            "Status (3 levels)": st,
        },
        "87": {
            "Scientific name": sn,
            "Tier (3 levels)": tier,
            "ISSCAAP Group Code": code,
            "Name in data": name,
            "Uncertainty (H,L,M) \nsee below": unc,
        },
        "48,58,88": {
            "ASFIS Scientific name": sn,
            "Tier (3 levels)": tier,
            "ISSCAAP Group Code": code,
            "ASFIS Stocks \nCommon name (species name)": name,
            "Uncertainty (H,L,M) \nsee below": unc,
            "Status (3 levels)": st,
            "FAO Fishing Area": area,
        },
        "Sharks": {
            "Species": name,
            "Scientific Name": sn,
            "Area number": loc,
            "Status": "Original Status",
            "status": st,
            "  (H,M,L)": unc,
            "Area": "FAO Area",
        },
        "Deep Sea": {
            "StatUnks": st,
            "Species": name,
            "Tier\xa0": tier,
            "Resource": loc,
        },
    }

    overview = standardize_columns(overview, columns_map)

    # Fill missing data for certain columns with a proxy column
    overview["27"]["ASFIS Name"] = fill_col_na(
        overview["27"], "ASFIS Name if different", "ASFIS Name"
    )
    overview["51fromAbwoTuna"]["ASFIS Scientific Name"] = fill_col_na(
        overview["51fromAbwoTuna"],
        "ASFIS Scientific Name 2024",
        "ASFIS Scientific Name",
    )

    # Match uncertainties from Area 67 to Area 67 wo Salmon
    overview["Area67woSalmon"] = add_column_from_merge(
        overview["Area67woSalmon"], overview["67"], "org lin no", "Uncertainty"
    )

    # Set Scientific Name to More Appropriate ASFIS Scientific Name column if present in sheet
    # Flags status with (to ignore) if More Appropriate Scientific Name is 'to ignore'
    # Rest of params are default in function definition
    overview = use_more_appropriate_scientific_name(overview)

    # Drop More Appropriate Scientific Name column if present in sheet
    overview = drop_cols(overview, ["More Appropriate Scientific Name"])

    # Add Area column to sheets
    sheet_to_area = {
        "Area31Jeremy": 31,
        "Area34Luca": 34,
        "51fromAbwoTuna": 51,
        "Area67woSalmon": 67,
        "Area67SalmonRishiguessSalmononX": "Salmon",
        "71fromABwoTuna": 71,
    }
    overview = add_area_column(overview, sheet_to_area)

    # We begin data correction phase
    # This includes any operation which augments or removes the values
    # contained in the raw data

    print("Correcting data...")

    # Clean up Deep Sea dataframe
    # Forward fill the Region column
    overview["Deep Sea"] = ffill_columns(overview["Deep Sea"], "Region")
    # Rows without ASFIS Name are blanks in original sheet
    overview["Deep Sea"] = (
        overview["Deep Sea"].dropna(subset=["ASFIS Name"]).reset_index(drop=True)
    )
    # Fix Location
    overview["Deep Sea"]["Location"] = fill_col_na(
        overview["Deep Sea"], "Location", "Region"
    )
    # Values for Status, Uncertainty, and Tier are spread out over two columns
    # based on how original sheet was formatted
    overview["Deep Sea"]["Status"] = fill_col_na(
        overview["Deep Sea"], "Unnamed: 3", "Status"
    )
    overview["Deep Sea"]["Uncertainty"] = fill_col_na(
        overview["Deep Sea"], "Unnamed: 5", "Uncertainty"
    )
    overview["Deep Sea"]["Tier"] = fill_col_na(
        overview["Deep Sea"], "Unnamed: 7", "Tier"
    )
    # Use the ASFIS Names in the original dataset to find the ASFIS Scientific Name
    overview["Deep Sea"]["ASFIS Scientific Name"] = overview["Deep Sea"][
        "ASFIS Name"
    ].map(lambda x: deep_sea_name_map.get(x, x))
    name_to_scientific = {
        name.lower(): sn
        for sn, name in scientific_to_name.items()
        if isinstance(name, str)
    }
    deep_sea_sn = overview["Deep Sea"]["ASFIS Name"].map(name_to_scientific)
    overview["Deep Sea"]["ASFIS Scientific Name"] = overview["Deep Sea"][
        "ASFIS Scientific Name"
    ].fillna(deep_sea_sn)

    # Retrieve ASFIS Names based on Scientific Name
    overview["Deep Sea"]["ASFIS Name"] = (
        overview["Deep Sea"]["ASFIS Scientific Name"]
        .apply(get_common_name, args=(scientific_to_name,))
        .fillna(overview["Deep Sea"]["ASFIS Name"])
    )
    # Retrieve ISSCAAP Code
    overview["Deep Sea"]["ISSCAAP Code"] = overview["Deep Sea"][
        "ASFIS Scientific Name"
    ].map(scientific_to_isscaap)
    # Ensure this stock is kept in final dataset even though uncertainty is high
    overview["Deep Sea"].loc[
        overview["Deep Sea"]["ASFIS Name"] == "Neon flying squid",
        ["Status", "Uncertainty", "Tier"],
    ] = ["O", "H", 2]

    # Clean up Sharks dataframe
    overview["Sharks"] = overview["Sharks"].dropna(
        subset=["ASFIS Scientific Name", "Status"]
    )

    # Helper function to make the shark location from the columns Ocean and Area
    def make_shark_loc(row):
        ocean, area = row["Ocean"], row["FAO Area"]

        if isinstance(area, float) and np.isnan(area):
            return ocean

        if ocean in area:
            return area

        return ocean + " " + area

    overview["Sharks"]["Location"] = overview["Sharks"][["Ocean", "FAO Area"]].apply(
        make_shark_loc, axis=1
    )
    # All Shark assessments are tier 1 and ISSCAAP Code 38
    overview["Sharks"]["Tier"] = 1
    overview["Sharks"]["ISSCAAP Code"] = 38
    overview["Sharks"]["Status"] = overview["Sharks"]["Status"].map(
        lambda x: x[0].upper()
    )

    # Correct the scientific names, common names, and/or ISSCAAP codes for the duplicated entries.
    # The keys of the dictionary are sheet, column to update, scientific name of updated row(s)
    correct_values_dict = {
        "51fromAbwoTuna": {
            "ASFIS Scientific Name": {
                "Tachysuridae": "Ariidae",
                "Thryssa sp": "Engraulidae",
            },
            "ISSCAAP Code": {"Ariidae": 33},
        },
        "57": {"ASFIS Name": {"Z_Aggregate group squids": "Squids"}},
        "71fromABwoTuna": {"ASFIS Name": {"Octopus spp.": "Octopuses nei"}},
        "Deep Sea": {
            "ASFIS Scientific Name": {
                "Pentaceros Richardsoni": "Pentaceros wheeleri",
                "Sebastes spp": "Sebastes mentella, Sebastes fasciatus",
            },
            "ASFIS Name": {"pelagic armourhead": "Slender armourhead"},
        },
    }
    overview = correct_values(overview, correct_values_dict, "ASFIS Scientific Name")

    # Update scientific name to 2024 version if appears in mapping
    overview = update_values(
        overview, scientific_update, update_col="ASFIS Scientific Name"
    )

    # Use common name as scientific name for stocks with missing scientific name
    for sheet, df in overview.items():
        overview[sheet]["ASFIS Scientific Name"] = fill_col_na(
            df, "ASFIS Scientific Name", "ASFIS Name"
        )

    # Standardize Common Name and ISSCAAP Code. We map Scientific Name to Common Name and ISSCAAP Code and
    # fill the missing values with the original data from the column.
    cols_w_map = {
        "ASFIS Name": scientific_to_name,
        "ISSCAAP Code": scientific_to_isscaap,
    }
    overview = standardize_column_values(
        overview,
        cols_w_map,
        key="ASFIS Scientific Name",
    )

    # Map MSF and S to M for Status
    overview = update_values(overview, {"MSF": "M", "S": "M"}, update_col="Status")

    # Remove Tunas present in Areas 31, 51, 71
    tuna_names = overview["Tuna"]["ASFIS Name"].unique()
    remove_dict = {
        "Area31Jeremy": {"ASFIS Name": tuna_names},
        "51fromAbwoTuna": {"ASFIS Name": tuna_names},
        "71fromABwoTuna": {"ASFIS Name": tuna_names},
    }
    overview = remove_values(overview, remove_dict)

    # Update the Scientific Name manually for some stocks
    with open(os.path.join(input_dir, "corrected_scientific_names.json"), "r") as file:
        corrected_scientific_names = json.load(file)

    overview = update_values(overview, corrected_scientific_names)

    # Correct Engraulis encrasicolus to Engraulis capensis only for Area 47
    overview = update_values(
        overview, {"Engraulis encrasicolus": "Engraulis capensis"}, sheets=["47"]
    )

    # Create list of all stocks to remove based on being a duplicate or an erroneous stock
    # Remove based on primary key Sheet, Original Line No.
    stocks_to_remove = {
        "21": [148],
        "27": [112, 114],
        "Area34Luca": [96, 109],
        "41": [25],
        "47": [16, 37],
        "51fromAbwoTuna": [10, 149],
        "61": [32],
        "77": [86],
        "Deep Sea": [23, 24, 44, 45, 46, 47, 56],
        "Sharks": [20, 24, 48],
    }
    overview = remove_stocks(overview, stocks_to_remove)

    # Change the location of certain stocks
    # Either add further specification to location (e.g. Brazil --> Brazil N),
    # add stock specification to location (e.g. South --> South 1),
    # or add '(to ignore)' flag if stock is erroneous
    # and to be ignored in assessed stocks list (e.g. Kenya --> Kenya (to ignore))
    location_changes = {
        "27": [
            (116, "Divisions 6.a, 7.b, and 7.j (to ignore 1)"),
            (117, "Divisions 6.a, 7.b, and 7.j (to ignore 2)"),
            (118, "Divisions 8.c and 9.a 1 (to ignore 1)"),
            (119, "Divisions 8.c and 9.a 2 (to ignore 2)"),
            (136, "Subareas V, VI, XII and XIV, NAFO subareas 1, 2: Deep stock"),
            (137, "Subareas V, VI, XII and XIV, NAFO subareas 1, 2: Shallow stock"),
        ],
        "Area34Luca": [
            (33, "South 3"),
            (34, "South 4"),
            (38, "South 1"),
            (39, "South 4"),
            (45, "South 3"),
            (46, "South 4"),
            (53, "South 3"),
            (54, "South 4"),
            (71, "South 3"),
            (72, "South 4"),
            (76, "South 3"),
            (77, "South 4"),
            (78, "South 1"),
            (106, "South 3"),
            (107, "South 4"),
            (108, "South 1"),
        ],
        "37": [
            (14, "Algeria 1"),
            (17, "Algeria 2"),
            (19, "Algeria 3"),
            (27, "Levant Sea 1"),
            (28, "Levant Sea 2"),
        ],
        "41": [
            (6, "AUFZ 1"),
            (7, "AUFZ 2"),
            (36, "Brazil N"),
            (37, "Brazil S"),
            (40, "Brazil N"),
            (41, "Brazil S"),
            (51, "Brazil N"),
            (52, "Brazil S"),
            (60, "Brazil N"),
            (61, "Brazil S"),
            (74, "Brazil N"),
            (75, "Brazil S"),
            (76, "Brazil N"),
            (77, "Brazil S"),
            (79, "Brazil N"),
            (80, "Brazil S"),
        ],
        "47": [
            (17, "South Africa, West of Cape Agulhas"),
            (18, "South Africa, Southeast of Cape Agulha"),
        ],
        "51fromAbwoTuna": [
            (23, "Mozambique 1"),
            (37, "India Maharashtra N"),
            (39, "India Kerala - Mixed"),
            (40, "India Maharashtra S"),
            (42, "India Kerala - Semi Industrial"),
            (63, "India Kerala (to ignore)"),
            (88, "51"),
            (92, "Mozambique 2"),
            (133, "Area 51 (to ignore)"),
            (166, "Area 51 (to ignore 1)"),
            (168, "Area 51 (to ignore 2)"),
            (280, "Area 51 (to ignore)"),
            (308, "Kenya (to ignore)"),
            (378, "Oman (to ignore)"),
            (404, "Area 51 (to ignore)"),
        ],
        "61": [
            (37, "Russia - Biomass 0.245"),
            (61, "B 0.022"),
            (62, "B 0.678"),
        ],
        "77": [
            (6, "Mexico 1"),
            (7, "Mexico 2"),
            (13, "United States of America, Mexico: Northern subpopulation"),
            (14, "United States of America, Mexico: Southern subpopulation"),
            (35, "Mojarras 1"),
            (36, "Mojarras 2"),
            (71, "Mexico (Southwest Baja Peninsula)"),
            (72, "Mexico (Central Eastern Gulf of California)"),
            (73, "Mexico (Southeast Gulf of California)"),
            (74, "Mexico (Southwest Baja Peninsula)"),
            (75, "Mexico (Central Eastern Gulf of California)"),
            (76, "Mexico (Southeast Gulf of California)"),
            (77, "Mexico (Gulf of Tehuantepec)"),
            (78, "Mexico (Southeast Gulf of California)"),
            (79, "Mexico (Gulf of Tehuantepec)"),
        ],
        "87": [
            (47, "Peru N"),
            (48, "Peru S"),
            (83, "Peru - Chile"),
            (84, "Chile Central-North"),
            (85, "Chile Central-South"),
            (86, "Chile - Aysén"),
            (87, "Chile - Los Lagos"),
            (88, "Chile - Aysén"),
            (89, "Chile - Los Lagos"),
            (92, "Chile S"),
            (93, "Chile N"),
            (97, "Chile S"),
            (98, "Chile N"),
            (100, "Chile N"),
            (101, "Chile Central"),
            (102, "Chile S"),
            (103, "Chile S"),
            (104, "Chile N"),
            (105, "Chile S"),
            (106, "Chile N"),
            (108, 1),
            (109, 2),
        ],
        "Deep Sea": [(13, "Divisions 3NO Grand Bank")],
        "Tuna": [(19, "Pacific"), (22, "Southern")],
    }

    overview = change_locations(overview, location_changes)

    # Some stocks of same species have differing ISSCAAP Codes and ASFIS Names
    # or string descriptions of ISSCAAP Code
    # We fix this here
    correct_isscaap_name_dict = {
        "21": {
            "ASFIS Name": {
                "Sebastes mentella, Sebastes fasciatus": "Beaked redfish, Acadian redfish"
            }
        },
        "Area34Luca": {"ASFIS Name": {"Pomadasys spp": np.nan}},
        "41": {"ASFIS Name": {"Macruronus novaezelandiae": "Blue grenadier"}},
        "47": {"ASFIS Name": {"Sciaenidae": "Croakers, drums NEI"}},
        "51fromAbwoTuna": {
            "ISSCAAP Code": {
                "Thenus unimaculatus": 43,
                "Thryssa spp": 35,
                "[Aggregate]": np.nan,
                "Macruronus novaezelandiae": 32,
            },
            "ASFIS Name": {
                "Thenus unimaculatus": "Shovelnose lobster",
                "Thryssa spp": "Other Anchovies",
                "Ablennes hians": "Flat needlefish",
                "Aristaeomorpha foliacea": "Giant red shrimp",
                "Arius spp": "[Arius spp]",
                "Coilia dussumieri, Coilia macrognathos, Coilia mystus": "Grenadier anchovy",
                "Encrasicholina heteroloba": "Shorthead anchovy",
            },
        },
        "57": {
            "ASFIS Name": {
                "Sciaenidae": "Croakers, drums NEI",
                "Thryssa spp": "Other Anchovies",
            }
        },
        "Area67woSalmon": {
            "ASFIS Name": {
                "Sebastes melanostictus, Sebastes aleutianus": "Blackspotted and Rougheye Rockfish Complex",
                "Sebastes miniatus, Sebastes crocotulus": "Vermilion and Sunset Rockfish Complex",
            }
        },
        "71fromABwoTuna": {
            "ISSCAAP Code": {"Diagramma pictum": 33},
            "ASFIS Name": {
                "Sciaenidae": "Croakers, drums NEI",
                "Sepiidae, Sepiolidae": "Cuttlefish, bobtail squids NEI",
                "Sparidae": "Porgies, seabreams NEI",
                "Uroteuthis (Photololigo) duvaucelii": "Indian squid",
            },
        },
        "77": {
            "ASFIS Name": {
                "Sebastes miniatus, Sebastes crocotulus": "Vermilion and Sunset Rockfish Complex"
            }
        },
        "81": {
            "ISSCAAP Code": {
                "Atractoscion atelodus": 33,
                "Ibacus peronii, Ibacus brucei, Ibacus chacei, Ibacus alticrenatus, Ibacus spp": 43,
                "Octopus australis": 57,
                "Centrostephanus rodgersii": 76,
                "Scylla spp, Scylla serrata, Scylla olivacea": 42,
            }
        },
        "87": {
            "ISSCAAP Code": {
                "Auxis thazard, A. rochei": 36,
                "Macruronus novaezelandiae": 32,
            },
            "ASFIS Name": {"Macruronus novaezelandiae": "Blue grenadier"},
        },
        "Deep Sea": {"ISSCAAP Code": {"Aggregate: Deepwater Sharks": 38}},
    }
    overview = correct_values(overview, correct_isscaap_name_dict)

    # Add Angolan stocks from file overview_2025-02-04.xlsx to Area 47
    angolan_stocks = pd.read_excel(os.path.join(input_dir, "overview_2025-02-04.xlsx"))
    angolan_stocks = angolan_stocks.drop(columns=["Scientific Name Found"])
    angolan_stocks = angolan_stocks.reset_index().rename(
        columns={"index": "Original Line No."}
    )
    angolan_stocks["Original Line No."] += 2
    angolan_stocks["Sheet"] = "overview_2025-02-04.xlsx"
    # Stocks to add are in these line numbers
    angolan_stocks_line_nos = [538, 2559, 2560, 2561, 2562]
    angolan_stocks_mask = angolan_stocks["Original Line No."].isin(
        angolan_stocks_line_nos
    )
    angolan_stocks = angolan_stocks[angolan_stocks_mask]
    overview["47"] = pd.concat([overview["47"], angolan_stocks]).reset_index(drop=True)
    
    # Fill the NaN Location values with the name of the Area
    for sheet, df in overview.items():
        overview[sheet] = fix_nan_location(df)

    # Only keep the standard columns
    # Map the columns to their possible data types
    standard_columns = {
        "Sheet": str,
        "Original Line No.": int,
        "Area": (int, str),
        "ISSCAAP Code": (int, float),
        "ASFIS Name": (str, float),
        "ASFIS Scientific Name": str,
        "Location": str,
        "Tier": (int, float),
        "Status": (str, float),
        "Uncertainty": (str, float),
    }
    overview = use_standard_columns(overview, standard_columns.keys())
    overview = standardize_dtypes(overview, standard_columns)

    # Only keep assessments with tier reported
    overview = filter_dfs(overview, {"Tier": [1, 2, 3]})

    # Standardize uncertainties to L, M, H or NaN
    uncertainty_map = {
        "high": "H",
        "medium": "M",
        "low": "L",
        "High": "H",
        "Medium": "M",
        "Low": "L",
        "F": "M",
        " ": np.nan,
    }
    overview = standardize_column_values(
        overview, {"Uncertainty": uncertainty_map}, key="Uncertainty"
    )

    # Concatenate the data into a single dataframe
    # Sort on Area, ASFIS Scientific Name, Location
    # Remove sheet 67 before concatenating data
    del overview["67"]
    cols_to_sort = ["Area", "ASFIS Scientific Name", "Location"]
    stock_assessments = concatenate_data(overview, cols_to_sort)

    # Only keep rows with Scientific Name listed
    stock_assessments = stock_assessments[
        stock_assessments["ASFIS Scientific Name"].apply(
            lambda sn: isinstance(sn, str) and sn != "nan"
        )
    ]

    # Save the complete data (assessed and unassessed)
    # and the assessed data separately
    # Schema: Area, ISSCAAP Code, ASFIS Name,ASFIS Scientific Name, Location, Tier, Status, Uncertainty
    # Stock id/Primary key: (Area, ASFIS Scientific Name, Location)
    cols_to_keep = [
        "Area",
        "ISSCAAP Code",
        "ASFIS Name",
        "ASFIS Scientific Name",
        "Location",
        "Tier",
        "Status",
        "Uncertainty",
    ]
    stock_assessments = stock_assessments[cols_to_keep]
    stock_assessments = stock_assessments.sort_values(
        ["Area", "ASFIS Scientific Name", "Location"]
    )

    # We begin data validation phase
    print("Validating Data...")

    # Check uniqueness and non-nullity of primary key
    validate_primary_key(stock_assessments)

    # Check that ASFIS Names and ISSCAAP Codes are consistent
    # for a given ASFIS Scientific Name
    validate_consistent_values(stock_assessments)

    # Data is validated, now we can save to output folder
    # Create separate dataframe for assessed stocks
    assessed_stocks_mask = stock_assessments["Status"].isin(["U", "M", "O"])
    assessed_stocks = stock_assessments[assessed_stocks_mask]

    print(f"Saving output files to {output_dir}")
    stock_assessments.to_excel(
        os.path.join(output_dir, "stock_assessments_w_unassessed.xlsx"), index=False
    )
    assessed_stocks.to_excel(
        os.path.join(output_dir, "stock_assessments.xlsx"), index=False
    )


if __name__ == "__main__":
    main()
