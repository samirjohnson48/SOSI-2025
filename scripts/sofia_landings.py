"""
"""

import os
import pandas as pd
import numpy as np
import json
from tqdm import tqdm

from utils.sofia_landings import *
from utils.species_landings import (
    format_fishstat,
    expand_sg_stocks,
    compute_species_landings,
)
from utils.stock_assessments import get_asfis_mappings


def main():
    # Define directories for input and output files
    if os.path.basename(os.getcwd()) == "SOSI-2025":
        parent_dir = os.getcwd()
    elif os.path.basename(os.path.dirname(os.getcwd())) == "SOSI-2025":
        parent_dir = os.path.dirname(os.getcwd())
    else:
        raise FileNotFoundError("SOSI-2025 folder could not be found.")

    input_dir = os.path.join(parent_dir, "input")
    output_dir = os.path.join(parent_dir, os.path.join("output", "clean_data"))

    # Retrieve SOFIA data
    sofia = pd.read_excel(
        os.path.join(input_dir, "sofia2024v2Oct31woTunasFinalchcecksMarch2024.xlsx"),
        sheet_name="sofia2024",
    )

    # Reformat SOFIA data
    sofia = sofia.rename(
        columns={
            "Name": "ASFIS Name",
            "Species": "ASFIS Scientific Name",
            "X2021": "Status",
        }
    )
    sofia = sofia[["Area", "ASFIS Scientific Name", "ASFIS Name", "Status"]]
    sofia = sofia.dropna(subset=["ASFIS Scientific Name", "ASFIS Name"], how="all")
    sofia = sofia[sofia["Area"] != "Tunas"]

    # Convert the multiple statuses to individual observations
    sofia["Status List"] = sofia["Status"].apply(convert_status_to_list)
    sofia = (
        sofia.explode("Status List")
        .drop(columns="Status")
        .rename(columns={"Status List": "Status"})
    )

    # Convert F to M for status
    sofia["Status"] = sofia["Status"].apply(lambda x: {"F": "M"}.get(x, x))

    # Add tunas separately and combine
    # Use tuna sheet from updated_assessment_overview since it contains the locations
    # These are same stocks listed in Tunas_HilarioISSF in sofia2024v2Oct31woTunasFinalchcecksMarch2024.xlsx
    # (see column U2021)
    sofia_tunas = pd.read_excel(
        os.path.join(input_dir, "updated_assessment_overview.xlsx"), sheet_name="Tuna"
    )
    sofia_tunas["Area"] = "Tuna"

    # Assign common names
    mappings = get_asfis_mappings(input_dir, "ASFIS_sp_2024.csv")
    sn_to_name = mappings["ASFIS Scientific Name to ASFIS Name"]
    sofia_tunas["ASFIS Name"] = sofia_tunas["ASFIS Scientific Name"].map(sn_to_name)

    # Update missing locations so we can find areas from location to area map
    tuna_mask1 = sofia_tunas["ASFIS Scientific Name"] == "Thunnus orientalis"
    tuna_mask2 = sofia_tunas["ASFIS Scientific Name"] == "Thunnus maccoyii"
    sofia_tunas.loc[tuna_mask1, "Location"] = "Pacific"
    sofia_tunas.loc[tuna_mask2, "Location"] = "Southern"
    sofia_tunas = sofia_tunas[
        ["Area", "ASFIS Scientific Name", "ASFIS Name", "Location", "Status"]
    ]

    sofia = pd.concat([sofia, sofia_tunas]).reset_index(drop=True)

    # Fix the scientific names and common names
    scientific_names = mappings["ASFIS Scientific Names"]

    sofia["ASFIS Scientific Name"] = sofia["ASFIS Scientific Name"].apply(
        get_scientific_name, args=(scientific_names,)
    )

    # Manually fix the rest

    sofia_sn_update = {
        "Alosa pontica": "Alosa immaculata",
        "Cancer magister": "Metacarcinus magister",
        "Cynoscion striatus": "Cynoscion guatucupa",
        "Limanda ferruginea": "Myzopsetta ferruginea",
        "Loligo gahi": "Doryteuthis gahi",
        "Loligo opalescens": "Doryteuthis opalescens",
        "Loligo reynaudi": "Loligo reynaudii",
        "Notothenia gibberifrons": "Gobionotothen gibberifrons",
        "Oncorhynch sp.": "Oncorhynchus spp",
        "Pagrus auratus": "Chrysophrys auratus",
        "Pandalus sp.": "Pandalus spp",
        "Perciformes": "Perciformes (Others)",
        "Sardinops caeruleus": "Sardinops sagax",
        "Sardinops melanostictus": "Sardinops sagax",
        "Sardinops ocellatus": "Sardinops sagax",
        "Sciaenids": "Sciaenidae",
        "Scombroidei": "Scombriformes (Scombroidei)",
        "Thyrsites atun": "Leionura atun",
        "Sabastes Species": "Sebastes spp",
        "Theragra chalcogramma": "Gadus chalcogrammus",
        "Lamanda aspera": "Limanda aspera",
        "Ophiodon elogatus": "Ophiodon elongatus",
        "Anoploma fimbria": "Anoplopoma fimbria",
        "Clupia pallasii": "Clupea pallasii",
        "Macruronus magellanicus": "Macruronus novaezelandiae",
        "Patinopecten yessoensis": "Mizuhopecten yessoensis",
        "Cancer porductus": "Cancer productus",
        "Nototodarus sloani": "Nototodarus sloanii",
        "Sardinops spp": "Sardinops sagax",
        "Oncorhynch spp": "Oncorhynchus spp",
        "Notothenia spp": "Gobionotothen gibberifrons",
    }

    sofia["ASFIS Scientific Name"] = sofia["ASFIS Scientific Name"].apply(
        lambda x: sofia_sn_update.get(x, x)
    )

    sofia_name_update = {
        "Cods, hakes, haddocks": "Gadiformes NEI, Hakes NEI",
        "Marine fishes not identified": "Marine fishes NEI",
        "Other Abalones, winkles, conchs": "Abalones NEI, Periwinkles NEI, Stromboid conchs NEI",
        "Other Clams, cockles, arkshells": "Venus clams NEI, Cockles NEI, Marine shells NEI",
        "Other Cods, hakes, haddocks": "Gadiformes NEI, Hakes NEI",
        "Other cos, hakes, haddocks, etc.": "Gadiformes NEI, Hakes NEI",
        "Other Flounders, halibuts, soles": "Flatfishes NEI",
        "Other flounder halibut and sole": "Flatfishes NEI",
        "Other Herrings, sardines, anchovies": "'Herrings, sardines NEI', Anchovies NEI",
        "Other herring, sardine, anchovy, ": "'Herrings, sardines NEI', Anchovies NEI",
        "Other Miscellaneous pelagic fishes": "Pelagic percomorphs NEI",
        "Other mussels": "Sea mussels NEI",
        "Other Oysters": "Cupped oysters NEI",
        "Other Salmons, trouts, smelts": "Pacific salmons NEI, Trouts NEI, Smelts NEI",
        "Other Scallops, pectens": "Scallops NEI",
        "Other Shads": "Shads NEI",
        "Other Sharks, rays, chimaeras": "Various sharks NEI, Deep-water skates and rays NEI",
        "Sharks, rays, chimaeras": "Various sharks NEI, Deep-water skates and rays NEI",
        "Other Shrimps, prawns": "Pacific shrimps NEI",
        "Other shrimps, prawns, etc.": "Pacific shrimps NEI",
        "Shrimps, prawns": "Pacific shrimps NEI",
        "Other Squids, cuttlefishes, octopuses": "Various squids NEI, Cuttlefishes NEI, Octopuses, etc. NEI",
        "Other squid, cuttlefish, octopuses": "Various squids NEI, Cuttlefishes NEI, Octopuses, etc. NEI",
        "Other Tunas, bonitos, billfishes": "Tunas NEI, Bonitos NEI",
        "Snappers": "Snappers NEI",
        "Groupers": "Groupers NEI",
        "Sciaenids": "Croakers, drums NEI",
        "Pacific Herring": "Pacific herring",
    }

    sofia["ASFIS Name"] = sofia["ASFIS Name"].apply(
        lambda x: x.replace("nei", "NEI") if isinstance(x, str) else x
    )
    sofia["ASFIS Name"] = sofia["ASFIS Name"].apply(
        lambda x: sofia_name_update.get(x, x)
    )

    # Assign Scientific Name based on common name, if absent
    sn_to_name = get_asfis_mappings(input_dir, "ASFIS_sp_2024.csv")[
        "ASFIS Scientific Name to ASFIS Name"
    ]
    name_to_sn = {v: k for k, v in sn_to_name.items()}

    sofia["ASFIS Scientific Name 2"] = sofia["ASFIS Name"].apply(
        convert_common_to_sn, args=(name_to_sn,)
    )

    sofia["ASFIS Scientific Name"] = sofia["ASFIS Scientific Name"].fillna(
        sofia["ASFIS Scientific Name 2"]
    )

    sofia = sofia.drop(columns="ASFIS Scientific Name 2")

    # Fill NaN values with common name so it can be used as part of primary key
    sofia["ASFIS Scientific Name"] = sofia["ASFIS Scientific Name"].fillna(
        sofia["ASFIS Name"]
    )

    # Remove tunas reported in FAO Areas
    # Retrieve location to area map for tunas
    with open(os.path.join(input_dir, "location_to_area.json"), "r") as file:
        location_to_area = json.load(file)

    for idx, tuna_row in sofia_tunas.iterrows():
        areas = location_to_area["Tuna"][tuna_row["Location"]]

        areas_mask = sofia["Area"].isin(areas)
        tuna_mask = sofia["ASFIS Scientific Name"] == tuna_row["ASFIS Scientific Name"]

        sofia = sofia[~(areas_mask & tuna_mask)]

    # Expand the Tunas across their FAO Areas

    sofia = expand_sg_stocks(sofia, ["Tuna"], location_to_area)

    # Retrieve fishstat data to assign landings
    fishstat = pd.read_csv(os.path.join(input_dir, "global_capture_production.csv"))

    # Format fishstat data
    asfis = mappings["ASFIS"]
    code_to_scientific = dict(zip(asfis["Alpha3_Code"], asfis["Scientific_Name"]))
    fishstat = format_fishstat(fishstat, code_to_scientific)

    year_start, year_end = 1950, 2021
    years = list(range(year_start, year_end + 1))

    tqdm.pandas()

    sofia[years] = sofia.progress_apply(
        compute_species_landings,
        args=(
            fishstat,
            location_to_area,
            year_start,
            year_end,
        ),
        axis=1,
    )

    # We do not have weighting for SOFIA stocks, so we normalized landings
    # by number of species of same name within a given area
    sofia_landings_fao_areas = normalize_landings(sofia, years)

    # Combine areas 48,58,88
    southern_mask = sofia_landings_fao_areas["FAO Area"].isin([48, 58, 88])
    tuna_list = sofia_tunas["ASFIS Scientific Name"].unique()
    tuna_mask = sofia_landings_fao_areas["ASFIS Scientific Name"].isin(tuna_list)
    sofia_landings_fao_areas.loc[southern_mask & ~tuna_mask, "Area"] = "48,58,88"

    sofia_landings_fao_areas.to_excel(
        os.path.join(output_dir, "sofia_landings_fao_areas.xlsx"), index=False
    )

    # Aggregate landings based on Area, not FAO Area
    agg_dict = {
        "ASFIS Name": "first",
    }
    for year in years:
        agg_dict[year] = "sum"

    sofia_landings = (
        sofia_landings_fao_areas.groupby(["Area", "ASFIS Scientific Name", "Status"])
        .agg(agg_dict)
        .reset_index()
    )

    sofia_landings.to_excel(
        os.path.join(output_dir, "sofia_landings.xlsx"), index=False
    )


if __name__ == "__main__":
    main()
