"""
"""

import os
import pandas as pd
import numpy as np
import json
from tqdm import tqdm

from utils.sofia_landings import *
from utils.species_landings import format_fishstat, compute_species_landings
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
    sofia["Species"] = sofia["Species"].fillna(sofia["Name"])
    sofia = sofia.rename(
        columns={
            "Name": "ASFIS Name",
            "Species": "ASFIS Scientific Name",
            "X2021": "Status",
        }
    )
    sofia = sofia[["Area", "ASFIS Scientific Name", "Status"]]
    sofia = sofia.dropna(subset="ASFIS Scientific Name")
    sofia = sofia[sofia["Area"] != "Tunas"]

    # Convert the multiple statuses to individual observations
    sofia["Status List"] = sofia["Status"].apply(convert_status_to_list)
    sofia = (
        sofia.explode("Status List")
        .drop(columns="Status")
        .rename(columns={"Status List": "Status"})
    )

    # Add tunas separately and combine
    # Use tuna sheet from updated_assessment_overview since it contains the locations
    # These are same stocks listed in Tunas_HilarioISSF in sofia2024v2Oct31woTunasFinalchcecksMarch2024.xlsx
    # (see column U2021)
    sofia_tunas = pd.read_excel(
        os.path.join(input_dir, "updated_assessment_overview.xlsx"), sheet_name="Tuna"
    )
    sofia_tunas["Area"] = "Tuna"
    # Update missing locations so we can find areas from location to area map
    tuna_mask1 = sofia_tunas["ASFIS Scientific Name"] == "Thunnus orientalis"
    tuna_mask2 = sofia_tunas["ASFIS Scientific Name"] == "Thunnus maccoyii"
    sofia_tunas.loc[tuna_mask1, "Location"] = "Pacific"
    sofia_tunas.loc[tuna_mask2, "Location"] = "Southern"
    sofia_tunas = sofia_tunas[["Area", "ASFIS Scientific Name", "Location", "Status"]]

    sofia = pd.concat([sofia, sofia_tunas]).reset_index(drop=True)

    # Get the names of proxy species to be used to retrieve landings for SOFIA stocks
    # These names can either be spelling corrections or similar species in area
    mappings = get_asfis_mappings(input_dir, "ASFIS_sp_2024.csv")
    scientific_names = mappings["ASFIS Scientific Names"]
    sofia["Proxy"] = sofia["ASFIS Scientific Name"].apply(
        get_proxy_name, args=(scientific_names,)
    )

    # Manually update proxy names for some species
    sofia_proxies = {
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
    }
    sofia_proxy_updates = {
        "Sardinops spp": "Sardinops sagax",
        "Oncorhynch spp": "Oncorhynchus spp",
        "Notothenia spp": "Gobionotothen gibberifrons",
    }

    sofia["Proxy"] = sofia["ASFIS Scientific Name"].apply(
        lambda x: sofia_proxies.get(x, x)
    )
    sofia["Proxy"] = sofia["Proxy"].apply(lambda x: sofia_proxy_updates.get(x, x))

    # Retrieve fishstat data to assign landings
    fishstat = pd.read_csv(os.path.join(input_dir, "global_capture_production.csv"))

    # Format fishstat data
    asfis = mappings["ASFIS"]
    code_to_scientific = dict(zip(asfis["Alpha3_Code"], asfis["Scientific_Name"]))
    fishstat = format_fishstat(fishstat, code_to_scientific)

    # Retrieve location to area map for tunas
    with open(os.path.join(input_dir, "location_to_area.json"), "r") as file:
        location_to_area = json.load(file)

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
            "Proxy",
        ),
        axis=1,
    )

    # We do not have weighting for SOFIA stocks, so we normalized landings
    # by number of species of same name within a given area
    # Tuna landings are already stock specific, so take them out before normalizing
    tuna_mask = sofia["Area"] == "Tuna"
    sofia_wo_tuna = sofia[~tuna_mask].copy()
    sofia_wo_tuna = normalize_landings(sofia_wo_tuna, years)

    sofia_landings = pd.concat([sofia_wo_tuna, sofia[tuna_mask]]).reset_index(drop=True)

    sofia_landings.to_excel(
        os.path.join(output_dir, "sofia_landings.xlsx"), index=False
    )


if __name__ == "__main__":
    main()
