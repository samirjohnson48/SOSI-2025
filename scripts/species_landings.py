"""
This file creates a list of assessed stocks with their corresponding species landings from Fishstat

Species landings for a given stock are the summed landings of that stock's species in that stock's FAO major fishing area(s).

Inputs
    - ./input/global_capture_production.csv: Global capture production data (1950-2022) from Fishstat database
    - ./input/ASFIS_sp_2024.csv: list of ASFIS species in 2024
    - ./output/clean_data/stock_assessments.xlsx: Cleaned list of all assessed stocks
    
Output:
    - ./output/clean_data/species_landings.xlsx: list of all assessed stocks with species landings from 1950 - 2021
    
Output schema (primary key = [Area, ASFIS Scientific Name, Location]):
    - Area: The group of stocks which are found in separate sheets from input
        Most of the time, this is an FAO major fishing area (21, 27, etc.)
        However, this can include other types of aggregations, such as 
        Salmon, Tuna, Deep Sea, and Sharks.
    - ASFIS Scientific Name: The current ASFIS Scientific Name pertaining to the species of the stock
    - Location: The reported location of the stock
    - 1950, ..., 2021: Total landings for years 1950, ..., 2021 for the stock's species in that stock's area(s)
"""

import os
import pandas as pd
import numpy as np
import json
from tqdm import tqdm


from utils.species_landings import *
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

    # Retrieve fishstat data from input folder
    fishstat = pd.read_csv(os.path.join(input_dir, "global_capture_production.csv"))

    # Format fishstat data
    mappings = get_asfis_mappings(input_dir, "ASFIS_sp_2024.csv")
    asfis = mappings["ASFIS"]
    code_to_scientific = dict(zip(asfis["Alpha3_Code"], asfis["Scientific_Name"]))

    fishstat = format_fishstat(fishstat, code_to_scientific)

    # Retrieve assessed stocks from clean_data folder
    species_landings = pd.read_excel(os.path.join(output_dir, "stock_assessments.xlsx"))
    species_landings = species_landings[["Area", "ASFIS Scientific Name", "Location"]]

    # Retrieve map of location to FAO major fishing area for Tuna, Sharks, and Deep Sea stocks
    with open(os.path.join(input_dir, "location_to_area.json"), "r") as file:
        location_to_area = json.load(file)

    # Expand the Special Group stocks across their FAO Areas
    special_groups = ["48,58,88", "Salmon", "Sharks", "Tuna"]

    species_landings = expand_sg_stocks(
        species_landings, special_groups, location_to_area
    )

    # Compute species landings for all assessed stocks
    year_start, year_end = 1950, 2021
    years = list(range(year_start, year_end + 1))

    pk = ["FAO Area", "ASFIS Scientific Name"]

    sl_reduced = species_landings.drop_duplicates(pk)[pk].copy()

    print("Computing species landings...")
    tqdm.pandas()
    sl_reduced[years] = sl_reduced.progress_apply(
        compute_species_landings, args=(fishstat, location_to_area), axis=1
    )

    species_landings = pd.merge(species_landings, sl_reduced, on=pk)

    # Substitute landings for certain stocks
    subs = [
        [47, ["Sardinella aurita", "Sardinella maderensis"], ["Sardinella spp"]],
        [21, ["Sebastes mentella, Sebastes fasciatus"], ["Sebastes spp"]],
        [
            34,
            ["Penaeus notialis, Penaeus monodon, Holthuispenaeopsis atlantic"],
            ["Penaeus spp"],
        ],
    ]
    species_landings = substitute_landings(species_landings, fishstat, subs, years)

    print("Species landings computed")

    # Save stocks with species landings
    file_path = os.path.join(output_dir, "species_landings.xlsx")
    print(f"Saving species landings data to {file_path}")
    species_landings.to_excel(file_path, index=False)


if __name__ == "__main__":
    main()
