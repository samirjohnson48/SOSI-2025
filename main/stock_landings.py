"""

"""

import os
import pandas as pd
import numpy as np
import json

# Change directory for import
import sys

sys.path.append(os.path.dirname(os.getcwd()))

from utils.stock_landings import *
from utils.stock_weights import extract_alphanumeric
from utils.stock_assessments import get_asfis_mappings
from utils.species_landings import format_fishstat


def main():
    # Define directories for input and output files
    parent_dir = os.path.dirname(os.getcwd())
    input_dir = os.path.join(parent_dir, "input")
    output_dir = os.path.join(parent_dir, os.path.join("output", "clean_data"))

    # Retrieve the species landings
    species_landings = pd.read_excel(os.path.join(output_dir, "species_landings.xlsx"))

    # Retrieve the weights
    weights = pd.read_excel(os.path.join(output_dir, "stock_weights.xlsx"))

    # Merge the two dataframes
    species_landings["Location Match"] = species_landings["Location"].apply(
        extract_alphanumeric
    )
    weights["Location Match"] = weights["Location"].apply(extract_alphanumeric)

    stock_landings = pd.merge(
        species_landings,
        weights,
        on=["Area", "ASFIS Scientific Name", "Location Match"],
        how="inner",
        suffixes=("", "_x"),
    )
    stock_landings = stock_landings.rename(columns={2021: "Species Landings 2021"})
    cols_to_keep = [
        "Area",
        "ASFIS Scientific Name",
        "Location",
        "Species Landings 2021",
        "Normalized Weight",
    ]
    stock_landings = stock_landings[cols_to_keep]

    # Add Num Stocks column for computing landings
    stock_landings = compute_num_stocks(stock_landings)

    stock_landings["Stock Landings 2021"] = stock_landings.apply(
        compute_landings, axis=1
    )
    stock_landings = stock_landings.drop(columns="Num Stocks")

    # Use proxy species landings for stocks with missing landings
    proxy_landings = pd.read_excel(
        os.path.join(input_dir, "January overview - one table.xlsx"),
        sheet_name="Stocks with Status and Tier",
    )
    proxy_landings = proxy_landings.rename(
        columns={
            "AREA": "Area",
            "scientific name w value": "Proxy Species",
            "Stock Catch Value": "Proxy Species Landings",
        }
    )
    proxy_landings = proxy_landings.dropna(subset=["Proxy Species Landings", "Proxy Species"])
    proxy_landings = proxy_landings.drop_duplicates(
        ["Area", "ASFIS Scientific Name", "Location"]
    )
    proxy_cols = [
        "Area",
        "ASFIS Scientific Name",
        "Location",
        "Proxy Species",
        "Proxy Species Landings",
    ]
    proxy_landings = proxy_landings[proxy_cols]

    # Fix NaN scientific names
    sn_mask = proxy_landings["ASFIS Scientific Name"].isna()
    loc_mask = proxy_landings["Location"] == "DEMS/Crust(Cameroon)"
    proxy_landings.loc[sn_mask & loc_mask, "ASFIS Scientific Name"] = "Coastal shrimps"
    
    # Use the proxy landings
    stock_landings = use_proxy_landings(stock_landings, proxy_landings)

    # For the remaining stocks with missing landings, we use the NEI species corresponding
    # to the stock's ISSCAAP Code. We split the landings according to the distribution
    # of status among stocks with landings
    # We retrieve the status of stocks for the distribution
    primary_key = ["Area", "ASFIS Scientific Name", "Location"]
    stock_assessments = pd.read_excel(
        os.path.join(output_dir, "stock_assessments.xlsx")
    )
    stock_assessments = stock_assessments[
        primary_key + ["ISSCAAP Code", "ASFIS Name", "Status"]
    ]
    stock_landings = pd.merge(stock_landings, stock_assessments, on=primary_key)

    # Retrieve ISSCAAP Code to NEI species mapping
    with open(os.path.join(input_dir, "ISSCAAP_to_NEI.json"), "r") as file:
        isscaap_to_nei = json.load(file)
    isscaap_to_nei = {
        int(k): v for k, v in isscaap_to_nei.items()
    }  # JSON saves keys are strings

    # Retrieve fishstat and ASFIS data for NEI landings
    fishstat = pd.read_csv(os.path.join(input_dir, "global_capture_production.csv"))
    mappings = get_asfis_mappings(input_dir, "ASFIS_sp_2024.csv")
    asfis = mappings["ASFIS"]
    code_to_scientific = dict(zip(asfis["Alpha3_Code"], asfis["Scientific_Name"]))
    scientific_to_name = mappings["ASFIS Scientific Name to ASFIS Name"]

    fishstat = format_fishstat(fishstat, code_to_scientific)
    fishstat["ASFIS Name"] = fishstat["ASFIS Scientific Name"].map(scientific_to_name)

    numerical_areas = [
        area for area in stock_landings["Area"].unique() if isinstance(area, int)
    ]
    stock_landings = compute_missing_landings(
        stock_landings, fishstat, numerical_areas, isscaap_to_nei
    )

    # Save assigned landings to output file
    cols_to_save = [
        "Area",
        "ASFIS Scientific Name",
        "Location",
        "Proxy Species",
        "Stock Landings 2021",
    ]
    stock_landings = stock_landings[cols_to_save]
    stock_landings.to_excel(
        os.path.join(output_dir, "stock_landings.xlsx"), index=False
    )


if __name__ == "__main__":
    main()
