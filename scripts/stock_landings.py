"""

"""

import os
import pandas as pd
import numpy as np
import json

from utils.stock_landings import *
from utils.stock_assessments import get_asfis_mappings
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
    output_dir = os.path.join(parent_dir, os.path.join("output", "clean_data"))

    # Retrieve the species landings
    species_landings = pd.read_excel(os.path.join(output_dir, "species_landings.xlsx"))

    # Retrieve the weights
    weights = pd.read_excel(os.path.join(output_dir, "stock_weights.xlsx"))
    weights = weights.drop(columns="Area")

    # Merge the two dataframes
    stock_landings = pd.merge(
        species_landings,
        weights,
        on=["FAO Area", "ASFIS Scientific Name", "Location"],
    )
    stock_landings = stock_landings.rename(columns={2021: "Species Landings 2021"})
    cols_to_keep = [
        "FAO Area",
        "ASFIS Scientific Name",
        "Location",
        "Area",
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
    proxy_landings = proxy_landings.dropna(
        subset=["Proxy Species Landings", "Proxy Species"]
    )
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
        "FAO Area",
        "ASFIS Scientific Name",
        "Location",
        "Proxy Species",
        "Stock Landings 2021",
    ]
    stock_landings_long = stock_landings[cols_to_save].copy()
    stock_landings_long.to_excel(
        os.path.join(output_dir, "stock_landings_fao_areas.xlsx"), index=False
    )

    # Save landings grouping special group stocks
    stock_landings_grouped = (
        stock_landings_long.groupby(["Area", "ASFIS Scientific Name", "Location"])
        .agg({"Proxy Species": "first", "Stock Landings 2021": "sum"})
        .reset_index()
    )

    stock_landings_grouped.to_excel(
        os.path.join(output_dir, "stock_landings.xlsx"), index=False
    )


if __name__ == "__main__":
    main()
