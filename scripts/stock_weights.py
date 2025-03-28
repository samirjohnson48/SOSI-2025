"""

"""

import os
import pandas as pd
import numpy as np
import json
from tqdm import tqdm

from utils.stock_weights import *
from utils.stock_assessments import fix_nan_location
from utils.species_landings import expand_sg_stocks


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

    # Retrieve list of assessed stocks
    weights = pd.read_excel(os.path.join(output_dir, "stock_assessments.xlsx"))

    primary_key = ["Area", "ASFIS Scientific Name", "Location"]
    weights = weights[primary_key]

    # Retrieve and reformat all files used for assigning weights
    # Main file for weights across all stocks
    base_weights = pd.read_excel(
        os.path.join(input_dir, "data_w_landings&weights.xlsx")
    )
    base_weights = base_weights.rename(
        columns={
            "country catch": "Weight 1",
            "weighted": "Weight 2",
        },
    )
    base_weights = base_weights[primary_key + ["Weight 1", "Weight 2"]]

    # Fix NaN location
    base_weights = fix_nan_location(base_weights)

    # File for weights in areas 21, 27, 67
    weights_21_27_67 = pd.read_excel(
        os.path.join(input_dir, "Complete_data_weighting.xlsx")
    )
    weights_21_27_67 = weights_21_27_67.rename(columns={"Weight": "Weight 2"})
    weights_21_27_67 = weights_21_27_67[primary_key + ["Weight 2"]]
    # Fix NaN locations
    weights_21_27_67 = fix_nan_location(weights_21_27_67)
    # Remove duplicate stocks
    idx_remove = [205, 207, 409]
    idx_keep = [i for i in weights_21_27_67.index if i not in idx_remove]
    weights_21_27_67 = weights_21_27_67.loc[idx_keep]

    # File for weights of Indian stocks
    weights_india = pd.read_excel(
        os.path.join(input_dir, "AB Stocks of India Jan2025.xlsx"), skiprows=1
    )
    weights_india = weights_india.rename(columns={"BMSY": "Weight 2"})
    weights_india = weights_india[primary_key + ["Weight 2"]]
    # Fix NaN locations
    weights_india = fix_nan_location(weights_india)

    # Assign weights based on reported landings for Areas 31, 7, 81
    weights_31_37_81 = retrieve_31_37_81_weights(
        os.path.join(input_dir, "updated_assessment_overview.xlsx")
    )
    weights_31_37_81 = weights_31_37_81[primary_key + ["Weight 1"]]
    # Fix NaN locations
    weights_31_37_81 = fix_nan_location(weights_31_37_81)

    # File for weights of Deep Sea stocks
    weights_ds = pd.read_excel(os.path.join(input_dir, "deep_sea_weights.xlsx"))
    weights_ds = weights_ds.rename(columns={"Weight": "Weight 2"})
    weights_ds = weights_ds[primary_key + ["Weight 2"]]

    # Add the weights to the list of assessed stocks
    weights = merge_weights(weights, base_weights, primary_key)
    weights = merge_weights(
        weights, weights_21_27_67, primary_key, weight1_na=True, clean_location=True
    )
    weights = merge_weights(weights, weights_india, primary_key, weight1_na=True)
    weights = merge_weights(weights, weights_31_37_81, primary_key)
    weights = merge_weights(weights, weights_ds, primary_key)

    # Assign the normalized weights based off Weight 1 and Weight 2
    # Specify the area for stocks in categorical areas e.g. 48,58,88, Tuna, etc.
    # Retrieve location_to_area map
    with open(os.path.join(input_dir, "location_to_area.json"), "r") as file:
        location_to_area = json.load(file)

    special_groups = ["48,58,88", "Salmon", "Sharks", "Tuna"]
    weights = expand_sg_stocks(weights, special_groups, location_to_area)

    # Progress bar
    tqdm.pandas()

    weights["Normalized Weight"] = (
        weights.groupby(["FAO Area", "ASFIS Scientific Name"])[["Weight 1", "Weight 2"]]
        .progress_apply(compute_weights)
        .reset_index(level=[0, 1], drop=True)
    )

    # Validate weight normalization
    validate_normalization(weights, group_key=["FAO Area", "ASFIS Scientific Name"])

    # Save assigned weights to output file
    file_path = os.path.join(output_dir, "stock_weights.xlsx")
    print(f"Saving stocks with weights to {file_path}")
    weights.to_excel(file_path, index=False)


if __name__ == "__main__":
    main()
