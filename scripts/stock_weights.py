"""

"""

import os
import pandas as pd
import numpy as np
import json
from tqdm import tqdm

from utils.stock_weights import *
from utils.stock_assessments import fix_nan_location


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

    # Update location of Chanos chanos stock
    chanos_mask = base_weights["ASFIS Scientific Name"] == "Chanos chanos"
    base_weights.loc[chanos_mask & base_weights["Location"].isna(), "Location"] = "51"
    # Fix NaN locations
    base_weights = fix_nan_location(base_weights)

    # Manually update some weights in base_weights based on MSY values
    area_mask = base_weights["Area"] == 34
    sns = ["Ethmalosa fimbriata", "Sardinella aurita", "Sardinella maderensis"]
    sn_mask1 = base_weights["ASFIS Scientific Name"].isin(sns)
    locs = ["SPN/AllZones (Mauritania, Senegal, Gambia)", "SPN/AllZones"]
    loc_mask1 = base_weights["Location"].isin(locs)
    base_weights.loc[area_mask & sn_mask1 & loc_mask1, "Weight 1"] = 200_000

    sn_mask2 = base_weights["ASFIS Scientific Name"] == "Pagellus spp"
    loc_mask2 = base_weights["Location"] == "South"
    base_weights.loc[area_mask & sn_mask2 & loc_mask2, "Weight 2"] = 1

    loc_mask3 = base_weights["Location"] == "Area 34"
    base_weights.loc[area_mask & sn_mask2 & loc_mask3, "Weight 2"] = 5

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

    weights["Area Specific"] = weights[["Area", "Location"]].apply(
        specify_area, args=(location_to_area,), axis=1
    )

    # Separate out Shark stocks into their FAO Areas
    # Then weights are normalized over these species groups to not double count landings

    sharks_mask = weights["Area"] == "Sharks"
    weights_sharks = weights[sharks_mask].copy()
    weights_rest = weights[~(sharks_mask)].copy()

    weights_sharks["Area Specific"] = weights_sharks["Area Specific"].apply(
        lambda areas: [int(a) for a in areas.split(", ")]
    )

    weights_sharks = weights_sharks.explode("Area Specific")

    # # Specify weights for Deep Sea stocks based on corresponding stock from Complete_data_weighting.xlsx
    # ds_sn_mask1 = weights_ds["ASFIS Scientific Name"] == "Pandalus borealis"
    # w_sn_mask1 = weights_21_27_67["ASFIS Scientific Name"] == "Pandalus borealis"
    # ds_loc_mask1 = weights_ds["Location"] == "Division 3LNO"
    # w_loc_mask1 = (weights_21_27_67["Location"] == "SFA 7") & (
    #     weights_21_27_67["Area"] == 21
    # )
    # weights_ds.loc[ds_sn_mask1 & ds_loc_mask1, "Weight 2"] = weights_21_27_67.loc[
    #     w_sn_mask1 & w_loc_mask1, "Weight 2"
    # ].values

    # ds_sn_mask2 = weights_ds["ASFIS Scientific Name"] == "Chionoecetes opilio"
    # w_sn_mask2 = weights_21_27_67["ASFIS Scientific Name"] == "Chionoecetes opilio"
    # ds_loc_mask2 = weights_ds["Location"] == "Grand Bank 3LNO"
    # w_loc_mask2 = (
    #     weights_21_27_67["Location"]
    #     == "Newfoundland and Labrador (Divisions 2HJ3KLNOP4R)"
    # ) & (weights_21_27_67["Area"] == 21)
    # weights_ds.loc[ds_sn_mask2 & ds_loc_mask2, "Weight 2"] = (
    #     weights_21_27_67.loc[w_sn_mask2 & w_loc_mask2, "Weight 2"].values / 2
    # )

    # # Ref: https://www.nafo.int/Portals/0/PDFs/sc/2022/scr22-013.pdf -- Table 1, Total (catch) in 2021
    # ds_sn_mask3 = (
    #     weights_ds["ASFIS Scientific Name"] == "Sebastes mentella, Sebastes fasciatus"
    # )
    # ds_loc_mask3 = weights_ds["Location"] == "Divisions 3LN Grand Bank"
    # weights_ds.loc[ds_sn_mask3 & ds_loc_mask3, "Weight 2"] = 10_172

    # # Ref: https://www.nafo.int/Portals/0/PDFs/sc/2022/scr22-044.pdf -- Table 1, Total (catch) in 2021
    # ds_loc_mask4 = weights_ds["Location"] == "Divisions 3O Grand Bank"
    # weights_ds.loc[ds_sn_mask3 & ds_loc_mask4, "Weight 2"] = 5_577

    # # Ref: https://www.nafo.int/Portals/0/PDFs/sc/2021/scr21-020.pdf -- Table B4, Landings in 2020
    # ds_sn_mask4 = weights_ds["ASFIS Scientific Name"] == "Hippoglossoides platessoides"
    # ds_loc_mask5 = weights_ds["Location"] == "3LNO"
    # weights_ds.loc[ds_sn_mask4 & ds_loc_mask5, "Weight 2"] = 1_175

    # weights_ds

    weights_exp = (
        pd.concat([weights_rest, weights_sharks])
        .sort_values(["Area", "ASFIS Scientific Name", "Location"])
        .reset_index(drop=True)
    )

    # Progress bar
    tqdm.pandas()

    weights_exp["Normalized Weight"] = (
        weights_exp.groupby(["Area Specific", "ASFIS Scientific Name"])[
            ["Weight 1", "Weight 2"]
        ]
        .progress_apply(compute_weights)
        .reset_index(level=[0, 1], drop=True)
    )

    # Validate weight normalization
    validate_normalization(
        weights_exp, group_key=["Area Specific", "ASFIS Scientific Name"]
    )

    # weights = weights.drop(columns="Area Specific")

    def aggregate_weight(group, area_col="Area Specific", nw_col="Normalized Weight"):
        if len(group) == 1:
            return group[nw_col].iloc[0]
        else:
            return json.dumps(dict(zip(group[area_col], group[nw_col])))

    weights_final = (
        weights_exp.groupby(["Area", "ASFIS Scientific Name", "Location"])[
            ["Area Specific", "Normalized Weight"]
        ]
        .apply(aggregate_weight)
        .reset_index(name="Normalized Weight")
    )

    # Save assigned weights to output file
    file_path = os.path.join(output_dir, "stock_weights.xlsx")
    print(f"Saving stocks with weights to {file_path}")
    weights_final.to_excel(file_path, index=False)


if __name__ == "__main__":
    main()
