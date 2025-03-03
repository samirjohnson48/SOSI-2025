# SOSI-2025

This repository contains the files used for the State of Stocks Index 2025.

## Requirements

All package requirements are listed in [requirements.txt](requirements.txt) and can be installed via
```
pip install -r requirements.txt
```

## Run Report

To produce all of the outputs in one go, simply run the [main.py](main.py) script, which runs all the scripts in [/scripts/](/scripts/) in the proper order.

```
python3 main.py
```

## Organization

The repository is organized into three folders: 

* [input](/input/): All inputs files for analysis
* [main](/scripts/): All scripts used for producing output files
* [utils](/utils/): All scripts containing utility functions used by the main processing scripts in the [/scripts/](/scripts/) directory.

### input

* [AB Stocks of India Jan2025.xlsx](/input/AB%20Stocks%20of%20India%20Jan2025.xlsx): Landings weights based on BMSY values for stocks in India (Areas 51, 57)
* [ASFIS_changes_2024.xlsx](/input/ASFIS_changes_2024.xlsx): ASFIS 2024 name changes
* [ASFIS_sp_2024.csv](/input/ASFIS_sp_2024.csv): ASFIS 2024 List of Species for Fishery Statistics Purposes
* [Complete_data_weighting.xlsx](/input/Complete_data_weighting.xlsx): Landings weights assigned for stocks in Areas 21, 27, and 67
* [corrected_scientific_names.json](/input/corrected_scientific_names.json): Dictionary map of ASFIS Scientific Name corrections for original scientific names listed in data
* [data_w_landings&weights.xlsx](/input/data_w_landings&weights.xlsx): Landings weights based on country catch values reported in Fishstat (priority weights) and manually assigned weights (secondary weights) for all stocks. This is the base file used for stock landings weights.
* [deep_sea_name_map.json](/input/deep_sea_name_map.json): Dictionary map of common name to ASFIS Scientific Name for Deep Sea stocks
* [global_aquaculture_production.csv](/input/global_aquaculture_production.csv): Global aquaculture production as reported in Fishstat.
* [global_capture_production.csv](/input/global_capture_production.csv): Global capture production as reported in Fishstat.
* [ISSCAAP_to_NEI.json](/input/ISSCAAP_to_NEI.json): Dictionary map of ISSCAAP Group Codes to corresponding NEI species listed in Fishstat. This is used for assigning proxy landings to stocks with no reported capture in Fishstat.
* [January overview - one table](/input/January%20overview%20-%20one%20table.xlsx): List of proxy species used for assigning proxy landings to similar stocks with no reported capture in Fishstat.
* [location_to_area.json](/input/location_to_area.json): Dictionary map of reported location of Deep Sea, Sharks, and Tuna stocks to FAO major fishing areas. Used for assigning landings to these stocks.
* [NOCS.xlsx](/input/NOCS.xlsx): Names of Countries and Territories (NOCS) used to map ISO3 country codes in Fishstat to the country short name.
* [overview_2025-02-04.xlsx]: List of stocks containing corrected Angolan stocks in Area 41.
* [sofia2024v2Oct31woTunasFinalchcecksMarch2024.xlsx](/input/sofia2024v2Oct31woTunasFinalchcecksMarch2024.xlsx): SOFIA 2024 stock assessments used for comparison between updated method (SOSI 2025) and previous method (SOFIA 2024).
* [updated_assessment_overview.xlsx](/input/updated_assessment_overview.xlsx): Base file of all stock assessments used in SOSI 2025 report. Contains sheets for all FAO major fishing areas (21, 27, 31, 34, 37, 41, 47, (48,58,88), 51, 57, 61, 67, 71, 77, 81, 87) and separate categories (Deep Sea, Salmon, Sharks, and Tunas) used in SOSI 2025 report.

### scripts

These are the scripts used to produce the analysis outputs. The outputs are produced in the [/output/] folder which is created upon running scripts in [/scripts/](/scripts/). They are separated into three subfolders: [/clean_data/], [/aggregate_tables/], and [/figures/]. 

To run any of these scripts individually, change directory to parent directory [SOSI-2025] as run the following (e.g. for [stock_assessments.py](/scripts/stock_assessments.py)):

```
python3 -m scripts.stock_assessments.py
```

The scripts are described below, in the order of the full data pipeline:

* [stock_assessments.py](/scripts/stock_assessments.py): Produces list of all stock assessments, with schema Area, ASFIS Scientific Name, Location, Tier, Status, and Uncertainty. Output includes two files: 
    * [stock_assessments.xlsx]: List of all assessed stocks.
    * [stock_assessments_w_unassessed.xlsx]: List of all assessed and unassessed stocks.
* [species_landings.py](/scripts/species_landings.py): Produces list of stocks with corresponding species landings from 1950-2021 as reported in Fishstat. Output file: [species_landings.xlsx] 
* [sofia_landings.py](/scripts/sofia_landings.py): Produces list of stocks from SOFIA 2024 data with corresponding 2021 stock landings as reported in Fishstat. Output file: [sofia_landings.xlsx]
* [stock_weights.py](/scripts/stock_weights.py): Produces list of stocks with corresponding normalized stock weights used to calculate 2021 stock landings. Output file: [stock_weights.xlsx]
* [stock_landings.py](/scripts/stock_landings.py): Produces list of stocks with computed 2021 stock landings. If proxy species is used for stock landings, it is listed under column 'Proxy Species'. Output file: [stock_landings.xlsx]
* [aggregate_tables.py](/scripts/aggregate_tables.py): Produces all aggregate tables used in report, using the output files in [/output/clean_data/] as inputs. Output is stored in folder [/output/aggregate_tables/].
* [capture_production_figures.py](/scripts/capture_production_figures.py): Produces all capture production analysis figures for the individual areas used in the report. Output figures are stored in [/output/figures/]. Data for the figures are stored in [/output/aggregate_tables/].
