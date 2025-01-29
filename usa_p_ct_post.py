# 1) Initiates POST requests to the usaspending bulk downloads API endpoint for a specified range of years
# 2) Generates zip files containing prime contracts awarded by the DoD
# 3) Parses the json response and extracts information necessary to access the generated zip files
# 4) Converts this file information into a Pandas df
# 5) Downloads the df to a specified filepath

import requests
import pandas as pd

# Variables
# Destination folder
folder = "C:\\usa_api_fileinfo\\"
# Start and end year
start_year = 2020
end_year = 2024


base_url = "https://api.usaspending.gov/api/v2/bulk_download/awards/"
# Parameters for the POST request
prime_award_types = ["A", "B", "C", "D", "IDV_A", "IDV_B", "IDV_B_A", "IDV_B_B", "IDV_B_C", "IDV_C", "IDV_D", "IDV_E"]
columns = ["contract_transaction_unique_key", "contract_award_unique_key", "award_id_piid", "modification_number", "transaction_number", "parent_award_id_piid", "parent_award_modification_number", "federal_action_obligation", "total_dollars_obligated", "total_outlayed_amount_for_overall_award", "base_and_exercised_options_value", "current_total_value_of_award", "base_and_all_options_value", "potential_total_value_of_award", "action_date", "action_date_fiscal_year", "period_of_performance_start_date", "period_of_performance_current_end_date", "period_of_performance_potential_end_date", "ordering_period_end_date", "solicitation_date", "awarding_sub_agency_code", "awarding_sub_agency_name", "awarding_office_code", "awarding_office_name", "funding_sub_agency_code", "funding_sub_agency_name", "funding_office_code", "funding_office_name", "treasury_accounts_funding_this_award", "federal_accounts_funding_this_award", "object_classes_funding_this_award", "recipient_uei", "recipient_duns", "recipient_name", "recipient_name_raw", "recipient_doing_business_as_name", "cage_code", "recipient_parent_uei", "recipient_parent_duns", "recipient_parent_name", "recipient_parent_name_raw", "recipient_country_code", "recipient_country_name", "recipient_address_line_1", "recipient_address_line_2", "recipient_city_name", "recipient_state_code", "recipient_state_name", "recipient_zip_4_code", "prime_award_transaction_recipient_cd_original", "prime_award_transaction_recipient_cd_current", "recipient_phone_number", "recipient_fax_number", "primary_place_of_performance_country_code", "primary_place_of_performance_country_name", "primary_place_of_performance_city_name", "primary_place_of_performance_state_code", "primary_place_of_performance_state_name", "primary_place_of_performance_zip_4", "prime_award_transaction_place_of_performance_cd_original", "prime_award_transaction_place_of_performance_cd_current", "award_or_idv_flag", "award_type_code", "award_type", "idv_type_code", "idv_type", "multiple_or_single_award_idv_code", "multiple_or_single_award_idv", "type_of_idc_code", "type_of_idc", "type_of_contract_pricing_code", "type_of_contract_pricing", "transaction_description", "prime_award_base_transaction_description", "action_type_code", "action_type", "solicitation_identifier", "number_of_actions", "product_or_service_code", "product_or_service_code_description", "dod_claimant_program_code", "dod_claimant_program_description", "naics_code", "naics_description", "domestic_or_foreign_entity_code", "domestic_or_foreign_entity", "dod_acquisition_program_code", "dod_acquisition_program_description", "country_of_product_or_service_origin_code", "country_of_product_or_service_origin", "place_of_manufacture_code", "place_of_manufacture", "extent_competed_code", "extent_competed", "solicitation_procedures_code", "solicitation_procedures", "other_than_full_and_open_competition_code", "other_than_full_and_open_competition", "number_of_offers_received", "program_acronym", "parent_award_type_code", "parent_award_type", "usaspending_permalink", "initial_report_date", "last_modified_date"]

# Create empty dictionary to store file information
file_info = {}

# Loop through each calendar year from start_year to end_year
for year in range(start_year, (end_year+1)):
    payload = {
        "filters": {
            "prime_award_types": prime_award_types,
            "agencies": [{"type": "awarding", "tier": "toptier", "name": "Department of Defense"}],
            "date_type": "action_date",
            "date_range": {
                "start_date": f"{year}-01-01",
                "end_date": f"{year}-12-31"
            }
        },
        "columns": columns
    }

    # Send POST request to the API
    response = requests.post(base_url, json=payload)

    # Check if the request was successful
    if response.status_code == 200:
        response_data = response.json()

        # Extract status_url, filename, and download_url
        status_url = response_data.get("status_url")
        filename = response_data.get("file_name")
        download_url = response_data.get("file_url")

        # Store the file information in the dictionary
        file_info[year] = {
            "status_url": status_url,
            "filename": filename,
            "download_url": download_url
        }

        print(f"Successfully processed year {year}: {status_url}")
    else:
        print(f"Failed to process year {year}. Status code: {response.status_code}")

# Convert file_info into pandas df
info_df = pd.DataFrame.from_dict(file_info, orient='index')

# Reset the index and add the year as a column
info_df.reset_index(inplace=True)
info_df.rename(columns={"index": "year"}, inplace=True)

filepath = (folder+f"p_ct_{start_year}_{end_year}")
print(filepath)

# Download info_df as csv to specified filepath
info_df.to_csv(path_or_buf=filepath, sep=',', header=True, index=False)
