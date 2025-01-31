import pandas as pd
import requests
import zipfile
from io import BytesIO

# Specify the file path of the fileinfo file
file_path = 'C:\\usa_api_fileinfo\\p_ct_2022_2023'

# Specify columns to keep
columns_to_keep = [
    "contract_transaction_unique_key", "contract_award_unique_key", "award_id_piid",
    "modification_number", "transaction_number", "parent_award_id_piid",
    "parent_award_modification_number", "base_and_exercised_options_value",
    "action_date", "recipient_uei", "recipient_duns", "recipient_name",
    "cage_code", "recipient_parent_uei", "recipient_parent_duns",
    "recipient_parent_name", "primary_place_of_performance_country_code",
    "primary_place_of_performance_country_name", "primary_place_of_performance_city_name",
    "primary_place_of_performance_state_code", "primary_place_of_performance_state_name",
    "primary_place_of_performance_zip_4", "prime_award_transaction_place_of_performance_cd_original",
    "prime_award_transaction_place_of_performance_cd_current", "action_type_code",
    "action_type", "product_or_service_code", "product_or_service_code_description",
    "dod_claimant_program_code", "dod_claimant_program_description", "naics_code",
    "naics_description", "dod_acquisition_program_code", "dod_acquisition_program_description",
    "usaspending_permalink", "last_modified_date"
]

# Read the file info CSV into a df
file_df = pd.read_csv(file_path)

# Loop through each row in the file_df
for index, row in file_df.iterrows():
    # Extract the download URL and year from the current row
    download_url = row['download_url']
    year = row['year']

    # Perform a GET request to download the zip file
    response = requests.get(download_url)

    # Check if the request was successful
    if response.status_code == 200:
        # Open the zip file
        with zipfile.ZipFile(BytesIO(response.content)) as zip_file:
            # Create an empty df for the current year
            df_year = pd.DataFrame()

            # Loop through all files in the zip file
            for file_name in zip_file.namelist():
                # Open and read the text file
                with zip_file.open(file_name) as text_file:
                    # Read the pipe delimited text file into a temporary df
                    temp_df = pd.read_csv(text_file, delimiter='|')

                    # Keep only the specified columns
                    temp_df = temp_df[columns_to_keep]

                    # Append the data to the year's df
                    df_year = pd.concat([df_year, temp_df], ignore_index=True)

            # Name the df based on the year it corresponds with
            df_name = f'df_{year}'
            globals()[df_name] = df_year

            print(f"df for year {year} created with {len(df_year)} rows.")
            print(df_year.head())
    else:
        print(f"Failed to download the file for year {year}. Status code: {response.status_code}")
