import pandas as pd
import requests
import zipfile
from io import BytesIO

# Specify the file path
file_path = 'C:\\usa_api_fileinfo\\p_ct_2020_2024'

# Read the file info CSV into a df
file_df = pd.read_csv(file_path)

# Loop through each row in the df
for index, row in file_df.iterrows():
    # Extract the download URL and year from the current row
    download_url = row['download_url']
    year = row['year']
    
    # Perform a GET request to download the zip file
    response = requests.get(download_url)
    
    # Check if the request was successful
    if response.status_code == 200:
        # Open the zip file from the response content
        with zipfile.ZipFile(BytesIO(response.content)) as zip_file:
            # Extract the first file within the zip file
            with zip_file.open(zip_file.namelist()[0]) as text_file:
                # Read the pipe delimited text file into a df
                df = pd.read_csv(text_file, delimiter='|')
                
                # Name the df based on the year it corresponds with
                df_name = f'df_{year}'
                globals()[df_name] = df
                
                # Print the head of the df
                print(f"Head of df for year {year}:")
                print(df.head())
    else:
        print(f"Failed to download the file for year {year}. Status code: {response.status_code}")
