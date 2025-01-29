# 1) Creates a pandas dataframe containing information from a usaspending API bulk download POST request
# 2) Downloads df to a specified filepath as a csv

import pandas as pd

start_date = "2024-12-01"
end_date = "2024-12-31"
status_url = "https://api.usaspending.gov/api/v2/download/status?file_name=All_PrimeTransactions_2025-01-29_H14M09S08664008.zip"
filename = "All_PrimeTransactions_2025-01-29_H14M09S08664008.zip"
file_url = "https://files.usaspending.gov/generated_downloads/All_PrimeTransactions_2025-01-29_H14M09S08664008.zip"
folder = 'C:\\usa_api_fileinfo\\'

infofile = ('prime_ct_{}_{}'.format(start_date, end_date)).replace('-', "")

print(infofile)

infodata = {'infofile':[infofile],
            'start_date':[start_date],
            'end_date':[end_date],
            'status_url':[status_url],
            'filename':[filename],
            'file_url':[file_url]}

info_df = pd.DataFrame(infodata)

# Set option to display all columns and rows
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

print(info_df)

filepath = (folder+infofile)
print(filepath)

info_df.to_csv(path_or_buf=filepath, sep=',', header=True, index=False)
