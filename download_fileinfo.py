# 1) Creates a pandas dataframe containing information from a usaspending API bulk download POST request
# 2) Downloads df to current directory as a csv

start_date = "2024-12-01"
end_date = "2024-12-31"
status_url = "https://api.usaspending.gov/api/v2/download/status?file_name=All_PrimeTransactions_2025-01-29_H14M09S08664008.zip"
filename = "All_PrimeTransactions_2025-01-29_H14M09S08664008.zip"
file_url = "https://files.usaspending.gov/generated_downloads/All_PrimeTransactions_2025-01-29_H14M09S08664008.zip"


infofile = ('prime_ct_{}_{}'.format(start_date, end_date)).replace('-', "")

print(infofile)
