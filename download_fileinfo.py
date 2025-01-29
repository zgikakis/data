# 1) Creates a pandas dataframe containing information from a usaspending API bulk download POST request
# 2) Downloads df to current directory as a csv

start_date = "2024-12-01"
end_date = "2024-12-31"

raw_filename = 'prime_ct_{}_{}'.format(start_date, end_date)
clean_filename = raw_filename.replace('-', "")

print(clean_filename)
