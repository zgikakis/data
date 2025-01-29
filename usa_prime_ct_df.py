# Generates a zip file containing data on prime contracts awarded by the DoD for the specified time frame
# Generated zip file contains a pipe delimtted text file
# Specify start_date and end_date in YYYY-MM-DD for a period <= 1 year
# Smaller time selections will be generated significantly faster than large selections
# Uses usaspending API endpoint: https://api.usaspending.gov/api/v2/bulk_download/awards/
# 1) Sends post request to generate zip file
# 2) Periodically checks status of zip file generation
# 3) Once zip fine generation has finished: returns status_url, filename, and download_url
# 5) Prints status_url, filename, and download_url
# 4) Sends get request to download_url; extracts pipe delimitted text file from zip and converts to pandas dataframe
# 5) Prints the dataframe


import requests
import time
import zipfile
import io
import pandas as pd

#YYYY-MM-DD for a period less than 1 year
start_date = "2023-12-01"
end_date = "2023-12-31"

url = 'https://api.usaspending.gov/api/v2/bulk_download/awards/'
prime_award_types = ["A","B","C","D","IDV_A","IDV_B","IDV_B_A","IDV_B_B","IDV_B_C","IDV_C","IDV_D","IDV_E"]
columns = ["contract_transaction_unique_key", "contract_award_unique_key", "award_id_piid", "modification_number", "transaction_number", "parent_award_id_piid", "parent_award_modification_number", "federal_action_obligation", "total_dollars_obligated", "total_outlayed_amount_for_overall_award", "base_and_exercised_options_value", "current_total_value_of_award", "base_and_all_options_value", "potential_total_value_of_award", "action_date", "action_date_fiscal_year", "period_of_performance_start_date", "period_of_performance_current_end_date", "period_of_performance_potential_end_date", "ordering_period_end_date", "solicitation_date", "awarding_sub_agency_code", "awarding_sub_agency_name", "awarding_office_code", "awarding_office_name", "funding_sub_agency_code", "funding_sub_agency_name", "funding_office_code", "funding_office_name", "treasury_accounts_funding_this_award", "federal_accounts_funding_this_award", "object_classes_funding_this_award", "recipient_uei", "recipient_duns", "recipient_name", "recipient_name_raw", "recipient_doing_business_as_name", "cage_code", "recipient_parent_uei", "recipient_parent_duns", "recipient_parent_name", "recipient_parent_name_raw", "recipient_country_code", "recipient_country_name", "recipient_address_line_1", "recipient_address_line_2", "recipient_city_name", "recipient_state_code", "recipient_state_name", "recipient_zip_4_code", "prime_award_transaction_recipient_cd_original", "prime_award_transaction_recipient_cd_current", "recipient_phone_number", "recipient_fax_number", "primary_place_of_performance_country_code", "primary_place_of_performance_country_name", "primary_place_of_performance_city_name", "primary_place_of_performance_state_code", "primary_place_of_performance_state_name", "primary_place_of_performance_zip_4", "prime_award_transaction_place_of_performance_cd_original", "prime_award_transaction_place_of_performance_cd_current", "award_or_idv_flag", "award_type_code", "award_type", "idv_type_code", "idv_type", "multiple_or_single_award_idv_code", "multiple_or_single_award_idv", "type_of_idc_code", "type_of_idc", "type_of_contract_pricing_code", "type_of_contract_pricing", "transaction_description", "prime_award_base_transaction_description", "action_type_code", "action_type", "solicitation_identifier", "number_of_actions", "product_or_service_code", "product_or_service_code_description", "contract_bundling_code", "contract_bundling", "dod_claimant_program_code", "dod_claimant_program_description", "naics_code", "naics_description", "domestic_or_foreign_entity_code", "domestic_or_foreign_entity", "dod_acquisition_program_code", "dod_acquisition_program_description", "country_of_product_or_service_origin_code", "country_of_product_or_service_origin", "place_of_manufacture_code", "place_of_manufacture", "subcontracting_plan_code", "extent_competed_code", "extent_competed", "solicitation_procedures_code", "solicitation_procedures", "other_than_full_and_open_competition_code", "other_than_full_and_open_competition", "number_of_offers_received", "commercial_item_acquisition_procedures_code", "commercial_item_acquisition_procedures", "program_acronym", "parent_award_type_code", "parent_award_type", "parent_award_single_or_multiple_code", "parent_award_single_or_multiple", "consolidated_contract_code", "consolidated_contract", "contracting_officers_determination_of_business_size", "contracting_officers_determination_of_business_size_code", "usaspending_permalink", "initial_report_date", "last_modified_date"]

request = {"filters":
            {"prime_award_types":prime_award_types,
            "date_type":"action_date",
            "date_range":{"start_date":start_date,"end_date":end_date},
            "agencies":[{"type":"awarding","tier":"toptier","name":"Department of Defense"}]},
            "columns":columns,
            "file_format":"pstxt"
            }

def get_zip(url, request):
    print('Sending request to API')
    session_object = requests.Session()

    # Send request to API, if not successful, try again a max of 5 times
    for attempt in range(5):
        api_response = session_object.post(url=url, json=request, timeout=60)
        if api_response.status_code == 200:
            print("Request sent")
            print('the request sent was {}'.format(request))
            break
        else:
            time.sleep(5)

    # Get status url
    if api_response.status_code == 200:
        status_url = api_response.json()['status_url']
        filename = api_response.json()['file_name']
        file_url = api_response.json()['file_url']

        print("the file status url for this download is {}".format(status_url))
        print("the file name for this download is {}".format(filename))
        print("the file url for this download is {}".format(file_url))

        print('Waiting 5 minutes for this file to be generated')
        time.sleep(300)

        # Check status of file creation
        download_check(session_object, status_url)

        # Get zip file
        zip_file = session_object.get(file_url)
        if zip_file.status_code == 200:
            print('Successfully downloaded zip file')

    else:
        print('Did not receive 200 response from server')

    return(status_url, filename, file_url)

def download_check(session_object, status_url):
    for attempt in range(15):
        download_url = session_object.get(status_url)
        if download_url.json()['status'] == 'finished':
            print('the file is ready to be processed, downloading')
            break
        else:
            print(download_url.json()['status'])
            print('file has not finished generating. waiting an additional 10 minutes')
            time.sleep(600)

    return(200)


status_url, filename, file_url = get_zip(url=url, request=request)

print('status_url: {}'.format(status_url))
print('filename: {}'.format(filename))
print('file_url: {}'.format(file_url))


# Download the zip file
response = requests.get(file_url)
response.raise_for_status()  # Ensure the request was successful

# Extract the text file from the zip file
with zipfile.ZipFile(io.BytesIO(response.content)) as z:
    text_file_name = z.namelist()[0]
    with z.open(text_file_name) as f:
        # Read the pipe-delimited text file into a DataFrame
        df = pd.read_csv(f, delimiter='|')

# Display the DataFrame
print(df)
