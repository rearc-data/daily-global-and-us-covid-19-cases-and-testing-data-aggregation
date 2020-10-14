import os
import boto3
import datetime 
import pandas as pd
from s3_md5_compare import md5_compare

# link to datasets
nytimes = {
    "us": "https://raw.githubusercontent.com/nytimes/covid-19-data/master/us.csv", 
    "states": "https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-states.csv", 
    "counties": "https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv" 
}

owid = {
    "data": "https://covid.ourworldindata.org/data/owid-covid-data.csv"
}

dataapi = {
    "us-summary": "https://covidtracking.com/data/download/national-history.csv", 
    "states": "https://covidtracking.com/data/download/all-states-history.csv"
}

# Final column names
covid_global_columns = ['geographic_level', 'country_name', 'country_iso2', 'country_iso3',
       'state_fips', 'state_name', 'county_fips', 'county_name', 'area_name',
       'lat', 'long', 'population', 'date', 'cases', 'deaths', 'tests',
       'tests_pending', 'tests_negative', 'tests_positive', 'tests_units',
       'patients_icu', 'patients_hosp', 'patients_vent', 'recovered',
       'version_timestamp']
covid_global_countries_columns = ['country_name', 'country_iso2', 'country_iso3', 'lat', 'long',
       'population', 'date', 'cases', 'deaths', 'tests', 'tests_units']
covid_us_counties_columns = ['state_fips', 'state_name', 'county_fips', 'county_name', 'area_name',
       'lat', 'long', 'date', 'cases', 'deaths']
covid_us_states_columns = ['state_fips', 'state_name', 'lat', 'long', 'date', 'cases', 'deaths',
       'tests_positive', 'tests_negative', 'tests_pending', 'tests',
       'patients_icu', 'patients_hosp', 'patients_vent', 'recovered']


def source_dataset():
    ny_us = pd.read_csv(nytimes["us"])
    ny_states = pd.read_csv(nytimes["states"])
    ny_counties = pd.read_csv(nytimes["counties"])
    owid_data = pd.read_csv(owid["data"])
    dataapi_us = pd.read_csv(dataapi["us-summary"])
    dataapi_states = pd.read_csv(dataapi["states"])

    data_dir = '/tmp'
    if not os.path.exists(data_dir):
        os.mkdir(data_dir)

    county_codes = pd.read_csv('county_codes.csv')
    state_codes = pd.read_csv('state_codes.csv')
    country_codes = pd.read_csv('country_codes.csv')

    # state data file
    covid_us_states = dataapi_states.copy()
    
    covid_us_states = covid_us_states.rename(columns={
        "negative": "tests_negative",
        "positive": "tests_positive",
        "pending": "tests_pending",
        "totalTestResults": "tests",
        "hospitalizedCurrently": "patients_hosp",
        "inIcuCurrently": "patients_icu",
        "onVentilatorCurrently": "patients_vent"
    })

    covid_us_states = covid_us_states.set_index('state').join(
                    state_codes[['state_name', 'post_code', 'state_fips', 'lat', 'long']].set_index('post_code'), 
                     how='left').reset_index()

    right_df = ny_states.rename(columns={'state':'state_name'})
    covid_us_states = pd.merge(covid_us_states, right_df[['date', 'state_name', 'cases', 'deaths']], 
                                how='left', 
                                on=['date', 'state_name'])

    covid_us_states = covid_us_states[covid_us_states_columns]

    covid_us_states.to_csv(os.path.join(data_dir, 'covid_19_us_states.csv'), index=False)


    # county data file
    covid_us_counties = ny_counties.copy()

    covid_us_counties = covid_us_counties.rename(columns={
                            "county": "county_name", 
                            "state": "state_name",
                            "fips": "county_fips"
                        })

    covid_us_counties = covid_us_counties.set_index('state_name').join(
        state_codes[['state_name', 'state_fips']].set_index('state_name'), 
        how='left').reset_index()

    covid_us_counties = covid_us_counties.set_index('county_name').join(
        county_codes[['county_name', 'lat', 'long']].set_index('county_name'),
        how='left').reset_index()

    covid_us_counties['area_name'] = None
    covid_us_counties_columns = ['state_fips', 'state_name', 'county_fips', 'county_name', 'area_name',
        'lat', 'long', 'date', 'cases', 'deaths']
    covid_us_counties = covid_us_counties[covid_us_counties_columns]

    covid_us_counties.to_csv(os.path.join(data_dir, 'covid_19_us_counties.csv'), index=False)

    # global country data file
    covid_global_countries = owid_data.copy()

    covid_global_countries = covid_global_countries.rename(columns={
        "location": "country_name", 
        "total_cases": "cases", 
        "total_deaths": "deaths", 
        "total_tests": "tests",
        "tests_units": "tests_units"
    })

    covid_global_countries = covid_global_countries.set_index("country_name").join(
                                country_codes.set_index("country_name"), how="left").reset_index()

    covid_global_countries = covid_global_countries[covid_global_countries_columns]

    covid_global_countries.to_csv(os.path.join(data_dir, 'covid_19_global_countries.csv'), index=False)


    # global all regions data file

    now = datetime.datetime.now()
    version_timestamp = now.strftime('%Y%m%d%H%M')

    covid_global = owid_data[[
        'continent', 'location', 'date', 'total_cases', 
        'total_deaths', 'total_tests', 'tests_units', 'population']].copy()

    # counties and states parts
    counties = covid_us_counties.copy()
    states = covid_us_states.copy()

    counties['geographic_level'] = 'US County'
    states['geographic_level'] = 'US State'
    counties['country_name'] = 'United States'
    states['country_name'] = 'United States'

    states = states.merge(country_codes[['country_name', 'country_iso2', 'country_iso3']], how='left', on='country_name')
    counties = counties.merge(country_codes[['country_name', 'country_iso2', 'country_iso3']], how='left', on='country_name')

    # world part
    world = covid_global[covid_global['location'] == 'World'].copy()
    world['geographic_level'] = 'Global'
    world['country_iso3'] = 'OWID_WRL'

    world = world.rename(columns={
        "total_deaths": "deaths",
        "total_cases": "cases",
        "total_tests": "tests"
    })

    # international part
    international = covid_global[covid_global['location'] == 'International'].copy()
    international['geographic_level'] = 'Country'
    international['country_name'] = 'International'

    international = international.rename(columns={
        "total_deaths": "deaths",
        "total_cases": "cases",
        "total_tests": "tests"
    })

    # us part
    us = covid_global[covid_global['location'] == 'United States'].copy()
    us['geographic_level'] = 'Country'
    us['country_name'] = 'United States'

    us_cases = dataapi_us.copy()

    us_cases = us_cases.rename(columns={
        "negative": "tests_negative",
        "positive": "tests_positive",
        "totalTestResults": "tests",
        "hospitalizedCurrently": "patients_hosp",
        "inIcuCurrently": "patients_icu",
        "onVentilatorCurrently": "patients_vent"
    })

    right_df = ny_us.rename(columns={'state':'state_name'})
    us_cases = pd.merge(us_cases, right_df[['date', 'cases']], 
                        how='left', 
                        on=['date'])

    us_cases = us_cases[['date', 'tests', 'patients_icu', 'patients_hosp', 'cases', 
       'tests_negative', 'patients_vent', 'tests_positive', 
       'recovered']].copy()

    us = us.merge(us_cases, how='left', on='date')

    us = us[[
        'continent', 'date', 'cases',
       'total_deaths', 'tests', 
       'tests_units', 
       'population', 'geographic_level',
       'country_name', 'patients_icu', 'patients_hosp', 'patients_vent', 
       'tests_negative', 'tests_positive', 'recovered' 
    ]]

    us = us.rename(columns={
        "total_deaths": "deaths",
    })

    us = us.merge(country_codes[['country_name', 'country_iso2', 'country_iso3', 'lat', 'long']], how='left', on='country_name')

    # countries part
    countries = covid_global[(covid_global['location'] != 'International') & \
                         (covid_global['location'] != 'World') & \
                         (covid_global['location'] != 'United States')].copy()
    countries = countries.rename(columns={'location': 'country_name'})
    countries['geographic_level'] = 'Country'

    countries = countries.rename(columns={
        "total_deaths": "deaths",
        "total_cases": "cases",
        "total_tests": "tests"
    })

    countries = countries.merge(country_codes[['country_name', 'country_iso2', 'country_iso3', 'lat', 'long']], how='left', on='country_name')
    
    # add missing columns
    data_parts = [us, states, counties, countries, international, world]
    for df in data_parts:
        cols = covid_global_columns 
        for col in cols:
            if col not in df.columns:
                df[col] = None
        df = df[covid_global_columns]

    # concatenate all together
    merged = pd.concat(data_parts, join='outer', ignore_index = True)
    merged['version_timestamp'] = version_timestamp

    merged = merged[covid_global_columns]

    merged.to_csv(os.path.join(data_dir, 'covid_19_global.csv'), index=False)

    # upload to s3
    data_set_name = os.environ['DATA_SET_NAME']

    s3_bucket = os.environ['S3_BUCKET']
    s3 = boto3.client('s3')

    s3_uploads = []
    asset_list = []

    for r, d, f in os.walk(data_dir):
        for filename in f:
            obj_name = os.path.join(r, filename).split('/', 3).pop().replace(' ', '_').lower()
            file_location = os.path.join(r, filename)
            new_s3_key = data_set_name + '/dataset/' + obj_name

            has_changes = md5_compare(s3, s3_bucket, new_s3_key, file_location)
            if has_changes:
                s3.upload_file(file_location, s3_bucket, new_s3_key)
                print('Uploaded: ' + filename)
            else:
                print('No changes in: ' + filename)

            asset_source = {'Bucket': s3_bucket, 'Key': new_s3_key}
            s3_uploads.append({'has_changes': has_changes, 'asset_source': asset_source})

    count_updated_data = sum(upload['has_changes'] == True for upload in s3_uploads)
    if count_updated_data > 0:
        asset_list = list(map(lambda upload: upload['asset_source'], s3_uploads))
        if len(asset_list) == 0:
            raise Exception('Something went wrong when uploading files to s3')

    # asset_list is returned to be used in lamdba_handler function
    # if it is empty, lambda_handler will not republish
    return asset_list

# if __name__ == '__main__':
#     source_dataset()