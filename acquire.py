import pandas as pd
import requests
import os

def build_df(base_url, entry):
    '''
    Takes in base url and entry of json you are interested in and creates df for all pages
    '''
    response = requests.get(base_url + '/api/v1/' + entry)
    n = response.json()['payload']['max_page']
    pages = []
    
    for page in range(1, n+1):
        response = requests.get(base_url + '/api/v1/' + entry + '?page=' + str(page))
        pages += response.json()['payload'][entry]
        
    df = pd.DataFrame(pages)

    return df

def get_data(base_url, entry):
    '''
    This function reads data, writes data to a csv file if a local file does not exist, and returns a df.
    '''
    if os.path.isfile(entry + '.csv'):
        
        # If csv file exists, read in data from csv file.
        df = pd.read_csv(entry + '.csv', index_col=0)
        
    else:
        
        # Read fresh data from db into a DataFrame.
        df = build_df(base_url, entry)
        
        # Write DataFrame to a csv file.
        df.to_csv(entry + '.csv')
        
    return df

def combined_df():
    '''
    Returns combined df of items, stores, and sales
    '''
    items = get_data(base_url='https://python.zgulde.net', entry='items')
    stores = get_data(base_url='https://python.zgulde.net', entry='stores')
    sales = get_data(base_url='https://python.zgulde.net', entry='sales')
    combined = sales.merge(items, how='left', left_on='item', right_on='item_id')
    combined = combined.merge(stores, how='left', left_on='store', right_on='store_id')
    return combined

def get_power():
    '''
    Returns df of Open Power Systems Data for Germany
    '''
    power = pd.read_csv('https://raw.githubusercontent.com/jenfly/opsd/master/opsd_germany_daily.csv')
    return power