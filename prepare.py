import pandas as pd
import acquire as a
import os

def clean_sales_data():
    '''
    Requires no inputs and returns cleaned sales data df
    '''
    df = a.combined_df() # call acquire function to get all data from API
    df = df.drop(columns=['item', 'store']) # drop redundant columns
    df.sale_date = pd.to_datetime(df.sale_date) # convert data column into datetime dtype
    df = df.set_index('sale_date') # set data column as index
    df['month'] = df.index.month_name() # add month name column
    df['day_of_week'] = df.index.day_name() # add day name column
    df['sales_total'] = df.sale_amount * df.item_price # add total sale amount column
    return df

def get_clean_sales_data():
    '''
    This function reads data, writes data to a csv file if a local file does not exist, and returns a df.
    '''
    if os.path.isfile('clean_sales_data.csv'):
        
        # If csv file exists, read in data from csv file.
        df = pd.read_csv('clean_sales_data.csv', index_col=0)
        
    else:
        
        # Read fresh data from db into a DataFrame and clean
        df = clean_sales_data()
        
        # Write DataFrame to a csv file.
        df.to_csv('clean_sales_data.csv')
        
    return df