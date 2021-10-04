import pandas as pd
import numpy as np
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

def get_clean_power_data():
    '''
    Gets and cleans power data with imputation
    '''
    df = a.get_power() # call function from acquire to get data from .csv url
    df.Date = pd.to_datetime(df.Date) # make date column into datetime dtype
    df = df.set_index('Date') # set date column as index
    df['month'] = df.index.month_name() # add month name column
    df['year'] = df.index.year # add year column
    # Wind imputation
    df.Wind = np.where(df.year < 2010, 0, df.Wind) # fill missing values with 0s for dates before data was collected
    df['wind_previous_day'] = df.Wind.shift(1) # add a column for wind values from previous day to use for remaining imputation
    df.Wind[np.isnan(df.Wind)] = df.wind_previous_day # impute using value for previous day
    # Solar imputation
    df.Solar = np.where(df.year < 2012, 0, df.Solar) # fill missing values with 0s for dates before data was collected
    df['solar_previous_day'] = df.Solar.shift(1) # add a column for solar values from previous day to use for remaining imputation
    df.Solar[np.isnan(df.Solar)] = df.solar_previous_day # impute using value for previous day
    df['solar_previous_day'] = df.Solar.shift(1) # run again since missing values were grouped
    df.Solar[np.isnan(df.Solar)] = df.solar_previous_day # run again since missing values were grouped
    df['Wind+Solar'] = np.where(df.year < 2012, 0, df['Wind+Solar']) # set values to 0 for time before solar data was collected
    df['Wind+Solar'][np.isnan(df['Wind+Solar'])] = df.Wind + df.Solar # impute remaining missing values with sum
    df = df.drop(columns=['wind_previous_day', 'solar_previous_day']) # drop columns used for imputation that are no longer needed
    return df
    
    