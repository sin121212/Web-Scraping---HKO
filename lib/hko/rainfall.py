# -*- coding: utf-8 -*-
"""
Created on Mon Nov 14 16:36:01 2022

@author: kitsin
"""

import requests
import selenium

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By

import time
import datetime

import pandas as pd

#%%
class Rainfall:
    
    def __init__(self, driver, start_date, end_date):
        
        self.driver = driver
        self.start_date = start_date
        self.end_date = end_date
        
        pass

    def web_scraping(self, yyyy, mm):
        
        def download_dataframe():
            
            '''
            Download by (1) Year (2) Month
            '''
            
            print(f'Rainfall: {yyyy}-{mm}')
        
            self.driver.get(f'https://www.hko.gov.hk/en/cis/dailyExtract.htm?y={yyyy}&m={mm}')
    
            time.sleep(2)
    
            # download table
            table = self.driver.find_element(By.ID, 't1')
            rows = table.find_elements(By.TAG_NAME, "tr")
            
            list_list = []
            
            for row in rows:
                
                row_list = []
                
                cols = row.find_elements(By.TAG_NAME, 'td')
                
                for col in cols:
                    row_list.append(col.text)
                    
                list_list.append(row_list)
                                    
            df = pd.DataFrame(list_list)
            
            return df
        
        def filter_column(df):
            
            df = df.iloc[:, : 9]
            
            return df
        
        def assign_column_name(df):
            
            col_name_list = ['Day', 'Mean Pressure (hPa)', 'Absolute Daily Max (deg. C)',
                             'Mean (deg. C)', 'Absolute Daily Min (deg. C)', 
                             'Mean Dew Point (deg. C)', 'Mean Relative Humidity (%)', 'Mean Amount of Cloud (%)',
                             'Total Rainfall (mm)']
            
            df.columns = col_name_list            
            
            return df

        def datetime_format(df):
            
            df['DateString'] = yyyy + mm + df['Day']
            
            df['Date'] = pd.to_datetime(df['DateString'], errors='coerce', format='%Y%m%d')
            
            return df
        
        def filtering_row(df):
            
            df = df[df['Date'].notnull()]            
            
            return df
        
        def output_format(df):
                        
            col_list = ['Date', 'Total Rainfall (mm)']
            df = df[col_list].copy()         
            
            df['Total Rainfall (mm)'] = pd.to_numeric(df['Total Rainfall (mm)'], errors='coerce')
            df['Total Rainfall (mm)'] = df['Total Rainfall (mm)'].fillna(0)

            return df
        
        def close_browser(df):
            
            self.driver.quit()
            time.sleep(2)
            
            return df

        df = (download_dataframe()
              .pipe(filter_column)
              .pipe(assign_column_name)
              .pipe(datetime_format)
              .pipe(filtering_row)
              .pipe(output_format)
               # .pipe(close_browser)
              )
        
        return df

    def loop_web_scraping(self):
        
        def create_df():
            
            data = {'Date': pd.date_range(start=self.start_date, end=self.end_date)}
            
            df = pd.DataFrame(data)

            # create_df
            return df
        
        def get_data_part_string(df):
            
            df['YearString'] = df['Date'].dt.strftime('%Y')
            df['MonthString'] = df['Date'].dt.strftime('%m')

            return df
        
        def get_unique_yyyy_mm(df):
            
            col_list = ['YearString', 'MonthString']
            
            df = df[col_list]
            
            df = df.drop_duplicates()
            
            return df
        
        def download_concat_data(df):
            
            download_df_list = []
            
            for index, row in df.iterrows():
                
                download_df = self.web_scraping(yyyy=row['YearString'], mm=row['MonthString'])
                
                download_df_list.append(download_df)
                
            df = pd.concat(download_df_list)
            
            return df
        
        df = (create_df()
            .pipe(get_data_part_string)
            .pipe(get_unique_yyyy_mm)
            .pipe(download_concat_data)
              )
        
        return df





#%%




