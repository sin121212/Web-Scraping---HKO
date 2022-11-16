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
class HotWeather:
    
    def __init__(self, url, driver, start_date, end_date):
        
        self.url = url
        self.driver = driver
        self.start_date = start_date
        self.end_date = end_date
        
        pass

    def web_scraping(self):
        
        def download_dataframe():
        
            self.driver.get(self.url)
    
            time.sleep(2)
    
            # startdate
            startdate_ele = self.driver.find_element(By.ID, 'startdate')
            startdate_ele.clear()
            startdate_ele.send_keys(self.start_date)
    
            # enddate
            enddate_ele = self.driver.find_element(By.ID, 'enddate')
            enddate_ele.clear()
            enddate_ele.send_keys(self.end_date)
    
            time.sleep(2)
    
            # click submit
            self.driver.find_element(By.ID, 'warningsearch').click()
    
            time.sleep(2)
    
            # download table
            table = self.driver.find_element(By.ID, 'result')
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
        
        def assign_column_name(df):
            
            col_name_list = ['StartTime', 'StartDate', 'EndTime', 'EndDate', 'Duration']
            
            df.columns = col_name_list            
            
            return df
            
        def filtering(df):
            
            df = df[df['StartDate'].notnull()]            
            
            return df
        
        def datetime_format(df):
            
            df['Date'] = pd.to_datetime(df['StartDate'], errors='coerce', format='%d/%b/%Y')
            
            return df
        
        def output_format(df):
            
            df['WarningHotWeather'] = 'Hot Weather'
            
            col_list = ['Date', 'WarningHotWeather']
            
            df = df[col_list]
            
            return df
        
        def close_browser(df):
            
            self.driver.quit()
            time.sleep(2)
            
            return df

        df = (download_dataframe()
                .pipe(assign_column_name)
                .pipe(filtering)
                .pipe(datetime_format)
                .pipe(output_format)
                .pipe(close_browser)
              )
        
        return df

#%%




