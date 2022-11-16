# -*- coding: utf-8 -*-
"""
Created on Mon Nov 14 10:51:13 2022

@author: kitsin
"""

import requests
import selenium

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By

import time
import datetime

#%%
'''
1. Tropical Cyclone Warning

    Date
    WarningTropical
    
'''
from lib.hko.tropical import Tropical

tropical = Tropical(url='https://www.weather.gov.hk/en/wxinfo/climat/warndb/warndb1.shtml',
                    driver=webdriver.Chrome(service=Service('C:\chromedriver_win32\chromedriver.exe')),
                    start_date='201901', 
                    end_date=datetime.date.today().strftime('%Y%m'))

tropical_df = tropical.web_scraping()
    
#%%
'''
2. Strong Monsoon

    Date
    WarningStrongMonsoon

'''
from lib.hko.strong_monsoon import StrongMonsoon

strong_monsoon = StrongMonsoon(url='https://www.weather.gov.hk/en/wxinfo/climat/warndb/warndb2.shtml',
                               driver=webdriver.Chrome(service=Service('C:\chromedriver_win32\chromedriver.exe')),
                               start_date='201901', 
                               end_date=datetime.date.today().strftime('%Y%m'))

strong_monsoon_df = strong_monsoon.web_scraping()

#%%
'''
3. Rainstorm

    Date
    WarningStrongMonsoon

'''
from lib.hko.rainstorm import Rainstorm

rainstorm = Rainstorm(url='https://www.weather.gov.hk/en/wxinfo/climat/warndb/warndb3.shtml',
                      driver=webdriver.Chrome(service=Service('C:\chromedriver_win32\chromedriver.exe')),
                      start_date='201901', 
                      end_date=datetime.date.today().strftime('%Y%m'))

rainstorm_df = rainstorm.web_scraping()

#%%
'''
4. Cold Weather

    Date
    WarningColdWeather

'''
from lib.hko.cold_weather import ColdWeather

cold_weather = ColdWeather(url='https://www.weather.gov.hk/en/wxinfo/climat/warndb/warndb12.shtml',
                           driver=webdriver.Chrome(service=Service('C:\chromedriver_win32\chromedriver.exe')),
                           start_date='201901', 
                           end_date=datetime.date.today().strftime('%Y%m'))

cold_weather_df = cold_weather.web_scraping()

#%%
'''
5. Hot Weather

    Date
    WarningHotWeather

'''
from lib.hko.hot_weather import HotWeather

hot_weather = HotWeather(url='https://www.weather.gov.hk/en/wxinfo/climat/warndb/warndb13.shtml',
                         driver=webdriver.Chrome(service=Service('C:\chromedriver_win32\chromedriver.exe')),
                         start_date='201901', 
                         end_date=datetime.date.today().strftime('%Y%m'))

hot_weather_df = hot_weather.web_scraping()

#%%
'''
6. Rainfall
    Date
    Rainfail
    
'''
from lib.hko.rainfall import Rainfall

rainfall = Rainfall(driver=webdriver.Chrome(service=Service('C:\chromedriver_win32\chromedriver.exe')),
                    start_date='2019-01-01', 
                    end_date=datetime.date.today())

# rainfall_df = rainfall.web_scraping(yyyy='2019', mm='01')

rainfall_df = rainfall.loop_web_scraping()

#%%
from lib.summary import Summary

warning_df_list = [tropical_df,
                   strong_monsoon_df,
                   rainstorm_df, 
                   cold_weather_df, 
                   hot_weather_df]

weather_df_list = [rainfall_df]

summary = Summary(start_date='2019-01-01', 
                  warning_df_list=warning_df_list, 
                  weather_df_list=weather_df_list)

daily_warning_df = summary.daily_warning_df
daily_weather_df = summary.daily_weather_df

custom_date_grouping_df = summary.custom_date_grouping_df

custom_week_warning_summary_df = summary.custom_week_warning_summary_df
custom_week_weather_summary_df = summary.custom_week_weather_summary_df

custom_week_summary_df = summary.custom_week_summary()

# custom_week_summary_df.to_excel('custom_week_summary_df.xlsx')

# stat_weekly_df = summary.stat_weekly()

#%%





