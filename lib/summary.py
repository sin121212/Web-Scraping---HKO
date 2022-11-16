# -*- coding: utf-8 -*-
"""
Created on Tue Nov 15 09:35:11 2022

@author: kitsin
"""

import pandas as pd
import numpy as np

import datetime

#%%

class Summary:
    
    def __init__(self, start_date, warning_df_list, weather_df_list):
        
        self.start_date = start_date
        self.warning_df_list = warning_df_list
        self.weather_df_list = weather_df_list
        
        self.daily_warning_df = self.daily_warning()
        self.daily_weather_df = self.daily_weather()
        self.custom_date_grouping_df = self.custom_date_grouping()
    
        self.custom_week_warning_summary_df = self.custom_week_warning_summary()
        self.custom_week_weather_summary_df = self.custom_week_weather_summary()
    
        pass
    
    def daily_warning(self):
        
        def create_df():
            
            data = {'Date': pd.date_range(start=self.start_date, end=datetime.date.today())}
            
            df = pd.DataFrame(data)

            # create_df
            return df
        
        def join_tables(df):
            
            for warn_df in self.warning_df_list:
                
                df = pd.merge(df,
                              warn_df,
                              how='left',
                              on='Date')

            return df
        
        def output_format(df):
            
            df = df.fillna('')
            
            return df
        
        def column_to_row(df):
            
            warning_col_list = [col for col in df if col.startswith('Warning')]
                        
            df = df.melt(id_vars=['Date'], 
                         var_name='Type',
                         value_vars=warning_col_list,
                         value_name='Value')
            
            return df
        
        df = (create_df()
              .pipe(join_tables)
              .pipe(output_format)
              .pipe(column_to_row)
              )
        
        # stat_daily
        return df

    def daily_weather(self):
        
        def create_df():
            
            data = {'Date': pd.date_range(start=self.start_date, end=datetime.date.today())}
            
            df = pd.DataFrame(data)

            # create_df
            return df
        
        def join_tables(df):
            
            for wea_df in self.weather_df_list:
                
                df = pd.merge(df,
                              wea_df,
                              how='left',
                              on='Date')

            return df
        
        def column_to_row(df):
            
            wea_col_list = ['Total Rainfall (mm)']
                        
            df = df.melt(id_vars=['Date'], 
                         var_name='Type',
                         value_vars=wea_col_list,
                         value_name='Value')
            
            return df
        
        df = (create_df()
              .pipe(join_tables)
              .pipe(column_to_row)
               )

        # daily_weather
        return df
        
    def custom_date_grouping(self):
        
        def create_df():
            
            data = {'Date': pd.date_range(start=self.start_date, end=datetime.date.today())}
            
            df = pd.DataFrame(data)

            # create_df
            return df
        
        def grouping(df):
            
            '''
            Date groupby 
                1. Year
                2. Week
            
            '''
            con_list, gp_list, yr_list = [], [], []
            
            # 2022
            con_list.append((df['Date'] >= '2021-12-20') & (df['Date'] <= '2022-01-16')), gp_list.append('Weeks 1-4'), yr_list.append('2022')
            con_list.append((df['Date'] >= '2022-01-16') & (df['Date'] <= '2022-02-13')), gp_list.append('Weeks 5-8'), yr_list.append('2022')
            con_list.append((df['Date'] >= '2022-02-14') & (df['Date'] <= '2022-03-13')), gp_list.append('Weeks 9-12'), yr_list.append('2022')
            con_list.append((df['Date'] >= '2022-03-14') & (df['Date'] <= '2022-04-10')), gp_list.append('Weeks 13-16'), yr_list.append('2022')
            con_list.append((df['Date'] >= '2022-04-11') & (df['Date'] <= '2022-05-08')), gp_list.append('Week 17-20'), yr_list.append('2022')
            con_list.append((df['Date'] >= '2022-05-09') & (df['Date'] <= '2022-06-05')), gp_list.append('Week 21-24'), yr_list.append('2022')
            con_list.append((df['Date'] >= '2022-06-06') & (df['Date'] <= '2022-07-03')), gp_list.append('Week 25-28'), yr_list.append('2022')
            con_list.append((df['Date'] >= '2022-07-04') & (df['Date'] <= '2022-07-31')), gp_list.append('Week 29-32'), yr_list.append('2022')
            con_list.append((df['Date'] >= '2022-08-01') & (df['Date'] <= '2022-08-28')), gp_list.append('Week 33-36'), yr_list.append('2022')
            con_list.append((df['Date'] >= '2022-08-29') & (df['Date'] <= '2022-09-04')), gp_list.append('Week 37'), yr_list.append('2022')
            con_list.append((df['Date'] >= '2022-09-05') & (df['Date'] <= '2022-09-11')), gp_list.append('Week 38'), yr_list.append('2022')
            con_list.append((df['Date'] >= '2022-09-12') & (df['Date'] <= '2022-09-18')), gp_list.append('Week 39'), yr_list.append('2022')
            con_list.append((df['Date'] >= '2022-09-19') & (df['Date'] <= '2022-09-25')), gp_list.append('Week 40'), yr_list.append('2022')
            con_list.append((df['Date'] >= '2022-09-26') & (df['Date'] <= '2022-10-02')), gp_list.append('Week 41'), yr_list.append('2022')
            con_list.append((df['Date'] >= '2022-10-03') & (df['Date'] <= '2022-10-09')), gp_list.append('Week 42'), yr_list.append('2022')
            con_list.append((df['Date'] >= '2022-10-10') & (df['Date'] <= '2022-10-16')), gp_list.append('Week 43'), yr_list.append('2022')
            con_list.append((df['Date'] >= '2022-10-17') & (df['Date'] <= '2022-10-23')), gp_list.append('Week 44'), yr_list.append('2022')
            con_list.append((df['Date'] >= '2022-10-24') & (df['Date'] <= '2022-10-30')), gp_list.append('Week 45'), yr_list.append('2022')
            con_list.append((df['Date'] >= '2022-10-31') & (df['Date'] <= '2022-11-06')), gp_list.append('Week 46'), yr_list.append('2022')

            # 2019
            con_list.append((df['Date'] >= '2018-12-21') & (df['Date'] <= '2019-01-17')), gp_list.append('Weeks 1-4'), yr_list.append('2019')
            con_list.append((df['Date'] >= '2019-01-17') & (df['Date'] <= '2019-02-14')), gp_list.append('Weeks 5-8'), yr_list.append('2019')
            con_list.append((df['Date'] >= '2019-02-15') & (df['Date'] <= '2019-03-14')), gp_list.append('Weeks 9-12'), yr_list.append('2019')
            con_list.append((df['Date'] >= '2019-03-15') & (df['Date'] <= '2019-04-11')), gp_list.append('Weeks 13-16'), yr_list.append('2019')
            con_list.append((df['Date'] >= '2019-04-12') & (df['Date'] <= '2019-05-09')), gp_list.append('Week 17-20'), yr_list.append('2019')
            con_list.append((df['Date'] >= '2019-05-10') & (df['Date'] <= '2019-06-06')), gp_list.append('Week 21-24'), yr_list.append('2019')
            con_list.append((df['Date'] >= '2019-06-07') & (df['Date'] <= '2019-07-04')), gp_list.append('Week 25-28'), yr_list.append('2019')
            con_list.append((df['Date'] >= '2019-07-05') & (df['Date'] <= '2019-08-01')), gp_list.append('Week 29-32'), yr_list.append('2019')
            con_list.append((df['Date'] >= '2019-08-02') & (df['Date'] <= '2019-08-29')), gp_list.append('Week 33-36'), yr_list.append('2019')
            con_list.append((df['Date'] >= '2019-08-30') & (df['Date'] <= '2019-09-05')), gp_list.append('Week 37'), yr_list.append('2019')
            con_list.append((df['Date'] >= '2019-09-06') & (df['Date'] <= '2019-09-12')), gp_list.append('Week 38'), yr_list.append('2019')
            con_list.append((df['Date'] >= '2019-09-13') & (df['Date'] <= '2019-09-19')), gp_list.append('Week 39'), yr_list.append('2019')
            con_list.append((df['Date'] >= '2019-09-20') & (df['Date'] <= '2019-09-26')), gp_list.append('Week 40'), yr_list.append('2019')
            con_list.append((df['Date'] >= '2019-09-27') & (df['Date'] <= '2019-10-03')), gp_list.append('Week 41'), yr_list.append('2019')
            con_list.append((df['Date'] >= '2019-10-04') & (df['Date'] <= '2019-10-10')), gp_list.append('Week 42'), yr_list.append('2019')
            con_list.append((df['Date'] >= '2019-10-11') & (df['Date'] <= '2019-10-17')), gp_list.append('Week 43'), yr_list.append('2019')
            con_list.append((df['Date'] >= '2019-10-18') & (df['Date'] <= '2019-10-24')), gp_list.append('Week 44'), yr_list.append('2019')
            con_list.append((df['Date'] >= '2019-10-25') & (df['Date'] <= '2019-10-31')), gp_list.append('Week 45'), yr_list.append('2019')
            con_list.append((df['Date'] >= '2019-11-01') & (df['Date'] <= '2019-11-07')), gp_list.append('Week 46'), yr_list.append('2019')

            # 2021
            con_list.append((df['Date'] >= '2020-12-20') & (df['Date'] <= '2021-01-16')), gp_list.append('Weeks 1-4'), yr_list.append('2021')
            con_list.append((df['Date'] >= '2021-01-16') & (df['Date'] <= '2021-02-13')), gp_list.append('Weeks 5-8'), yr_list.append('2021')
            con_list.append((df['Date'] >= '2021-02-14') & (df['Date'] <= '2021-03-13')), gp_list.append('Weeks 9-12'), yr_list.append('2021')
            con_list.append((df['Date'] >= '2021-03-14') & (df['Date'] <= '2021-04-10')), gp_list.append('Weeks 13-16'), yr_list.append('2021')
            con_list.append((df['Date'] >= '2021-04-11') & (df['Date'] <= '2021-05-08')), gp_list.append('Week 17-20'), yr_list.append('2021')
            con_list.append((df['Date'] >= '2021-05-09') & (df['Date'] <= '2021-06-05')), gp_list.append('Week 21-24'), yr_list.append('2021')
            con_list.append((df['Date'] >= '2021-06-06') & (df['Date'] <= '2021-07-03')), gp_list.append('Week 25-28'), yr_list.append('2021')
            con_list.append((df['Date'] >= '2021-07-04') & (df['Date'] <= '2021-07-31')), gp_list.append('Week 29-32'), yr_list.append('2021')
            con_list.append((df['Date'] >= '2021-08-01') & (df['Date'] <= '2021-08-28')), gp_list.append('Week 33-36'), yr_list.append('2021')
            con_list.append((df['Date'] >= '2021-08-29') & (df['Date'] <= '2021-09-04')), gp_list.append('Week 37'), yr_list.append('2021')
            con_list.append((df['Date'] >= '2021-09-05') & (df['Date'] <= '2021-09-11')), gp_list.append('Week 38'), yr_list.append('2021')
            con_list.append((df['Date'] >= '2021-09-12') & (df['Date'] <= '2021-09-18')), gp_list.append('Week 39'), yr_list.append('2021')
            con_list.append((df['Date'] >= '2021-09-19') & (df['Date'] <= '2021-09-25')), gp_list.append('Week 40'), yr_list.append('2021')
            con_list.append((df['Date'] >= '2021-09-26') & (df['Date'] <= '2021-10-02')), gp_list.append('Week 41'), yr_list.append('2021')
            con_list.append((df['Date'] >= '2021-10-03') & (df['Date'] <= '2021-10-09')), gp_list.append('Week 42'), yr_list.append('2021')
            con_list.append((df['Date'] >= '2021-10-10') & (df['Date'] <= '2021-10-16')), gp_list.append('Week 43'), yr_list.append('2021')
            con_list.append((df['Date'] >= '2021-10-17') & (df['Date'] <= '2021-10-23')), gp_list.append('Week 44'), yr_list.append('2021')
            con_list.append((df['Date'] >= '2021-10-24') & (df['Date'] <= '2021-10-30')), gp_list.append('Week 45'), yr_list.append('2021')
            con_list.append((df['Date'] >= '2021-10-31') & (df['Date'] <= '2021-11-06')), gp_list.append('Week 46'), yr_list.append('2021')

            df['Week'] = np.select(con_list, gp_list, '')
            
            df['Year'] = np.select(con_list, yr_list, '')
            
            return df
        
        def checking(df):
            
            print(df['Week'].value_counts())
            
            print(df['Year'].value_counts())
            
            return df
        
        def filtering(df):
            
            '''
            Filter the useless date (e.g. 2021)
            '''
            
            df = df[df['Week'] != '']
            
            return df
        
        def week_order(df):
            
            '''
            Sort the week group by custom order
            '''
            
            con_list, cho_list = [], []
            
            con_list.append(df['Week'] ==  'Weeks 1-4'), cho_list.append(1)
            con_list.append(df['Week'] ==  'Weeks 5-8'), cho_list.append(2)
            con_list.append(df['Week'] ==  'Weeks 9-12'), cho_list.append(3)
            con_list.append(df['Week'] ==  'Weeks 13-16'), cho_list.append(4)
            con_list.append(df['Week'] ==  'Week 17-20'), cho_list.append(5)
            con_list.append(df['Week'] ==  'Week 21-24'), cho_list.append(6)
            con_list.append(df['Week'] ==  'Week 25-28'), cho_list.append(7)
            con_list.append(df['Week'] ==  'Week 29-32'), cho_list.append(8)
            con_list.append(df['Week'] ==  'Week 33-36'), cho_list.append(9)
            con_list.append(df['Week'] ==  'Week 37'), cho_list.append(10)
            con_list.append(df['Week'] ==  'Week 38'), cho_list.append(11)
            con_list.append(df['Week'] ==  'Week 39'), cho_list.append(12)
            con_list.append(df['Week'] ==  'Week 40'), cho_list.append(13)
            con_list.append(df['Week'] ==  'Week 41'), cho_list.append(14)
            con_list.append(df['Week'] ==  'Week 42'), cho_list.append(15)
            con_list.append(df['Week'] ==  'Week 43'), cho_list.append(16)
            con_list.append(df['Week'] ==  'Week 44'), cho_list.append(17)
            con_list.append(df['Week'] ==  'Week 45'), cho_list.append(18)
            con_list.append(df['Week'] ==  'Week 46'), cho_list.append(19)

            df['WeekOrder'] = np.select(con_list, cho_list, np.nan)     
            
            df['WeekOrder'] = df['WeekOrder'].astype(int)

            return df

        df = (create_df()
              .pipe(grouping)
              .pipe(checking)
              .pipe(filtering)
              .pipe(week_order)
              )
        
        # custom_date_grouping
        return df
    
    def custom_week_warning_summary(self):
    
        def call_df():
            
            df = self.custom_date_grouping_df.copy()
            
            return df
    
        def join_warning(df):
            
            df = pd.merge(df,
                          self.daily_warning_df,
                          how='left',
                          on='Date')
            
            return df
            
        def groupby(df):

            fun = lambda x: ';'.join(x[x != ''].unique())
                        
            df = df.groupby(['Year', 'Week', 'WeekOrder', 'Type'])['Value'].apply(fun)
            
            df = df.reset_index()
            
            return df
        
        df = (call_df()
              .pipe(join_warning)
              .pipe(groupby)
              )
        
        # custom_week_warning_summary
        return df
    
    def custom_week_weather_summary(self):
    
        def call_df():
            
            df = self.custom_date_grouping_df.copy()
            
            return df
    
        def join_weather(df):
            
            df = pd.merge(df,
                          self.daily_weather_df,
                          how='left',
                          on='Date')
            
            return df
            
        def groupby(df):
                        
            df = df.groupby(['Year', 'Week', 'WeekOrder', 'Type'])['Value'].sum()
            
            df = df.reset_index()
            
            return df
        
        def number_format(df):
            
            df['Value'] = df['Value'].round(2)
            
            return df
        
        df = (call_df()
              .pipe(join_weather)
              .pipe(groupby)
              .pipe(number_format)
              )
        
        # custom_week_weather_summary
        return df
    
    def custom_week_summary(self):
        
        def concat_df():
        
            df_list = [self.custom_week_warning_summary_df, self.custom_week_weather_summary_df]
        
            df = pd.concat(df_list)
        
            return df
        
        def datatype(df):
            
            df['Value'] = df['Value'].astype(str)
            
            return df
        
        def row_to_column(df):
            
            fun = lambda x: ';'.join(x[x != ''].unique())
            
            df = df.pivot_table(index=['Year', 'Type'],
                                columns=['WeekOrder', 'Week'],
                                values='Value', 
                                aggfunc=fun)
                
            return df
        
        df = (concat_df()
              .pipe(datatype)
              .pipe(row_to_column)
              )
        
        # custom_week_summary
        return df
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    # def stat_weekly(self):
        
    #     def select_df():
            
    #         df = self.daily_warning_df.copy()
            
    #         return df
        
    #     def reshape(df):
            
    #         '''
    #         Concat string if not '' by group
            
    #         '''
            
    #         df['Year'] = df['Date'].dt.year
    #         # df['Month'] = df['Date'].dt.month
    #         df['Week'] = df['Date'].dt.isocalendar().week
            
            
    #         fun = lambda x: ';'.join(x[x != ''].unique())
            
    #         warning_df = df.pivot_table(index=['Year', 'Week'],
    #                                     columns=['WarningType'],
    #                                     values='WarningValue', 
    #                                     aggfunc=fun)
            
    #         date_df = (df
    #                    .groupby(['Year', 'Week'])
    #                    .agg(
    #                        StartDate=('Date', 'min'), EndDate=('Date', 'max')
    #                        )
    #                    )
            
    #         df = warning_df.join(date_df)
            
    #         df = df.reset_index()
            
    #         df = df.set_index(['Year', 'Week', 'StartDate', 'EndDate'])
            
    #         return df
        
    #     df = (select_df()
    #           .pipe(reshape)
    #           )
                
    #     # stat_weekly
    #     return df
    
    
    