import pandas as pd 
import os 
import seaborn as sns

import urllib
import zipfile
import datetime

class COT_REPORT:
    def __init__(self, data_directory):
#         df = self.load_data(data_directory)
#         self.data = df
#         self.tickers_list = df['Future'].values
        self.data_directory = data_directory
    def download_csv_data(self, year, data_directory=None):
        """Downloads COT Futures only data for a given year and 
        saves it as {year}.csv file in the dest_folder. 
        If dest_folder is not provided it defaults to the script
        directory.
        download url: f'https://www.cftc.gov/files/dea/history/deacot{year}.zip'
        """
        url = f'https://www.cftc.gov/files/dea/history/deacot{year}.zip'
        data_directory = self.data_directory if not data_directory else data_directory
        zip_path, _ = urllib.request.urlretrieve(url)
        with zipfile.ZipFile(zip_path, "r") as zf:
            zipinfos = zf.infolist()

            for zipinfo in zipinfos:
                zipinfo.filename = f'{year}.csv'
                zf.extract(zipinfo, data_directory)
                
    def download_since(self, year=1993):
        for y in range(year, datetime.datetime.today().year + 1):
            print(y)
            self.download_csv_data(y, 'data')
            
    def read_annual_cot_report(self, file, short=True, styled=True):
        """Reads report for full year from 
        https://cftc.gov/MarketReports/CommitmentsofTraders/HistoricalCompressed/index.htm
        Futures only reports 
        filename: annual
        """
        df = pd.read_csv(file)
        df.columns = df.columns.str.lstrip()
        if short == True:
        # Columns from SHORT report:
            monitored_cols = [
                'Market and Exchange Names',
                'As of Date in Form YYYY-MM-DD',
                'Noncommercial Positions-Long (All)',
                'Noncommercial Positions-Short (All)',
            ]

            df = df[monitored_cols]
        df = df.set_index('As of Date in Form YYYY-MM-DD')
        df.index = pd.to_datetime(df.index).date
        df.index.name = 'As of Date'
        split = df['Market and Exchange Names'].str.split(' - ', n=1, expand=True)
        df['Future'] = split[0]
        df['Exchange'] = split[1]
        df['Total'] = df['Noncommercial Positions-Long (All)'] + df['Noncommercial Positions-Short (All)'] 
        df['% Long'] = df['Noncommercial Positions-Long (All)'] / df['Total']
        df['% Short'] = df['Noncommercial Positions-Short (All)']  / df['Total']
        df['Net Pos'] = df['Noncommercial Positions-Long (All)'] - df['Noncommercial Positions-Short (All)']
        df = df.rename(columns={'Noncommercial Positions-Long (All)': 'Long NC',
                                'Noncommercial Positions-Short (All)': 'Short NC'})
        df = df.drop(columns='Market and Exchange Names')
        return df
        
    def load_data(self):
        """Iterates over the annual .csv files to load them into a DataFrame"""
        df = pd.DataFrame()
        for filename in os.listdir(self.data_directory):
            if filename.endswith(".csv"):
                path = os.path.join(self.data_directory, filename) 
                f = self.read_annual_cot_report(path, short=True)
                df = pd.concat([df, f])

        return df.sort_index(ascending=False)
    
    def style_df(self, df):
        cmap=sns.diverging_palette(10, 240, n=9, as_cmap=True)
        return (df.style.format({'% Long': '{:.0%}',
                       '% Short': '{:.0%}',
                       'Long NC': '{:,}',
                       'Short NC': '{:,}',
                       'Total': '{:,}',
                       'Net Pos': '{:,}'})
                  .background_gradient(cmap)
        )
    
    def get_future(self, name):
        df = self.load_data()
        df = df[df['Future'] == str.upper(name)]
        df = df.drop(columns=['Future', 'Exchange'])
    
        return Future(df)
        
        
class Future:
    def __init__(self, data):
        self.data = data
        
    def table(self, styled=False, rows=None):
        df = self.data if rows == None else self.data[:rows]
            
        if styled == True:
            return self.style_data(df)
        else:
            return df        
    def style_data(self):
        cmap=sns.diverging_palette(10, 240, n=9, as_cmap=True)
        return (self.data.style.format({'% Long': '{:.0%}',
                       '% Short': '{:.0%}',
                       'Long NC': '{:,}',
                       'Short NC': '{:,}',
                       'Total': '{:,}',
                       'Net Pos': '{:,}'})
                  .background_gradient(cmap))
    
    
    
    def load_summary(self):
        stat = pd.DataFrame()
        stat['min'] = self.data.min()
        stat['max'] = self.data.max()
        stat['rolling'] = self.data.head(12).mean().round()
        return stat.T.style.format({'Long NC': '{:,}',
                             'Short NC': '{:,}',
                             'Total': '{:,}',
                             '% Long': '{:.0%}',
                             '% Short': '{:.0%}',
                             'Net Pos': '{:,}',})    
