from datetime import date
from datetime import datetime
import pandas as pd
import numpy as np
import requests

MONTHS = {
    'January': 1,
    'February': 2,
    'March': 3,
    'April': 4,
    'May': 5,
    'June': 6,
    'July': 7,
    'August': 8,
    'September': 9,
    'October': 10,
    'November': 11,
    'December': 12
}


class Scraper(object):
    def __init__(self):
        self.url = 'https://finviz.com/insidertrading.ashx'
        self.header = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.75 Safari/537.36",
                       "X-Requested-With": "XMLHttpRequest"
                       }
        self.year = datetime.now().year
        self.df = None

    def get_df(self):

        page = requests.get(self.url, headers=self.header)
        self.df = pd.read_html(page.text, attrs={'class': 'body-table'},
                               index_col=["Date", "Ticker"], header=0,
                               parse_dates=True)[0]  # Read tables and select first from list

    def clean_df(self):
        self.df = self.df.loc[self.df['Transaction'].isin(
            ['Sale', 'Buy'])]  # removes option excercises
        self.df['#Shares'] = np.where(self.df['Transaction'] == 'Sale',
                                      self.df['#Shares'] * (-1), self.df['#Shares'])
        self.df['Value ($)'] = np.where(self.df['Transaction'] == 'Sale',
                                        self.df['Value ($)'] * (-1), self.df['Value ($)'])
        self.df = self.df.loc[:, ['#Shares', 'Value ($)']]
        self.df = self.df.groupby(['Date', 'Ticker']).sum()
        self.df = self.df.reset_index()
        self.df['Date'] = pd.to_datetime(self.df['Date'], format='%b %d')
        self.__fix_dates()
        self.df = self.df.set_index('Date')

    def split_by_month(self):
        # get months
        months = self.df.index.month_name().unique()
        for month in months:
            month_code = MONTHS[month]
            new_df = self.df[self.df.index.month == month_code]
            new_df = new_df.reset_index()
            new_df = new_df.set_index(['Date', 'Ticker'])
            try:
                if (month == 'December') and ('January' in months):
                    old_df = pd.read_csv(
                        f"Data/Insider_trans_{month}_{self.year - 1}.csv", index_col=['Date', 'Ticker'])
                else:
                    old_df = pd.read_csv(
                        f"Data/Insider_trans_{month}_{self.year}.csv", index_col=['Date', 'Ticker'])
                df_combined = old_df.append(new_df)
                df_combined = df_combined.drop_duplicates(keep='last')
                df_combined.sort_index()
                if month == 'December' and 'January' in months:
                    df_combined.to_csv(
                        f"Data/Insider_trans_{month}_{self.year - 1}.csv")
                else:  # Handle decembers
                    df_combined.to_csv(
                        f"Data/Insider_trans_{month}_{self.year}.csv")

            except FileNotFoundError as f:
                if (month == 'December') and ('January' in months):
                    new_df.to_csv(
                        f"Data/Insider_trans_{month}_{self.year - 1}.csv")

                else:
                    new_df.to_csv(
                        f"Data/Insider_trans_{month}_{self.year}.csv")

    def __fix_dates(self):
        ind = self.df.loc[:, 'Date']
        ind = pd.to_datetime(ind, format='%b %d')
        dates = []
        for _, date in enumerate(ind):
            if (date.month == 12) and (datetime.now().month == 1):
                date = date.replace(year=datetime.now().year - 1)
            else:
                date = date.replace(year=datetime.now().year)
            dates.append(date)
        self.df['Date'] = dates

    def run(self):
        print("Beginning to scrape Insider Trading data from Finviz.com")
        print("================================================================")
        self.get_df()
        print("Data gathered from Finviz")
        print("================================================================")
        print('Cleaning data...')
        print("================================================================")
        self.clean_df()
        print('Saving data in Data/ directory')
        print("================================================================")
        self.split_by_month()
        print('Done!')


if __name__ == '__main__':
    Scraper().run()
