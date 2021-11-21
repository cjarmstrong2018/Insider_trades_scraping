from datetime import date
from datetime import datetime
import pandas as pd
import numpy as np
import requests


class Scraper(object):
    def __init__(self):
        print("Starting Scraper")
        self.url = 'https://finviz.com/insidertrading.ashx'
        self.header = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.75 Safari/537.36",
                       "X-Requested-With": "XMLHttpRequest"
                       }
        self.year = datetime.now().year
        self.df = None

    def get_df(self):

        page = requests.get(self.url, headers=self.header)
        print(page.status_code)
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
        self.df = self.df.set_index('Date')

    def split_by_month(self):
        # get months
        months = []
        for _, date in enumerate(self.df.index):
            # date = ind[0]
            m, d = date.split()
            if m not in months:
                months.append(m)
        for month in months:
            new_df = self.df[self.df.index.str.startswith(month)]
            new_df = new_df.reset_index()
            new_df = new_df.set_index(['Date', 'Ticker'])
            try:
                if month != 'Dec':
                    old_df = pd.read_csv(
                        f"Insider_trans_{month}_{self.year}.csv", index_col=['Date', 'Ticker'])
                else:
                    old_df = pd.read_csv(
                        f"Insider_trans_{month}_{self.year - 1}.csv", index_col=['Date', 'Ticker'])
                print('File Found')

                df_combined = old_df.combine(
                    new_df, lambda a, b: a if np.abs(a) < np.abs(b) else b)
                if month != 'Dec':
                    df_combined.to_csv(
                        f"Insider_trans_{month}_{self.year}.csv")
                else:
                    df_combined.to_csv(
                        f"Insider_trans_{month}_{self.year - 1}.csv")

            except FileNotFoundError:
                print('No file found')
                if month != 'Dec':
                    new_df.to_csv(
                        f"Data/Insider_trans_{month}_{self.year}.csv")
                else:
                    new_df.to_csv(
                        f"Data/Insider_trans_{month}_{self.year - 1}.csv")

        print(self.df[self.df.index.str.startswith(months[0])])
        print(months)


if __name__ == '__main__':
    s = Scraper()
    s.get_df()
    s.clean_df()
    s.split_by_month()
