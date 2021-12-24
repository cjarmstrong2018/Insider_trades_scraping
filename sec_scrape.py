import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
import re


class Scraper(object):
    def __init__(self):
        self.sec = "https://www.sec.gov/"
        self.form_4_tbl = "https://www.sec.gov/cgi-bin/browse-edgar?company=&CIK=&type=4&owner=only&count=100&action=getcurrent"
        self.header = {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.75 Safari/537.36",
            "X-Requested-With": "XMLHttpRequest"
        }
        self.transactions = []

    def crawl_tables(self):
        '''
        Collect all atags to be processed by clicking 'Next 100' until there are no
        more tables to parse
        Inputs: None
        Returns: List of atags to be processed
        '''
        url = self.form_4_tbl
        req = requests.get(url, headers=self.header)
        soup = BeautifulSoup(req.content, 'html.parser')
        num_tbls = soup.find_all('h1')
        tbl_valid = 1 if len(num_tbls) == 1 else 0
        atags = []
        while tbl_valid:
            tbl = soup.find_all('table')[-2]
            tags = tbl.find_all('a', string="[html]")
            atags.extend(tags)
            next_url_ext = soup.find(
                "input", attrs={'type': "button", "value": "Next 100"})['onclick']
            next_url_ext = next_url_ext[18:-1]
            next_url = self.sec + next_url_ext
            next_req = requests.get(next_url, headers=self.header)
            soup = BeautifulSoup(next_req.content, 'html.parser')
            num_tbls = soup.find_all('h1')
            tbl_valid = 1 if len(num_tbls) == 1 else 0
        return atags

    def parse_atags(self, atags):
        '''
        Iterate through all atags, navigating to the Form 4 filing
        '''
        i = 0
        for tag in atags:
            form_soup = self.navigate_to_form4(tag)
            try:
                self.parse_form4(form_soup)
            except Exception as e:
                print('Error in :', tag['href'])
                continue
            i += 1
            print(i)

    def navigate_to_form4(self, atag):
        '''
        Given an atag, navigate to the form 4 filing
        Inputs: 
            atag - (tag) atag with href to each filing
        Returns: 
            form_req (request) - request object of the Form 4 filing
        '''
        next_url = self.sec + atag['href']
        next_req = requests.get(next_url, headers=self.header)
        next_soup = BeautifulSoup(next_req.content, 'html.parser')
        ext = None
        tds = next_soup.find_all('td', scope='row')
        for td in tds:
            if td.find('a'):
                ext = td.find('a')['href']
                break
        # final form
        form_url = self.sec + ext
        form_req = requests.get(form_url, headers=self.header)
        return form_req

    def parse_form4(self, form_req):
        '''
        Parse Form 4 filing to extract insider trade information
        Inputs: 
            form_soup - (soup object) soup object of the form 4 filing
        Returns: 
            Nothing - appends tuple to self.transactions for creation of df
            form is (date, ticker, # shares, price, total value) 
        '''
        form_soup = BeautifulSoup(form_req.content, 'html.parser')
        # Find Ticker
        ticker_loc = form_soup.find(
            lambda tag: tag.name == "span" and "Ticker" in tag.text)
        ticker_loc = ticker_loc.findNext('span', attrs={'class': 'FormData'})
        ticker = ticker_loc.text
        # Table
        df = pd.read_html(form_req.content, match=re.compile(
            r'Table I - Non-Derivative Securities Acquired, Disposed of, or Beneficially Owned'))[0]
        if df.empty:
            return
        df = df.iloc[:, [1, 5, 6, 7]]
        df.columns = ['Date', 'Amount', 'Type', 'Price']
        # check if option excercise... skip if is
        # derivs = pd.read_html(form_req.content, match = re.compile(r'Table II - Derivative Securities Acquired, Disposed of, or Beneficially Owned'), parse_dates = True)[0]
        # if not derivs.empty:
        #     continue
        # grab date
        date = df.loc[0, 'Date']
        df['Amount'] = df['Amount'].astype(str)
        df['Type'] = df['Type'].astype(str)
        df['Price'] = df['Price'].str.extract(r'(\d+\.?\d+)')  # FIX FIX FIX
        df['Price'] = pd.to_numeric(df['Price'])
        df['Price'] = df['Price'].fillna(0)

        df['Amount'] = df['Amount'].str.extract(
            r'(^\d+\,?\d+)').replace(r'[^\d\.\-]g', "")
        df['Amount'] = df['Amount'].fillna(0)
        df['Amount'] = pd.to_numeric(df['Amount'], errors='coerce')

        df['Amount'] = np.where(df['Type'] == 'D', (-1)
                                * df['Amount'], df['Amount'])
        df['Value'] = df['Amount'] * df['Price']

        form = (date, ticker, df['Amount'].sum(),
                df['Price'].mean(), df['Value'].sum())
        self.transactions.append(form)

    def create_df(self):
        atags = self.crawl_tables()
        self.parse_atags(atags)
        df = pd.DataFrame(self.transactions, columns=[
                          'Date', 'Ticker', '# Shares', 'Price', 'Value'])
        df = df.drop_duplicates()
        print(df)


if __name__ == '__main__':
    scrape = Scraper()
    scrape.create_df()
