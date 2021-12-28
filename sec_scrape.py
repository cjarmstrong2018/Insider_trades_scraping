from genericpath import exists
import pandas as pd
import numpy as np
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import re
from os import path, mkdir
from datetime import datetime
import calendar


class Scraper(object):
    def __init__(self):
        self.sec = "https://www.sec.gov/"
        self.form_4_tbl = "https://www.sec.gov/cgi-bin/browse-edgar?company=&CIK=&type=4&owner=only&count=100&action=getcurrent"
        self.header = {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.75 Safari/537.36",
            "X-Requested-With": "XMLHttpRequest"
        }
        self.r = self._requests_retry_session()
        self.most_recent_date = self.get_last_parsed_filing()
        self.new_recent_filing = None
        self.transactions = []
        self.df = None
        if not path.isdir("Data"):
            mkdir("Data")

    def run(self):
        print("Beginning to scrape filings")
        print("================================================================")
        self.create_df()
        print("Data gathered...")
        print("================================================================")
        print("Cleaning data...")
        self.clean_df()
        print("Saving Data")
        self.split_and_save()
        self.save_most_recent_filing_time()
        print("Done!")

    def crawl_tables(self):
        '''
        Collect all atags to be processed by clicking 'Next 100' until there are no
        more tables to parse
        Inputs: None
        Returns: List of atags to be processed
        '''
        url = self.form_4_tbl
        req = self.r.get(url, headers=self.header)
        soup = BeautifulSoup(req.content, 'html.parser')
        num_tbls = soup.find_all('h1')
        tbl_valid = 1 if len(num_tbls) == 1 else 0
        atags = []
        while tbl_valid:
            tbl = soup.find_all('table')[-2]
            tags = tbl.find_all('a', string="[html]")
            # Remove all form 4A entries, could change later
            valid_tags = self.not_form4A(tags)
            atags.extend(valid_tags)
            next_url_ext = soup.find(
                "input", attrs={'type': "button", "value": "Next 100"})['onclick']
            next_url_ext = next_url_ext[18:-1]
            next_url = self.sec + next_url_ext
            next_req = self.r.get(next_url, headers=self.header)
            soup = BeautifulSoup(next_req.content, 'html.parser')
            num_tbls = soup.find_all('h1')
            tbl_valid = 1 if len(num_tbls) == 1 else 0
        return atags

    def not_form4A(self, atags):
        '''
        Returns True if link is to a form 4 and False if it is to a form 4A
        '''
        valid = []
        for tag in atags:
            p = tag.parent.parent
            t = p.find('td').text
            if t == '4':
                valid.append(tag)
        return valid

    def parse_atags(self, atags):
        '''
        Iterate through all atags, navigating to the Form 4 filing
        '''
        i = 0
        for tag in atags:
            time = self.get_filing_time(tag)
            # saves most recent filing time
            if i == 0:
                self.new_recent_filing = time
            # prevents double counting filings
            if time <= self.most_recent_date:
                print("Reached most recent filing, Done!")
                break
            try:
                form_soup = self.navigate_to_form4(tag)
                self.parse_form4(form_soup, time)
            except ConnectionError:
                print('Connection Error...\n Retry')
            except Exception as e:
                print('Error in :', tag['href'])
                continue
            i += 1
            if i % 100 == 0:
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
        next_req = self.r.get(next_url, headers=self.header)
        next_soup = BeautifulSoup(next_req.content, 'html.parser')
        ext = None
        tds = next_soup.find_all('td', scope='row')
        for td in tds:
            if td.find('a'):
                ext = td.find('a')['href']
                break
        # final form
        form_url = self.sec + ext
        form_req = self.r.get(form_url, headers=self.header)
        return form_req

    def parse_form4(self, form_req, datetime):
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
        date = datetime.date()
        df['Amount'] = df['Amount'].astype(str)
        df['Type'] = df['Type'].astype(str)
        df['Price'] = df['Price'].str.extract(r'(\d+\.?\d+)')
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

    def find_date(self, form_soup, df):
        '''
        Searches soup for the filing date, if none provided, use earliest transaction date
        Inputs:
            form_soup - (Soup) soup object from form 4
            df - (DataFrame) Table 1 of the
        Returns: datetime object
        '''
        form_data = form_soup.find_all('span', attrs={'class': 'FormData'})
        try:
            date = form_data[-1].text
            date = re.match(r'(\d{1,2})/(\d{1,2})/(\d{1,4})', date)
            date = datetime.strptime(date, '%b/%d/%Y')
        except Exception:
            date = df.loc[0, 'Date']
        return date

    def create_df(self):
        atags = self.crawl_tables()
        print("Filing paths gathered...parsing form 4s now")
        print("================================================================")
        self.parse_atags(atags)
        self.df = pd.DataFrame(self.transactions, columns=[
            'Date', 'Ticker', '# Shares', 'Price', 'Value'])

    def clean_df(self):
        '''
        Cleans the df that was just parsed before saving
        '''
        self.df = self.df.drop_duplicates()
        self.df['Date'] = pd.to_datetime(self.df['Date'])
        # self.df['Date'] = pd.to_datetime(
        #     self.df['Date'], infer_datetime_format=True)
        # self.df = self.df.loc[:, ['# Shares', 'Value ($)']]
        self.df = self.df.set_index(['Date', 'Ticker'])

        price = self.df.loc[:, "Price"]
        to_sum = self.df.loc[:, ["# Shares", "Value"]]
        price = price.groupby(['Date', 'Ticker']).mean()
        to_sum = to_sum.groupby(['Date', 'Ticker']).sum()
        self.df = pd.concat([to_sum, price], axis=1)
        self.df = self.df.sort_index()

    def get_last_parsed_filing(self):
        '''
        Reads date.txt file that holds the filing time of the
        most recently parsed entry
        Returns: datetime object
        '''
        if path.exists("date.txt"):
            with open('date.txt', 'r') as f:
                date = f.readline()
                date = datetime.strptime(date, '%Y-%m-%d %H:%M:%S')
        else:
            date = datetime.fromtimestamp(0)
        return date

    def _requests_retry_session(self,
                                retries=3,
                                backoff_factor=0.3,
                                status_forcelist=(500, 502, 504),
                                session=None,
                                ):
        session = session or requests.Session()
        retry = Retry(
            total=retries,
            read=retries,
            connect=retries,
            backoff_factor=backoff_factor,
            status_forcelist=status_forcelist,
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        return session

        # def split_by_month(self):
        #     # get months
        #     months = self.df.index.month_name().unique()
        #     for month in months:
        #         month_code = MONTHS[month]
        #         new_df = self.df[self.df.index.month == month_code]
        #         new_df = new_df.reset_index()
        #         new_df = new_df.set_index(['Date', 'Ticker'])
        #         year = self.year
        #         try:
        #             if (month == 'December') and 'January' in months:
        #                 year -= 1
        #                 old_df = pd.read_csv(
        #                     f"Data/{(year)}/{month}_{year}.csv", index_col=['Date', 'Ticker'])
        #             else:
        #                 old_df = pd.read_csv(
        #                     f"Data/{year}/{month}_{year}.csv", index_col=['Date', 'Ticker'])
        #             df_combined = old_df.append(new_df)
        #             df_combined = df_combined.drop_duplicates(keep='last')
        #             df_combined.sort_index()
        #             if (month == 'December') and ('January' in months):
        #                 df_combined.to_csv(
        #                     f"Data/{year}/{month}_{year}.csv")
        #             else:  # Handle decembers
        #                 df_combined.to_csv(
        #                     f"Data/{year}/{month}_{year}.csv")

        #         except FileNotFoundError as f:
        #             dir = f"Data/{year}"
        #             if not path.isdir(dir):
        #                 mkdir(dir)

        #             if (month == 'December') and ('January' in months):
        #                 new_df.to_csv(
        #                     f"Data/{year}/{month}_{year}.csv")

        #             else:
        #                 new_df.to_csv(
        #                     f"Data/{year}/{month}_{year}.csv")
    def get_filing_time(self, atag):
        '''
        Navigates from <atag> to filing timestamp
        Input:
            atag - a tag link to form4
        Returns - DateTime object
        '''
        parent = atag.parent
        # Navigate to correct tag and select text
        accepted = parent.next_sibling.next_sibling
        accepted = accepted.next_sibling.next_sibling
        accepted = accepted.get_text(separator=" ")
        # parse date
        accepted_date = datetime.strptime(accepted, '%Y-%m-%d %H:%M:%S')
        return accepted_date

    def save_most_recent_filing_time(self):
        '''
        Saves the most recent filing time in date.txt
        Creates a new file if it does not exist
        Returns: None
        '''
        to_write = self.new_recent_filing.strftime("%Y-%m-%d %H:%M:%S")
        print(f"Saving {to_write} as most recent")
        with open('date.txt', 'w+') as f:
            f.write(to_write)

    def split_and_save(self):
        self.df = self.df.reset_index(level=1, drop=False)
        g = self.df.groupby(pd.Grouper(freq='M'))
        dfs = [group for _, group in g]
        self.save_dfs(dfs)

    def save_dfs(self, dfs):
        '''
        Saves each df in the proper location
        '''
        for df in dfs:
            date = df.index[0]
            month = date.month
            month = calendar.month_name[month]
            year = date.year
            dir = f"Data/{year}"
            if not path.isdir(dir):
                mkdir(dir)
            csv_path = f"Data/{year}/{month}_{year}.csv"
            if path.exists(csv_path):
                old_df = pd.read_csv(csv_path, index_col="Date")
                combined = pd.concat([df, old_df])
                combined = combined.reset_index(drop=False)
                price = combined.loc[:, ['Date', 'Ticker', 'Price']]
                to_sum = combined.loc[:, [
                    'Date', 'Ticker', "# Shares", "Value"]]
                price = price.groupby(['Date', 'Ticker']).mean()
                to_sum = to_sum.groupby(['Date', 'Ticker']).sum()
                df = pd.concat([to_sum, price], axis=1)
                df = df.reset_index(level=1, drop=False)
            df.to_csv(csv_path)


if __name__ == '__main__':
    scrape = Scraper().run()
