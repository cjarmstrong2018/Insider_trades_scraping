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
            print(next_url)
            next_req = requests.get(next_url, headers=self.header)
            soup = BeautifulSoup(next_req.content, 'html.parser')
            num_tbls = soup.find_all('h1')
            tbl_valid = 1 if len(num_tbls) == 1 else 0
        return atags

    def parse_atags(self, atags):
        '''
        Iterate through all atags, navigating to the Form 4 filing
        '''
        pass

    def navigate_to_form4(self, atag):
        '''
        Given an atag, navigate to the form 4 filing
        Inputs: (tag) atag with href to each filing
        Returns: (soup) - soup object of the Form 4 filing
        '''
        pass

    def parse_form4(self, form_soup):
        '''
        Parse Form 4 filing to extract insider trade information
        Inputs: 
            form_soup - (soup object) soup object of the form 4 filing
        Returns: 
            Nothing - appends tuple to self.transactions for creation of df
            form is (date, ticker, # shares, price, total value) 
        '''
        pass


if __name__ == '__main__':
    scrape = Scraper()
    print(scrape.crawl_tables())
