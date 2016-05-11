#!/usr/bin/env python

import urllib2
import pytz
import pandas_datareader as pdr
import csv
import os

from bs4 import BeautifulSoup
from datetime import datetime
from pandas_datareader import data as web


SITE = "http://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
START = datetime(2015,5,11)
END = datetime.today().utcnow()

#to scrape list of tickers from Wikipedia(SITE)
def scrape_list(site):
    hdr = {'User-Agent': 'Mozilla/5.0'}
    req = urllib2.Request(site, headers=hdr)
    page = urllib2.urlopen(req)
    soup = BeautifulSoup(page, "html.parser")

    table = soup.find('table', {'class': 'wikitable sortable'})
    sector_tickers = dict()
    for row in table.findAll('tr'):
        col = row.findAll('td')
        if len(col) > 0:
            sector = str(col[3].string.strip()).lower().replace(' ', '_')
            ticker = str(col[0].string.strip())
            if sector not in sector_tickers:
                sector_tickers[sector] = list()
            sector_tickers[sector].append(ticker)
    return sector_tickers

#downloader
def download(sector_tickers, start, end):
    csv_path = "CSV/"
    perc_change_list = []
    for sector, tickers in sector_tickers.iteritems():
        print 'Downloading data from Yahoo for %s sector' % sector
        
        if not os.path.isdir(csv_path+sector):
    		os.makedirs(csv_path+sector)

    	#to generate all data from yahoo finance for all tickers in S&P500
    	data1 = web.DataReader(tickers, 'yahoo', start, end)
    	data1['perc_change']= 100* ((data1['Close']-data1['Open'])/data1['Open']);

    	#finding correlation between stocks in a sector (for ex: technology) based on perc_change in price. 
    	stockCorr = correlation(data1['perc_change'])
    	
    	#converting this matrix into a csv file
    	stockCorr.to_csv(csv_path+"correlation_"+sector+".csv", header=True, index=True, na_rep=" ")
        
       	#to generate csv files for each stock
        for ticker in tickers: 
        		data = web.DataReader(ticker, 'yahoo', start, end)
        		data['perc_change']= 100* ((data['Close']-data['Open'])/data['Open']);
        		data.to_csv(csv_path+sector+'/'+ticker+".csv") 
    
def correlation(df):
	#define returns
	rets = df.pct_change()
	
	#calculate correlation
	corr = rets.corr()
	return corr

def get_snp500():
    sector_tickers = scrape_list(SITE)
    download(sector_tickers, START, END)
    

if __name__ == '__main__':
    get_snp500()
