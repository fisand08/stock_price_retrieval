import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
import time
import shutil
import os
from lxml import etree
from general_helpers import is_number, convert_cap_value
import datetime

"""
- handler function - referesh daily stock data every 24h at end of market
- store history upon first starting progam - make it somehow to run it perpetually
- keep inout flexible
"""


def handler(verbose):
    """
    description:
        - function that is "alive" - schedules everything
    
    dev:
        - needs to be alive and read stock list (for the case new stocks are added to be observed - listener)
    """
    
    # read inputs
    stock_list =  open(stock_input_file,'r').read().split('\n')

    # read key information for all stocks that were requested, store history
    df_stocks = pd.DataFrame()
    for stock_id in stock_list:
        full_name, market, currency, current_price, prev_close, volume, market_cap, dividend_value, dividend_percent = get_current_data(stock_id, verbose)
        new_line = pd.DataFrame({
            "STOCK_ID": [full_name],
            "MARKET": [market],
            "CURRENCY": [currency]
        })
        df_stocks = pd.concat([df_stocks,new_line], ignore_index=True)
        df_history = get_historic_data(stock_id, verbose)
        df_history.to_csv(  os.path.join(os.getcwd(),'stock_data',stock_id + '_history.csv'), index=False )

    # after historic data is stored, keep account of time and update historic data with current market data
    while True:
        for stock_id in stock_list:
            # get market and closing time
            market = df_stocks[df_stocks['STOCK_ID'] == stock_id].iloc[0]['MARKET']


    

def get_closing_time():
    pass

def get_historic_data(stock_id, verbose):
    """
    Description:
        - gets current and historic data for a stock
    Inputs:
        - stock id: str; identifier of stock at yahoo finance
        - verbose: bool; flag if additional output is printed
    """
    url = 'https://finance.yahoo.com/quote/' + stock_id + '/history?p=' + stock_id

    if verbose:
        print(f'*** Fetching historic data from {url}')

    s=Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=s)
    driver.implicitly_wait(5)
    driver.get(url)
    
    # Press button
    try:
        time.sleep(1)
        buttons = driver.find_elements(By.TAG_NAME,'button')[0].click()
    except Exception as e:
        if verbose:
            if not 'element not interactable' in str(e): # only print if not typical error
                print(f'ERROR: ({e})')
        
    # Add all time
    dropdown_block = driver.find_element(By.XPATH,'//div[@class="M(0) O(n):f D(ib) Bd(0) dateRangeBtn O(n):f Pos(r)"]')
    divs = dropdown_block.find_elements(By.TAG_NAME,'div')
    for d in divs:
        d.click()

    WebDriverWait(driver,10).until(EC.element_to_be_clickable((By.XPATH, '//button[@data-value="MAX"]'))).click()
    time.sleep(0.5)
    hyperlinks = driver.find_elements(By.TAG_NAME, 'a')
    for l in hyperlinks:
        l_text = l.get_attribute('href')
        if 'download' in l_text and 'history' in l_text and 'query' in l_text:
            data_url = str(l_text)
            #print(f' dataframe: {data_url}')
    if data_url:
        #print(f'Reading data url: {data_url}')
        df = pd.read_csv(data_url)
        #print(df.head())
    return df

def get_current_data(stock_id,verbose):
    """
    description:
        - uses requests and bs4 to get from yahoo finance summary:
            -up-to-date current stock information
            -general informaton on stock
    
    inputs: 
        -stock_id: string; ID of the stock, e.g. NVS for novartis
        -verbose: bool; if additional information is printed

    outputs:
        -currency: string; currency in which the stock is traded in
        -full_name: string; full name of the stock
        -current_price: float; current price of the stock
        -volume: float; current volume of the stock
        -market_cap: float; current market cap of the stock
    """
    url = f'https://finance.yahoo.com/quote/{stock_id}/?p={stock_id}'
    if verbose:
        print(f'*** retrieving current stock data from {url}')
    full_name, currency, current_price, volume, market_cap, prev_close, market, dividend_value, dividend_percent = np.nan, np.nan, np.nan, np.nan, np.nan,np.nan,np.nan,np.nan,np.nan
    r = requests.get(url)
    if verbose:
        print(f'Status:{r.status_code}')
    if r.status_code == 200:
        soup = BeautifulSoup(r.text,"html.parser")
        
        # read current price
        price_field = soup.find('fin-streamer', class_='Fw(b) Fz(36px) Mb(-4px) D(ib)')# attrs={'data-field':'regularMarketPrice'})
        if price_field:
            if is_number(price_field.text.strip()):
                current_price = float(price_field.text)
        
        # read current date
        close_field = soup.find('div', id='quote-market-notice', class_="C($tertiaryColor) D(b) Fz(12px) Fw(n) Mstart(0)--mobpsm Mt(6px)--mobpsm Whs(n)")
        close = close_field.find('span').text
    
        # read name
        name_field = soup.find('div',class_='Mt(15px) D(f) Pos(r)')
        full_name = name_field.find('h1').text

        # read market and curreny
        market_field = soup.find('div',class_='C($tertiaryColor) Fz(12px)')
        market_currency = market_field.find('span').text
        if 'Currency' in market_currency:
            market = market_currency.split('-')[0]
            currency = market_currency.split()[-1]

        # read tabular data
        table_block = soup.find('div',id='quote-summary')
        table = table_block.find('table')
        # previous close
        prev_close_field = table.find('td',attrs = {"data-test":'PREV_CLOSE-value'})
        if prev_close_field:
            if is_number(prev_close_field.text):
                prev_close = float(prev_close_field.text)
        # volume
        volume_field = table.find('fin-streamer',attrs={"data-field":'regularMarketVolume'})
        if volume_field:
            if is_number(volume_field.text.replace(',','')):
                volume = float(volume_field.text.replace(',',''))
        
        # read table (2nd table)
        table_block_2 = soup.find('div',attrs={'data-test':'right-summary-table'})
        table_2 = table_block_2.find('table')#,class_='W(100%) M(0) Bdcl(c)')
        # market cap
        cap_field = table_2.find('td', attrs={'data-test':'MARKET_CAP-value'})
        if cap_field:
            market_cap = convert_cap_value(cap_field.text)
        # dividend
        dividend_field = table_2.find('td',attrs={'data-test':'DIVIDEND_AND_YIELD-value'})
        if dividend_field:
            if len(dividend_field.text.split()) > 1:
                d1 = dividend_field.text.split()[0]
                d2 = dividend_field.text.split()[1].replace('(','').replace(')','')
                if is_number(d1):
                    dividend_value = float(d1)
                if is_number(d2[:-1]) and '%' in d2:
                    dividend_percent = float(d2[:-1])

    return full_name, market, currency, current_price, prev_close, volume, market_cap, dividend_value, dividend_percent