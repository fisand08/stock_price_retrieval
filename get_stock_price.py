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



def get_historic_data(stock_id, verbose):
    
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
            print(f'ERROR: ({e})')
        
    # Add all time
    #driver.find_element(By.XPATH, '//div[@data-test="dropdown"]').click()
    #driver.find_element(By.XPATH, '//div[@class_="Pos(r) D(ib) Va(m) Mstart(8px)"').click()
    #driver.find_element(By.XPATH,'//div[@role="button"]').click()
    #driver.find_element(By.XPATH,'//svg[@data-icon="CoreArrowDown"]').click()
    dropdown_block = driver.find_element(By.XPATH,'//div[@class="M(0) O(n):f D(ib) Bd(0) dateRangeBtn O(n):f Pos(r)"]')
    print(f'dropdown text:{dropdown_block.text}')
    divs = dropdown_block.find_elements(By.TAG_NAME,'div')
    for d in divs:
        d.click()

    WebDriverWait(driver,10).until(EC.element_to_be_clickable((By.XPATH, '//button[@data-value="MAX"]'))).click()
    time.sleep(0.5)
    hyperlinks = driver.find_element(By.TAG_NAME, 'a')
    for l in hyperlinks:
        l_text = l.get_attribute('href')
        if 'download' in l_text and 'history' in l_text and 'query' in l_text:
            data_url = l_text
            print(f'URL for dataframe: {data_url}')
    
def get_current_data(stock_id,verbose):
    """
    uses requests and bs4 to get from yahoo finance summary:
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
        print(f'*** retrieving stock data from {url}')
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


def getCurrentDataPerStock_sel(stock_id,verbose):
    
    price = np.nan
    volume = np.nan
    url = 'https://www.finance.yahoo.com/quote/' + stock_id + '?p=' + stock_id
    if verbose:
        print('Fetching data from ' + url)
    s=Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=s)
    driver.implicitly_wait(5)
    driver.get(url)
    
    # Press button
    button = True
    if button:
        time.sleep(1)
        buttons = driver.find_elements(By.TAG_NAME,'button')[-1].click()
        
    # get current price
    price_labels = driver.find_elements(By.XPATH,'//fin-streamer[@data-field="regularMarketPrice"]')
    price_label = price_labels[6]
    print('Price=' + price_label.text)
    current_price = float((price_label.text).replace(',',''))

    # get volume
    tables = driver.find_elements(By.TAG_NAME,'td')
    table = tables[13]
    volume = float( (table.text).replace(',','') )
    
    return current_price, volume


def getCurrentDataPerStock(stock_id,verbose):
    #stock_id = stock_id.lower()
    stock_id = stock_id.replace('.','.')
    current_price = np.nan
    volume = np.nan
    url = 'https://finance.yahoo.com/quote/' + stock_id + '?p=' + stock_id + '&.tsrc=fin-srch'
    url = 'https://www.finance.yahoo.com/quote/' + stock_id + '?p=' + stock_id
    #url = url + '/'
    #https://finance.yahoo.com/quote/ROG.SW?p=ROG.SW&.tsrc=fin-srch
    #url = 'https://finance.yahoo.com/quote/' + stock_id
    
    
    url = url.strip()
    if verbose:
        print(5*'\n' + 'Fetching data from ' + url)
    r = requests.get(url)
    status = r.status_code
    if str(status) == '200':
            
        if verbose:
            print('Visit successful w requests')
        
        soup = BeautifulSoup(r.content, 'html.parser')
        
        # get stock price
        s = soup.find('div', class_='D(ib) Mend(20px)')
        content = s.find_all('fin-streamer')
        current_price = float((content[0].text).replace(',',''))
        
        # get additional data
        s = soup.find('table', class_="W(100%)")
        content = s.find_all('tr')
        for en, field in enumerate(content):
            ftext = field.text
            # Volume
            if 'Volume' in ftext and not 'Avg' in ftext:
                volume = float((ftext).replace('Volume','').replace(',',''))
    
    else:
        if verbose:
            print('Using Selenium instead')
        current_price,volume = getCurrentDataPerStock_sel(stock_id,verbose)
    
    
    return current_price, volume
    

def getHistoricDataFile(stock_id, verbose):
    url = 'https://finance.yahoo.com/quote/' + stock_id + '/history?p=' + stock_id

    if verbose:
        print('Fetching historic data from ' + url)

    s=Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=s)
    driver.implicitly_wait(5)
    driver.get(url)
    
    # Press button
    button = True
    if button:
        time.sleep(1)
        buttons = driver.find_elements(By.TAG_NAME,'button')[-1].click()
        
        
    # get all hyperlinks
    hlinks = driver.find_elements(By.TAG_NAME,'a')
    for hl in hlinks:
        href = hl.get_attribute('href')
        if 'query1' in href and 'download' in href and 'period' in href:
            print(href)
            df_hist = pd.read_csv(href)
    return df_hist
    
def preprocessHistoricData(df_historic):
    return df_historic

def updateHistoricData(stock_id, df_history):
    pass

def readMonitor(xlsx_file):
    df_stock = pd.read_excel('Stocks_Monitored2.xlsx')
    #print(df_stock.head())
    return df_stock
    
def monitorStocks(df_stock, verbose):
    for j in list(df_stock.index):
        stock_id = df_stock.loc[j,'STOCK_NAME']
        
        current_price, volume = getCurrentDataPerStock(stock_id,verbose)
        df_stock.loc[j,'PRICE'] = current_price
        df_stock.loc[j,'VOLUME'] = volume        
    return df_stock
    
def getHistoricData(df_stock,verbose):
    try:
        os.mkdir('Historic_Data')
    except:
        print('Directory "Historic_Data" already exists')
        
    for j in list(df_stock.index):
        stock_id = df_stock.loc[j,'STOCK_NAME']
        df_historic = getHistoricDataFile(stock_id, verbose)
        df_historic = preprocessHistoricData(df_historic)
        df_historic.to_csv(stock_id + '_history.csv',index=False)
        p1 = os.path.join( os.getcwd(), stock_id + '_history.csv')
        p2 = os.path.join( os.getcwd(), 'Historic_Data', stock_id + '_history.csv')
        shutil.move( p1,p2)
        
    return True