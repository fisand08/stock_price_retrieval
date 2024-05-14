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
import os
from lxml import etree
from general_helpers import is_number, convert_cap_value
from datetime import datetime, timedelta

"""
- add some commenting
"""


def handler(verbose, stock_input_file):
    """
    description:
        - function that is "alive" - schedules everything
    """
    iteration = 0
    collected_stocks = []
    df_stocks = pd.DataFrame(columns=['STOCK_ID','STOCK_NAME','MARKET','CURRENCY'])

    while iteration < 3: # iteration loop here because stock input might change over time - new stock of interest added
        # read inputs
        stock_list_file =  open(stock_input_file,'r')
        stock_list = [x.strip() for x in stock_list_file.read().split('\n')]
        # read key information for all stocks that were requested, store history
        for stock_id in stock_list:
            if not stock_id in collected_stocks:
                # general data
                #full_name, market, currency, current_price, prev_close, volume, market_cap, dividend_value, dividend_percent = get_current_data(stock_id, verbose)
                full_name, market, currency, current_price, prev_close, volume, market_cap, dividend_value, dividend_percent = get_current_new(stock_id, verbose)

                
                new_line = pd.DataFrame({
                    "STOCK_ID":[stock_id.strip()],
                    "STOCK_NAME": [full_name.strip()],
                    "MARKET": [market.strip()],
                    "CURRENCY": [currency.strip()]
                })
                df_stocks = pd.concat([df_stocks,new_line], ignore_index=True)
                # historic data
                df_history = get_historic_data(stock_id, verbose)
                df_history = add_weekend_rows(df_history)
                df_history.to_csv(  os.path.join(os.getcwd(),'stock_data',stock_id + '_history.csv'), index=False )
                collected_stocks.append(stock_id)
        # daily stock update - new loop to not fetch historic data daily (error prone selenium routine)
        current_time = datetime.now()  # get current swiss time

        print(f'current time b: {current_time}')
        print(df_stocks)
        current_day = current_time.day
        if not current_day in [5,6]:
            for stock_id in stock_list:
                # get market and closing time
                print(f'columns: {df_stocks.columns}')
                print(f'Checking "{stock_id}"')
                market = df_stocks[df_stocks['STOCK_ID'] == stock_id.strip()].iloc[0]['MARKET']
                closing_times = read_stock_closing_time(os.path.join(os.getcwd(),'inputs','stock_markets_closing.csv'), current_time)
                closing_status = get_stock_exchange_closing_status(current_time,market,closing_times)
                if closing_status:
                    if verbose:
                        print(f'Retrieving data for stock_id {stock_id} because market closed')
                    full_name, market, currency, current_price, prev_close, volume, market_cap, dividend_value, dividend_percent = get_current_data(stock_id, verbose) # get stock data
                    history_path = os.path.join(os.getcwd(),'stock_data',stock_id + '_history.csv')
                    df_history = pd.read_csv(  history_path ) # open historic dataframe
                    date = current_time.strftime("%Y-%m-%d") # format date from current time
                    if not df_history.iloc[-1]['Date'] == date:
                        print(f'Current date {date} not yet found in history, so update it...')
                        df_history = merge_current_data(df_history, prev_close, volume, current_price, date)
                        df_history.to_csv(history_path, index=False)
                    else:
                        print(f'Current date {date} already found in data, ignoring...')

        df_stocks.to_csv('stocks_db.csv',index=False)
        iteration=iteration + 1
        time.sleep(1) #time.sleep(60)
        stock_list_file.close()


def merge_current_data(df, prev_close, volume, current_price, date):
    """
    description:
        - updates historic dataframe with new data
    inputs:
        - df_history: pd.DataFrame; historic data
        - prev_close: float; previous closing of market
        - volume: float; market volume
        - current_prie: float; price (after closing because called after market close)
        - date: ?; current date
    """
    new_idx = df.index[-1] + 1
    df.loc[new_idx, 'Date'] = date
    df.loc[new_idx, 'Open'] = prev_close
    df.loc[new_idx, 'Close'] = current_price
    df.loc[new_idx, 'Volume'] = volume
    return df

def read_stock_closing_time(stock_close_csv_path, current_time):
    """
    description:
        - reads csv file with stock closing times in reference to zurich time
    inputs:
        - path to csv file
    returns:
        - dictionary with stock exchange closing times
    """
    df_c = pd.read_csv(stock_close_csv_path)
    closing_times = {}
    for j in df_c.index:
        h, m, s, d = df_c.loc[j,'h'], df_c.loc[j,'m'], df_c.loc[j,'s'], df_c.loc[j,'d'] 
        if d == 0:
            closing_times.update( { df_c.loc[j,'MARKET_ID'] : current_time.replace(hour=h, minute=m, second=s)  } )        
        else:
            closing_times.update( { df_c.loc[j,'MARKET_ID'] : current_time.replace(hour=h, minute=m, second=s) + timedelta(days=float(d))  } )   
    
    return closing_times

def get_stock_exchange_closing_status(zurich_time, market, closing_times):
    """
    description:
        - function checks if stock market is closed
    inputs:
        - zurich_time: datetime obj.; current time in zurich
        - market: str; id for the market
    outputs:
        - closing_status: bool; True if closed
    dev:
        - add shortcuts like NYSE, compatible with yahoo
    """
    if market in closing_times:
        closing_time = closing_times[market]
        return zurich_time >= closing_time
    else:
        print(f"{market} is not found in the list.")
        return None


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
    print('*** Clicking away annoying pop-ups ***')
    try:
        buttons = driver.find_elements(By.TAG_NAME,'button')[:3]
        for en, button in enumerate(buttons):
            print(f'button {en}')
            #print(button.text)
            button.click()
            time.sleep(0.5)
    except Exception as e:
        if verbose:
            print('button failed')
            if not 'element not interactable' in str(e): # only print if not typical error
                print(f'ERROR: ({e})')
        
    # Add all time
    print('*** Clicking the time dropdown menu ***')

    
    try:
        dropdown_block = driver.find_element(By.XPATH,'//div[@class="M(0) O(n):f D(ib) Bd(0) dateRangeBtn O(n):f Pos(r)"]')
        print('dropdown block A')
        print(dropdown_block)
        print(dropdown_block.text)
    except:
        dropdown_block = driver.find_element(By.XPATH,'//div[@class="menuContainer  svelte-1j5x891"]')
        print('dropdown block B')
        print(dropdown_block)
        print(dropdown_block.text)
    
    divs = dropdown_block.find_elements(By.TAG_NAME,'div')
    bus = dropdown_block.find_elements(By.TAG_NAME,'button')
    for d in divs:
        try:
            d.click()
        except:
            pass
         
    for b in bus:
        b.click()
    print('*** Clicking on "Max" to get all time data ***')
    try:
        WebDriverWait(driver,10).until(EC.element_to_be_clickable((By.XPATH, '//button[@data-value="MAX"]'))).click()
    except:
        WebDriverWait(driver,10).until(EC.element_to_be_clickable((By.XPATH, '//button[@value="MAX"]'))).click()



    
    print('*** Locating download link ***')
    time.sleep(0.5)
    hyperlinks = driver.find_elements(By.TAG_NAME, 'a')
    for l in hyperlinks:
        try:
            l_text = l.get_attribute('href')
        except:
            pass
        if 'download' in l_text and 'history' in l_text and 'query' in l_text:
            data_url = str(l_text)
            #print(f' dataframe: {data_url}')
    print('*** Retrieving data ***')
    if data_url:
        #print(f'Reading data url: {data_url}')
        df = pd.read_csv(data_url)
        print(df.head(2))
    # remove unnessearcy columns
    df = df.drop(['High','Low','Adj Close'],axis=1)
    print('*** Found the data ***')
    #print(df.head(2))
    return df


def add_weekend_rows(df):
    date_format = '%Y-%m-%d'
    new_rows = []
    # convert dates 
    for j in df.index:
        date_object = datetime.strptime(str(df.loc[j,'Date']), date_format)
        df.loc[j,'Date_2'] = date_object
        if date_object.weekday() == 4:
            saturday = date_object + timedelta(days=1)
            sunday = date_object + timedelta(days=2)
            new_rows.append({'Date':saturday.strftime('%Y-%m-%d'),  'Date_2': saturday, 'Open': df.loc[j,'Open'], 'Close': df.loc[j,'Close'], 'Volume': df.loc[j,'Volume']})
            new_rows.append({'Date':sunday.strftime('%Y-%m-%d'),  'Date_2': sunday, 'Open': df.loc[j,'Open'], 'Close': df.loc[j,'Close'], 'Volume': df.loc[j,'Volume']})
    # Convert new_rows to DataFrame
    new_rows_df = pd.DataFrame(new_rows)
    
    # Concatenate the original df with the new_rows_df and sort by date
    df = pd.concat([df, new_rows_df], ignore_index=True)
    df = df.sort_values(by='Date_2').reset_index(drop=True)
    
    return df
    

def get_current_new(stock_id,vebose):
    """
    description:
        - uses requests and bs4 to get from yahoo finance summary:
            -up-to-date current stock information
            -general informaton on stock
        - can be used to either retrieve general information on a stock or current data (e.g. price)
    
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
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
    url = f'https://finance.yahoo.com/quote/{stock_id}/?p={stock_id}'
    r = requests.get(url, allow_redirects=True, headers = headers)
    print(f'Visiting {url}')
    print(f'Status {r.status_code}')
    if r.status_code == 200:
        full_name, currency, curr_price, volume, market_cap, prev_close, market, divi_abs, divi_p = np.nan, np.nan, np.nan, np.nan, np.nan,np.nan,np.nan,np.nan,np.nan

        out_dict = {}
        soup = BeautifulSoup(r.text,"html.parser")

        # Read Full name
        try:
            name_block = soup.find('h1', class_='svelte-3a2v0c').text
            print(name_block)
        except:
            pass
        if '(' in name_block:
            full_name = name_block.split('(')[0].strip()
        out_dict.update({'full_name':full_name})

        # Read Market and currency
        try:
            exchange_block = soup.find('span', class_='exchange svelte-1fo0o81').text
            print(exchange_block)
        except:
            pass
        market = exchange_block.split('-')[0].strip()
        out_dict.update({'market':market})

        for cu in ['USD','CHF','EUR','GBP','JPY','CAD','SEK','HUF','MYR','RUB','PHP','MXN','INR','INR','IDR','ZAR']:
            if cu in exchange_block.upper():
                currency = cu
        out_dict.update({'curency':currency})
        print(out_dict)
        
        
        main_table = soup.find('div', attrs = {'data-testid':"quote-statistics"})  
        volume = main_table.find('fin-streamer',attrs={"data-field":'regularMarketVolume'}).text.replace(',','')
        out_dict.update({'volume':float(volume)})

        prev_close = main_table.find('fin-streamer',attrs = {"data-field":'regularMarketPreviousClose'}).text.replace(',','')
        out_dict.update({'prev_close':float(prev_close)})

        curr_price = soup.find('fin-streamer', {'data-field':'regularMarketPrice'}).text.replace(',','')
        
        out_dict.update({'curr_price':float(curr_price)})
        market_cap = main_table.find('fin-streamer', {'data-field':'marketCap'}).text
        if 'B' in market_cap:
            market_cap = market_cap.replace('B','')
            market_cap = float(market_cap) * (10**9)
        if 'T' in str(market_cap):
            market_cap = market_cap.replace('T','')
            market_cap = float(market_cap) * (10**12)
        out_dict.update({'market_cap':float(market_cap)})
        
        tab_fields = main_table.find_all('span', class_="value svelte-tx3nkj")
        for en, f in enumerate(tab_fields):
            #print(f.text)
            if '%' in f.text and '(' in f.text:
                divi = f.text
                divi_abs = divi.split()[0]
                divi_p = divi.split()[1].replace('(','').replace(')','').replace('%','')
                out_dict.update({'divi_abs':divi_abs})
                out_dict.update({'divi_p':divi_p})
    print(out_dict)

    return full_name, market, currency, curr_price, prev_close, volume, market_cap, divi_abs, divi_p

def get_current_data(stock_id,verbose):
    """
    description:
        - uses requests and bs4 to get from yahoo finance summary:
            -up-to-date current stock information
            -general informaton on stock
        - can be used to either retrieve general information on a stock or current data (e.g. price)
    
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
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}

    #r = requests.get(url, allow_redirects = True, headers=headers)
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