
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
        l_text = l.get_attribute('href')
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
