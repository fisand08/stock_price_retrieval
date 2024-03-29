# stocks engine

Script to retrieve and maintain an archive of stock data. Designed to run on server 24/7. 

## Description

* Retrieves historic data for stocks
* Keeps stock data up to date on a daily basis after markets close

## Details

* Yahoo Finance is used to retrieve the stock data. Historic data is retrieved using Selenium, while daily updates are done using requests
* Stock data is stored in "stock_data" directory in CSV format
* In "inputs" directory, a file with stock market closing times in relation to Swiss/Zurich time is stored. It is used to determine the point when to update the stock archive on a daily basis.
* Additional stock market details as well as additional stocks of interest can be added dynamically via text/CSV file. 


## Getting Started

### Dependencies

* pandas 
* selenium
* lxml
* bs4
* numpy
* requests

### Executing program

* How to run the program

Call handler function in python code with path to input file with stock names. "verbose" is a boolean to print certain information to command line
```
from get_stock_price import handler
handler(verbose, stock_input_file)
```

## Future developments

* For perpetual use, add the while statement to while == True, remove the iterator
* Slight changes on how the code is called from external programs (e.g. not by file, but by list of stocks)
* Improvement of commenting of functions
* Improve verbosity a bit

## Version History

* 0.1
    * Initial Release

