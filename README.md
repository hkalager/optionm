# optionm
This repository conducts a buy-side analysis of call and put options on top 100 US firms by market cap. For a thorough introduction see the Wiki tab.

# Warning:
* There is no profitable strategy here! 
* All codes and analyses are subject to error.


# Replication:

In order to replicate the results you need to run the py scripts in Python. The steps are identified in file names as "step1_XXX.py", "step2_XXX.py" et cetra. Each file is accompanied by the necessary guidance within the script. 

Make sure you have imported WRDS package into Python before runnning the scripts. 

# Dataset:

– The main chunk of data on option contract including details on premiums, implied volatility, and prices are from [OptionMetrics](https://optionmetrics.com/). 

– Market capitalisation information is obtained from [CRSP](https://www.crsp.org/). Only common stocks (Share Codes 10 and 11) listed in Exchange Codes 1 to 3 (NYSE and NASDAQ) are considered.

– Matching OptionMetric data with CRSP is done through 8-character CUSIP code from [CUSIP Global Services](https://www.cusip.com). 

All datasets are accessed through [WRDS](https://wrds-web.wharton.upenn.edu/wrds/)


# Access requirement:

To access and replicate the results you need an active subscription with WRDS. 
Specifically, you need active subscriptions to OptionMetrics, CUSIP, and CRSP libraries on WRDS.

# Packages 
The following packages in Python to run these scripts:
- WRDS
- Numpy
- Pandas
- statsmodels
- matplotlib
