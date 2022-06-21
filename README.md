# optionm
This repository conducts buy- and sell- side analysis of call and put options on top 100 US firms by market cap. For a thorough introduction see the Wiki tab.

# Warning:
* This is an exploratory study with no profitable strategy in sight.
* Provision of codes is not an investment advice.
* All codes and analyses are subject to error.


# Replication:

– In a terminal window install the requirements as:

` pip install -r requirements.txt`

– In Python environment import the OptionM module as:

` from optionm_module import OptionM as om`

  You will be asked to enter your credentials for accessing WRDS at this stage.

– Specify the module using `study_period` and `horizon` as:

` a=om(tudy_period=range(2001,now.year-1),horizon=91,progress=100)`

Choices for `horizon` are `[10, 30, 60, 91, 122, 152, 182, 273, 365, 547,730]`

– Obtain the necessary OptionMetrics record matched with CRSP through:

` a.step1_crsp()`

– Process the data to generate different proxies of volatitlity matched with each record as:

`a.step2_proc()`

– Analyse the data for a buy-side analysis for top `market_cap_count` firms by market capitalisation as:

`a.analyse_buy(market_cap_count=100)`

– Analyse the data for a sell-side analysis for top `market_cap_count` firms by market capitalisation as:

`a.analyse_sell(market_cap_count=100)`
 
It is suggested that you replicate this process for different maturity periods (e.g. 30, 60, 91, 182, 365) to see the figures as in Wiki tab. 

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
