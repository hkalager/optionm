# optionm
This repository conducts a buy-side analysis of call and put options on top 100 US firms by market cap. For a thorough introduction see the Wiki tab.

# Warning: 
* All codes and analyses are subject to error.


# Replication:

In order to replicate the results you need to run the py scripts in Python. The steps are identified in file names as "step1_XXX.py", "step2_XXX.py" et cetra. Each file is accompanied by the necessary guidance within the script. In the current format, you need to set the benchmarks for each porfolios seperately. After running the script named "step3_XX.m" you'll get a figure comparing the benchmark excess return vs the knockoff portfolios. 

# Dataset:

– The daily stock data used in this study is from CRSP on WRDS. Once you acquire the account from WRDS you need to enter your username and password into the script "step0_dl_dsf.m" to start collecting the data. 

– The annual fundamentals are from merged Compustat/CRSP file on WRDS. 

– The daily stock indexes closing prices are from Compustat on WRDS. 

– The federal funds rate is from Federal Reserve Board’s H.15 release that contains selected interest rates for U.S. Treasuries and private money market and capital market instruments. All rates are reported in annual terms. Daily figures are for Business days and Monthly figures are averages of Business days unless otherwise noted. The cvs file "Fed_Funds_FRB.csv" contains these rates and is obtained from  https://fred.stlouisfed.org/series/FEDFUNDS


# Access requirement:

To access and replicate the results you need an active subscription with WRDS see (https://wrds-web.wharton.upenn.edu/wrds/). 
Specifically, you need active subscriptions to OptionMetrics and CRSP libraries on WRDS. 

# Packages 
The following packages in Python to run these scripts:
– wrds
– Numpy
– Pandas
– statsmodels
– matplotlib
