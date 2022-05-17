#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue May 17 11:18:47 2022

This script compares for top 100 stocks by Market Cap in each year
degree to which stardard call and put options are gainful. The script links 
processed data in previous step with CRSP market info and calculates some 
descriptive stats. 

All analysis are done for a sell-side interested in hedging/speculating by 
selling call/put options.

Common disclaimers apply. 
@author: Arman Hassanniakalager GitHub: https://github.com/hkalager

Last review: 17/05/2022
"""

import numpy as np
import pandas as pd
import wrds
import matplotlib.pyplot as plt
from datetime import datetime,timedelta
#from statsmodels.stats.weightstats import ttest_ind
import warnings
warnings.filterwarnings("ignore")
now=datetime.now()
db=wrds.Connection()
print('Connection established to WRDS ... now getting data')
sql_query_init="""select dsf.cusip, dsf.permno, dsf.date, dsf.prc, dsf.shrout,
dsfhdr.hshrcd, dsfhdr.htick, dsfhdr.hcomnam from crsp.dsf join crsp.dsfhdr on dsfhdr.cusip=dsf.cusip
where date='1995-12-31' and dsf.hexcd>=1 and dsf.hexcd<=3 and dsfhdr.hshrcd>=10
and dsfhdr.hshrcd<=11"""
market_cap_count=100
study_period=range(2001,now.year-1)
print('top '+str(market_cap_count)+' US firms by Market Cap are studied between '+
      str(study_period[0])+' - '+str(study_period[-1]))
count_call=[]
forward_to_hist_ratio=[]
call_imp_to_hist_ratio=[]
call_forward_to_imp=[]
call_gain_mu=[]
call_in_money_ratio=[]
call_at_money_ratio=[]
call_out_money_ratio=[]

call_in_money_mu=[]
call_out_money_mu=[]

put_imp_to_hist_ratio=[]
put_forward_to_imp=[]

put_gain_mu=[]
put_in_money_ratio=[]
put_at_money_ratio=[]
put_out_money_ratio=[]

put_in_money_mu=[]
put_out_money_mu=[]

for year_sel in study_period:
    flname=fl_lbl_crsp='Study_table_'+str(year_sel)+'_proc.csv'
    proc_db=pd.read_csv(flname)
    #db.describe_table('crsp', 'dsfhdr')
    crs_tbl=db.raw_sql(sql_query_init)
    day_back=0
    while crs_tbl.shape[0]==0:
        day_back-=1
        last_date_trading=datetime(year=year_sel,month=1,day=1)+timedelta(days=day_back)
        last_date_trading.strftime('%Y-%m-%d')
        new_sql_query=sql_query_init.replace('1995-12-31',
                                             last_date_trading.strftime('%Y-%m-%d'))
        
        crs_tbl=db.raw_sql(new_sql_query)
    crs_tbl['mkval']=crs_tbl.prc*crs_tbl.shrout
    crs_tbl=crs_tbl.sort_values(by='mkval',ascending=False,ignore_index=True)
    top_mkcap_cusip=crs_tbl.cusip[0:market_cap_count].values
    db_top=pd.DataFrame()
    for cusip_top in top_mkcap_cusip:
        proc_db_sel=proc_db[proc_db.cusip.values==cusip_top]
        if proc_db_sel.shape[0]>0:
            db_top=db_top.append(proc_db_sel)
    db_top['profit']=(db_top.cp_flag=='C').astype(int)*\
        (db_top.premium+db_top.forward_price-db_top.real_forward_price)+\
            (db_top.cp_flag=='P').astype(int)*\
                (db_top.premium+db_top.real_forward_price-db_top.forward_price)
    db_top['profit'][db_top['profit']>=db_top['premium']]=\
        db_top['premium'][db_top['profit']>=db_top['premium']]
    db_top['%profit']=db_top['profit']/db_top['forward_price']
    call_tbl=db_top[db_top.cp_flag=='C']
    count_trading_days=np.unique(call_tbl.date).shape[0]
    put_tbl=db_top[db_top.cp_flag=='P']
    count_call_sel=call_tbl.shape[0]
    count_call.append(count_call_sel)
    forward_to_hist_ratio.append(np.mean(db_top.rv_d_forward/db_top.rv_d_hist))
    # First evaluate call options
    call_imp_to_hist_ratio_sel=call_tbl.impl_volatility/call_tbl.rv_d_hist
    call_imp_to_hist_ratio.append(np.mean(call_imp_to_hist_ratio_sel))
    
    call_forward_to_imp_sel=call_tbl.rv_d_forward/call_tbl.impl_volatility
    call_forward_to_imp.append(np.mean(call_forward_to_imp_sel))
    
    
    call_gain_mu.append(np.mean(call_tbl['%profit']))
    ratio_in_money_call=np.sum(call_tbl.profit>0)/count_call_sel
    call_in_money_ratio.append(ratio_in_money_call)
    
    mu_in_money_call=np.mean(call_tbl[call_tbl.profit>0]['%profit'])
    call_in_money_mu.append(mu_in_money_call)
    
    ratio_at_money_call=np.sum(call_tbl.profit==0)/count_call_sel
    call_at_money_ratio.append(ratio_at_money_call)
    
    ratio_out_money_call=np.sum(call_tbl.profit<0)/count_call_sel
    call_out_money_ratio.append(ratio_out_money_call)
    
    mu_out_money_call=np.mean(call_tbl[call_tbl.profit<0]['%profit'])
    call_out_money_mu.append(mu_out_money_call)
    # Now evaluate put options
    count_put_sel=count_call_sel
    
    put_imp_to_hist_ratio_sel=put_tbl.impl_volatility/put_tbl.rv_d_hist
    put_imp_to_hist_ratio.append(np.mean(put_imp_to_hist_ratio_sel))
    
    put_forward_to_imp_sel=put_tbl.rv_d_forward/put_tbl.impl_volatility
    put_forward_to_imp.append(np.mean(put_forward_to_imp_sel))
    
    put_gain_mu.append(np.mean(put_tbl['%profit']))
    ratio_in_money_put=np.sum(put_tbl.profit>0)/count_put_sel
    put_in_money_ratio.append(ratio_in_money_put)
    
    mu_in_money_put=np.mean(put_tbl[put_tbl.profit>0]['%profit'])
    put_in_money_mu.append(mu_in_money_put)
    
    ratio_at_money_put=np.sum(put_tbl.profit==0)/count_put_sel
    put_at_money_ratio.append(ratio_at_money_put)
    
    ratio_out_money_put=np.sum(put_tbl.profit<0)/count_put_sel
    put_out_money_ratio.append(ratio_out_money_put)
    
    mu_out_money_put=np.mean(put_tbl[put_tbl.profit<0]['%profit'])
    put_out_money_mu.append(mu_out_money_put)
    
    print('Sell-side analysis completed for year '+str(year_sel)+' ...')
## Store results in a DataFrame
result_tbl=pd.DataFrame()
result_tbl['year']=study_period

result_tbl['count p/c']=count_call
result_tbl['forward/hist vol']=forward_to_hist_ratio

result_tbl['c implied/hist vol']=call_imp_to_hist_ratio
result_tbl['c implied/forward vol']=np.power(call_forward_to_imp,-1)

result_tbl['c %gain']=call_gain_mu

mean_call_gain=(1+np.mean(call_gain_mu))**12-1

result_tbl['c in-money ratio']=call_in_money_ratio
result_tbl['c in-money gain']=call_in_money_mu

result_tbl['c out-money ratio']=call_out_money_ratio
result_tbl['c out-money gain']=call_out_money_mu

result_tbl['p implied/hist vol']=put_imp_to_hist_ratio
result_tbl['p implied/forward vol']=np.power(put_forward_to_imp,-1)

result_tbl['p %gain']=put_gain_mu
mean_put_gain=(1+np.mean(put_gain_mu))**12-1

result_tbl['p in-money ratio']=put_in_money_ratio
result_tbl['p in-money gain']=put_in_money_mu

result_tbl['p out-money ratio']=put_out_money_ratio
result_tbl['p out-money gain']=put_out_money_mu



## Now plotting 


# plot on % options in and out-of money
fig, ax = plt.subplots()
X_axis=result_tbl['year']
X_axis=pd.to_datetime(X_axis,format='%Y')
ax.plot(X_axis,result_tbl['c in-money ratio']*100,'^-r',label='call in-money')
ax.plot(X_axis,result_tbl['p in-money ratio']*100,'^-b',label='put in-money')
ax.plot(X_axis,result_tbl['c out-money ratio']*100,'v-r',label='call out-of-money')
ax.plot(X_axis,result_tbl['p out-money ratio']*100,'v-b',label='put out-of-money')
ax.axhline(50,c='k',ls='--',lw=1)
ax.set_xlabel('Time')
ax.set_ylabel('% total')
#ax.set_yscale('log')
ax.set_ylim(0,100)
ax.set_title('Sell-side % In/Out-of Money Options by Type')
ax.legend(loc='best',fontsize='small',ncol=2)

# plot on how profitable are options
fig, ax = plt.subplots()
X_axis=result_tbl['year']
X_axis=pd.to_datetime(X_axis,format='%Y')
ax.axhline(0,c='k',ls='--',lw=1)
ax.plot(X_axis,result_tbl['c in-money gain']*100,'^-r',label='in-money call')
ax.plot(X_axis,result_tbl['p in-money gain']*100,'^-b',label='in-money put')
ax.plot(X_axis,result_tbl['c out-money gain']*100,'v-r',label='out-money call')
ax.plot(X_axis,result_tbl['p out-money gain']*100,'v-b',label='out-money put')
ax.plot(X_axis,result_tbl['c %gain']*100,'.-r',lw=2,label='all calls')
ax.plot(X_axis,result_tbl['p %gain']*100,'.-b',lw=2,label='all puts')


ax.set_xlabel('Time')
ax.set_ylabel('% return')
ax.set_ylim(-15,5)
ax.set_title('Sell-side Average Monthly %Gain for Options by Type')
ax.legend(loc=0,fontsize='small',ncol=3)


## Report on the averages

print('average annualised gain to SELL CALL is '+str(np.around(mean_call_gain*100,2))+'% for '
      +str(study_period[0])+' - '+str(study_period[-1]))

mean_put_gain=(1+np.mean(put_gain_mu))**12-1
print('average annualised gain to SELL PUT is '+str(np.around(mean_put_gain*100,2))+'% for '
      +str(study_period[0])+' - '+str(study_period[-1]))

