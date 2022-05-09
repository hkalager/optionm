#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr 01 12:41:24 2022

This script retrieves data from OptionMetrics and match records with CRSP. 
The matching is done using 8-char CUSIP numbers. The selected records are US 
stocks with CRSP Share Code 10 & 11 and Exchange Codes
1 to 3 (NYSE, AMEX, and Nasdaq). Put and call options for standard contracts
(100 shares) with 30 days maturity is recorded.

Common disclaimers apply.

Script by Arman Hassanniakalager GitHub @hkalager

Last reviewed 05/05/2022
"""

import pandas as pd
import numpy as np
import wrds
#from multiprocessing import Pool
from datetime import datetime
import os.path
now=datetime.now()
study_period=range(2001,now.year)
progress_step=100

db=wrds.Connection()
print('Connection established to WRDS ... now getting data')


#optionm_description=db.describe_table('optionm', table='secnmd')
optionm_tbl=db.get_table(library='optionm', table='secnmd',columns={'secid','cusip'})
sql_optionm="select distinct secid,cusip from optionm.secnmd"
optionm_tbl=db.raw_sql(sql_optionm)
optionm_tbl=optionm_tbl.sort_values(by='secid',ignore_index=True)
print('Successfully obtained header info for OptionMetrics ...')

#cusip_description=db.describe_table('cusip_all', 'issue')
sql_cusip=""" SELECT distinct issuer_num, issue_num, issue_check, cusip8                                          
FROM cusip_all.issue                                                                   
WHERE   currency_code='USD' and domicile_code='US' """
cusip_master=db.raw_sql(sql_cusip)
print('Successfully obtained CUSIP data ...')



sql_cusip_join=""" SELECT distinct cusip_all.issue.issuer_num, cusip_all.issue.issue_num, cusip_all.issue.issue_check, 
cusip_all.issue.cusip8, optionm.secnmd.secid                                     
FROM cusip_all.issue join optionm.secnmd on optionm.secnmd.cusip=cusip_all.issue.cusip8                                                          
WHERE   cusip_all.issue.currency_code='USD' and cusip_all.issue.domicile_code='US' """
matched_cusip=db.raw_sql(sql_cusip_join)
print('Successfully obtained merged CUSIP+OptionMetrics data ...')


print('Generating cusip9 ...')
cusip9_list=[]
for s in range(0,matched_cusip.shape[0]):
    
    sel_cusip8=matched_cusip.cusip8[s]
    sel_issuer=matched_cusip.issuer_num[s]
    sel_issue=matched_cusip.issue_num[s]
    sel_check=matched_cusip.issue_check[s]
    cusip9=sel_issuer+sel_issue+sel_check
    cusip9_list.append(cusip9)

matched_cusip['cusip9']=cusip9_list
matched_cusip=matched_cusip.drop(columns=['issuer_num','issue_num','issue_check'])
#crsp_dsf_desc=db.describe_table('crsp', 'dsfhdr')
sql_query_crsp_stocks="""select distinct permno, permco, cusip from crsp.dsfhdr 
where hshrcd >=10 and hshrcd <=11 and hexcd>=1 and hexcd<=3 """
crsp_stocks=db.raw_sql(sql_query_crsp_stocks)
print('Successfully obtained CRSP data ...')

idx_include=[]
pemno_list=[]
for s in range(0,matched_cusip.shape[0]):
    sel_cusip8=matched_cusip.cusip8[s]
    if sel_cusip8 in crsp_stocks.cusip.values:
        idx_include.append(1)
        permno_sel=crsp_stocks.permno[crsp_stocks.cusip==sel_cusip8].iloc[0]
        pemno_list.append(permno_sel)
    else:
        idx_include.append(0)
        pemno_list.append(permno_sel)
matched_cusip['included_CRSP']=idx_include
matched_cusip['permno']=pemno_list
matched_cusip=matched_cusip[matched_cusip.included_CRSP==1]
matched_cusip=matched_cusip.reset_index(drop=True)
matched_cusip_list=matched_cusip.cusip8.to_list()
print('Successfully identified matched CUSIP-CRSP data ...')


sql_query="""SELECT DISTINCT stdopd1996.secid,                  
	secnmd.cusip,
    stdopd1996.date,                         
	stdopd1996.forward_price,                
	stdopd1996.premium,                      
	stdopd1996.impl_volatility,                                
	stdopd1996.cp_flag,                      
	secprd1996.close, 
    secprd1996.return,                       
	hvold1996.volatility                     
FROM (( ( wrds.optionm.stdopd1996          
INNER JOIN wrds.optionm.secprd1996        
ON ( stdopd1996.secid = secprd1996.secid  
	AND stdopd1996.date = secprd1996.date) ) 
INNER JOIN wrds.optionm.hvold1996         
ON ( stdopd1996.secid = hvold1996.secid   
	AND stdopd1996.days = hvold1996.days     
	AND stdopd1996.date = hvold1996.date) )
INNER JOIN wrds.optionm.secnmd       
ON ( stdopd1996.secid = secnmd.secid   ) )
WHERE stdopd1996.days = 30                
	AND stdopd1996.impl_volatility >= 0      
	AND stdopd1996.days > 0                  
ORDER BY stdopd1996.secid ASC  """

for year_sel in range(study_period[0]-1,study_period[-1]+1):
    fl_lbl_crsp='Study_table_'+str(year_sel)+'_crsp.csv'
    if os.path.isfile(fl_lbl_crsp)==False:
        print('data collection started for year '+str(year_sel))
        t0=datetime.now()
        sql_query_sel=sql_query.replace('1996',str(year_sel))
        op_table=db.raw_sql(sql_query_sel,date_cols=['date'])
        op_table=op_table.reset_index(drop=True)
        t1=datetime.now()
        dt=t1-t0
        print('data collection completed for '+str(year_sel)+' after '+str(dt.seconds)+ ' seconds')
        
        included_crsp=np.zeros_like(op_table.index)
        t0=datetime.now()
        print('Identifying derivatives on CRSP for year '+str(year_sel)+' ...')
        
        for s in op_table.index:
            if op_table.cusip[s] in matched_cusip_list:
                included_crsp[s]=1
            else:
                included_crsp[s]=0
        
        op_table['included_crsp']=included_crsp
        op_table=op_table[op_table.included_crsp==1]
        op_table=op_table.drop(columns='included_crsp')
        op_table=op_table.sort_values(by=['secid','date'])
        t1=datetime.now()            
        dt=t1-t0
        print('Matching derivatives with CRSP completed after '+str(dt.total_seconds()
                                                                    )+' secs')
        
        op_table.to_csv(fl_lbl_crsp,index=False)
    else:
        print('Matched OptionMetrics-CRSP dataset exists for year '+str(year_sel))
