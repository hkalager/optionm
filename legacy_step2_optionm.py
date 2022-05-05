#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr 29 12:41:24 2022

This script retrieves aims to match OptionMetric records with CRSP and drop
those not matching. The matching is done using 8-char CUSIP numbers. The 
remaining records are US stocks with CRSP Share Code 10 & 11 and Exchange Codes
1 to 3 (NYSE, AMEX, and Nasdaq)

@author: arman
"""

import pandas as pd
import numpy as np
import wrds
#from multiprocessing import Pool
from datetime import datetime
db=wrds.Connection()
print('Connection established to WRDS ...')

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

study_period=range(2001,2022)

for year_sel in range(study_period[0],study_period[-1]+1):
    fl_lbl_crsp='Study_table_'+str(year_sel)+'_crsp.csv'
    # drop the non-CRSP 
    try:
        study_tbl=pd.read_csv(fl_lbl_crsp,index_col=0)
        print('Matched CRSP table exist for year '+str(year_sel)+' ...')
    except:
        fl_lbl='Study_table_'+str(year_sel)+'.csv'
        study_tbl=pd.read_csv(fl_lbl)
        included_crsp=[]
        t0=datetime.now()
        print('Identifying derivatives on CRSP for year '+str(year_sel)+' ...')
        for s in study_tbl.index:
            if study_tbl.cusip[s] in matched_cusip.cusip8.values:
                included_crsp.append(1)
            else:
                included_crsp.append(0)
        t1=datetime.now()            
        dt=t1-t0
        print('Matching derivatives with CRSP completed after '+str(dt.total_seconds()
                                                                    //60)+' mins')
        study_tbl['included_crsp']=included_crsp
        study_tbl=study_tbl[study_tbl.included_crsp==1]
        study_tbl=study_tbl.drop(columns='included_crsp')
        study_tbl.to_csv(fl_lbl_crsp)
    

for year_sel in range(study_period[0],study_period[-1]):
    fl_lbl_crsp='Study_table_'+str(year_sel)+'_crsp.csv'
    fl_lbl_crsp_next='Study_table_'+str(year_sel+1)+'_crsp.csv'
    study_tbl=pd.read_csv(fl_lbl_crsp,index_col=0)
    study_tbl_next=pd.read_csv(fl_lbl_crsp_next,index_col=0)
    study_tbl=study_tbl.append(study_tbl_next)
        



    
