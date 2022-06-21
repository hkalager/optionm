#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu May  5 10:06:39 2022

This script includes:
    – a function to be called in parallel for the second
    stage of processing records in OptionMetrics dataset. 

Common disclaimers apply.

Script by Arman Hassanniakalager GitHub @hkalager

Last reviewed 17/06/2022
"""

import pandas as pd
import numpy as np
from datetime import datetime,timedelta
from os.path import isfile
import warnings
warnings.filterwarnings("ignore")



def gen_db(year_sel,progress_step=100,horizon=60):
    months_horizon=horizon//30
    months_horizon=months_horizon%12
    yr_horizon=horizon//365
    flname='Study_table_'+str(year_sel)+'_'+str(horizon)+'_proc.csv'
    if isfile(flname)==False:
        db_col_all=pd.DataFrame()
        print('data processing started for year '+str(year_sel))
        fl_lbl_crsp_last='Study_table_'+str(year_sel-1)+'_'+str(horizon)+'_crsp.csv'
        fl_lbl_crsp='Study_table_'+str(year_sel)+'_'+str(horizon)+'_crsp.csv'
        fl_lbl_crsp_next='Study_table_'+str(year_sel+1)+'_'+str(horizon)+'_crsp.csv'
        study_tbl_last=pd.read_csv(fl_lbl_crsp_last)
        db_col_all=db_col_all.append(study_tbl_last)
        study_tbl=pd.read_csv(fl_lbl_crsp)  # This is the main table
        db_col_all=db_col_all.append(study_tbl)
        study_tbl['date']=pd.to_datetime(study_tbl['date'])
        study_tbl_next=pd.read_csv(fl_lbl_crsp_next)
        db_col_all=db_col_all.append(study_tbl_next)
        if type(db_col_all.iloc[0,2])==str:
            db_col_all['date']=pd.to_datetime(db_col_all['date'])
        db_col=db_col_all[(db_col_all['date']>=pd.Timestamp(year_sel-yr_horizon-1, 12-months_horizon-1, 1))]
        db_col=db_col[(db_col['date']<pd.Timestamp(year_sel+yr_horizon+1, months_horizon+1, 1))]
        db_col=db_col.reset_index(drop=True)
        db_col=db_col.sort_values(by=['date','secid'])
        
        rv_d_hist=pd.Series(index=study_tbl.index)
        rv_d_forward=pd.Series(index=study_tbl.index)
        real_forward_price=pd.Series(index=study_tbl.index)
        count_iter=study_tbl.index[-1]-study_tbl.index[0]
        progress_size=count_iter//progress_step
        t21=datetime.now()
        for s in range(study_tbl.index[0],study_tbl.index[-1]):
            sel_asset=int(study_tbl['secid'][s])
            sel_date=study_tbl.date[s]
            cp_flag=study_tbl.cp_flag[s]
            hist_start=sel_date-timedelta(days=horizon)
            forward_date=sel_date+timedelta(days=horizon)
            mini_tbl=db_col[db_col.secid==sel_asset]
            hist_cond=np.logical_and(mini_tbl.secid==sel_asset,
                                     np.logical_and(mini_tbl.date>=hist_start,
                                                    np.logical_and(mini_tbl.date<sel_date,mini_tbl.cp_flag==cp_flag)))
            
            ret_ser_hist=np.unique(mini_tbl['return'][hist_cond])
            rv_d_hist[s]=np.power(np.sum(np.power(ret_ser_hist,2))*252/ret_ser_hist.shape[0],.5)
            forward_cond=np.logical_and(mini_tbl.secid==sel_asset,
                                     np.logical_and(mini_tbl.date<forward_date,
                                                    np.logical_and(mini_tbl.date>=sel_date,mini_tbl.cp_flag==cp_flag)))
            ret_ser_forward=np.unique(mini_tbl['return'][forward_cond])
            rv_d_forward[s]=np.power(np.sum(np.power(ret_ser_forward,2))*252/ret_ser_forward.shape[0],.5)
            real_forward_price[s]=mini_tbl['close'][forward_cond].values[-1]
            
            if (s-study_tbl.index[0]+1)%progress_size==0:
                t22=datetime.now()
                dt2=t22-t21
                progress_made=((s-study_tbl.index[0]+1))//progress_size
                print(str(progress_made)+'% completed after '+
                      str(dt2.seconds)+ ' seconds for year '+str(year_sel))
        study_tbl['rv_d_hist']=rv_d_hist
        study_tbl['rv_d_forward']=rv_d_forward
        study_tbl['real_forward_price']=real_forward_price
        study_tbl=study_tbl.sort_values(by=['date','secid'])
        study_tbl=study_tbl[pd.isna(study_tbl.rv_d_hist)==False]
        
        study_tbl.to_csv(flname,index=False)
    else:
        print('Processed OptionMetrics dataset exists for year '+str(year_sel))

