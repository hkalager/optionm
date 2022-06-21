#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
OptionMetric Package to analyse call and put options by means of
implied volatility and profitability on buy- and sell-sides.

First script created on Fri Apr 01 12:41:24 2022
Current module created on Thu Jun 16 14:14:03 2022

Common disclaimers apply. 
@author: Arman Hassanniakalager GitHub: https://github.com/hkalager

Last review: 21/06/2022
"""
import pandas as pd
import numpy as np
from datetime import datetime,timedelta
from os.path import isfile
import wrds
from multiprocessing import Pool
import matplotlib.pyplot as plt
from statsmodels.stats.weightstats import ttest_ind
from helper_codes import gen_db
from functools import partial
global gen_db

class OptionM:
    '''Inputs: 
    – study_period: range in calendar years (default=range(2001,now.year-1))
    – horizon: number of calendar days to maturity of options (default=91)
    – progress: used for step-size progress report (default=100)

    This module has four methods:
    
    – step1_crsp(): This procedure retrieves data from OptionMetrics and match records with CRSP. 
    The matching is done using 8-char CUSIP numbers. The selected records are US 
    stocks with CRSP Share Code 10 & 11 and Exchange Codes
    1 to 3 (NYSE, AMEX, and Nasdaq). Put and call options for standard contracts
    (100 shares) with  10, 30, 60, 91, 122, 152, 182, 273, 365, 547 and 730 
     days maturity is recorded.
     
    – step2_proc(): This procedure adds three columns to the OptionMetrics dataset:
        * rv_d_hist:          d-day  historical realised volatility 
           calculated as sum of squared daily close-to-close returns
           from d days prior to date to date of record. 
        * rv_d_forward:       d-day  forward realised volatility calculated
                as sum of squared daily close-to-close returns
            from date of record to d days after the date.  
        * real_forward_price: Closing price at the expiry date of the option
        d can be selected from 10, 30, 60, 91, 122, 152, 182, 273, 365, 547 and 730 
        
    – step3_buy(): This procedure compares for top 100 stocks by Market Cap in each year
    degree to which stardard call and put options are gainful. The script links 
    processed data in previous step with CRSP market info and calculates some 
    descriptive stats. 
    All analysis are done for a buy-side interested in hedging/speculating by 
    buying call/put options.
    
    – step4_sell(): This procedure compares for top 100 stocks by Market Cap in each year
    degree to which stardard call and put options are gainful. The script links 
    processed data in previous step with CRSP market info and calculates some 
    descriptive stats. 
    All analysis are done for a sell-side interested in hedging/speculating by 
    selling call/put options.

    '''
    db=wrds.Connection()
    print('Connection established to WRDS ...')
    now=datetime.now()
    __version__='1.0.5'
    def __init__(self,study_period=range(2001,now.year-1),horizon=91,progress=100):
        
        # Check study period entered 
        type_set=[type(s) for s in study_period]
        is_not_int=[idx!=int for idx in type_set]
        if any(is_not_int):
            raise ValueError('The study period must included integers only')
        
        if study_period[0]<1997:
            raise ValueError('OptionMetric records start in 1996. Please use a sample period starting after 1997')
        
        is_not_future=[idx>=self.now.year-1 for idx in study_period]
        
        if any(is_not_future):
            raise ValueError('Invalid entry. OptionMetric records finish in '+str(self.now.year-1))
        
        # Check horizon
        horizon_choices=np.array([10, 30, 60, 91, 122, 152, 182, 273, 365, 547,730])
        if type(horizon)==str:
            horizon=int(horizon)
        if horizon<0:
            horizon=abs(horizon)
            if horizon in horizon_choices:
                pass
            else:
                diff_val=abs(horizon_choices-horizon)
                horizon_idx=np.where(diff_val==min(diff_val))[0][0]
                horizon=horizon_choices[horizon_idx]

        print('The selected horizon is '+str(horizon)+' days')
        print('choices for horizon are 10, 30, 60, 91, 122, 152, 182, 273, 365, 547 and 730 ')
        print('The methods are step1_crsp(), step2_proc(), analyse_buy(), and analyse_sell()')
        
        self.h=horizon
        self.p=progress
        self.s=study_period
    
    
    def step1_crsp(self,study_period=None,horizon=None):
        if study_period==None:
            study_period=self.s
        else:
            self.s=study_period
        
        if horizon==None:
            horizon=self.h
        else:
            self.h=horizon
        
        db=self.db
        
        #optionm_description=db.describe_table('optionm', table='secnmd')
        optionm_tbl=db.get_table(library='optionm', table='secnmd',columns={'secid','cusip'})
        sql_optionm="select distinct secid,cusip from optionm.secnmd"
        optionm_tbl=db.raw_sql(sql_optionm)
        optionm_tbl=optionm_tbl.sort_values(by='secid',ignore_index=True)
        print('Successfully obtained header info for OptionMetrics ...')

        #cusip_description=db.describe_table('cusip_all', 'issue')
        #sql_cusip=""" SELECT distinct issuer_num, issue_num, issue_check, cusip8                                          
        #FROM cusip_all.issue                                                                   
        #WHERE   currency_code='USD' and domicile_code='US' """
        #cusip_master=db.raw_sql(sql_cusip)
        #print('Successfully obtained CUSIP data ...')



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
        WHERE stdopd1996.days = XX                
        	AND stdopd1996.impl_volatility >= 0      
        	AND stdopd1996.days > 0                  
        ORDER BY stdopd1996.secid ASC  """

        sql_query=sql_query.replace('XX',str(horizon))


        for year_sel in range(study_period[0]-1,study_period[-1]+2):
            fl_lbl_crsp='Study_table_'+str(year_sel)+'_'+str(horizon)+'_crsp.csv'
            if isfile(fl_lbl_crsp)==False:
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
                
                # for s in op_table.index:
                #     if op_table.cusip[s] in matched_cusip_list:
                #         included_crsp[s]=1
                #     else:
                #         included_crsp[s]=0
                
                included_crsp=[(op_table.cusip[s] in matched_cusip_list) for s in 
                               op_table.index]
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
    # END OF FIRST PROCEDURE
    
    def step2_proc(self,study_period=None,horizon=None,progress_step=None):
        if study_period==None:
            study_period=self.s
        else:
            self.s=study_period
        
        if horizon==None:
            horizon=self.h
        else:
            self.h=horizon
        
        if progress_step==None:
            progress_step=self.p
        else:
            self.p=progress_step
        
        p=Pool()
        p.map(partial(gen_db,progress_step=progress_step,horizon=horizon),study_period)
        p.terminate()

            
    # END OF SECOND PROCEDURE
    def analyse_buy(self,market_cap_count=100,horizon=None,study_period=None):
        db=self.db        
        if type(horizon)!=int:
            horizon=self.h
        else:
            self.h=horizon
            
        if study_period==None:
            study_period=self.s
        else:
            self.s=study_period
            
        sql_query_init="""select dsf.cusip, dsf.permno, dsf.date, dsf.prc, dsf.shrout,
        dsfhdr.hshrcd, dsfhdr.htick, dsfhdr.hcomnam from crsp.dsf join crsp.dsfhdr on dsfhdr.cusip=dsf.cusip
        where date='1995-12-31' and dsf.hexcd>=1 and dsf.hexcd<=3 and dsfhdr.hshrcd>=10
        and dsfhdr.hshrcd<=11"""
        
        print('Top '+str(market_cap_count)+' US firms by Market Cap are studied between '+
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
            flname='Study_table_'+str(year_sel)+'_'+str(horizon)+'_proc.csv'
            if isfile(flname):
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
                db_top=db_top.drop(db_top[db_top.rv_d_hist==0].index)
                db_top=db_top.reset_index(drop=True)
                
                db_top['profit']=(db_top.cp_flag=='C').astype(int)*\
                    (db_top.real_forward_price-db_top.forward_price-db_top.premium)+\
                        (db_top.cp_flag=='P').astype(int)*\
                            (db_top.forward_price-db_top.real_forward_price-db_top.premium)
                db_top['profit'][db_top['profit']<=-1*db_top['premium']]=\
                    -1*db_top['premium'][db_top['profit']<=-1*db_top['premium']]
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
                
                print('Buy-side analysis completed  for year '+str(year_sel)+' ...')
            else:
                print('Processed dataset missing for year '+str(year_sel)+' ...')
        ## Store results in a DataFrame
        result_tbl=pd.DataFrame()
        result_tbl['year']=study_period

        result_tbl['count p/c']=count_call
        result_tbl['forward/hist vol']=forward_to_hist_ratio

        result_tbl['c implied/hist vol']=call_imp_to_hist_ratio
        result_tbl['c implied/forward vol']=np.power(call_forward_to_imp,-1)

        result_tbl['c %gain']=call_gain_mu

        mean_call_gain=((1+np.mean(call_gain_mu))**(365/horizon))-1


        result_tbl['c in-money ratio']=call_in_money_ratio
        result_tbl['c in-money gain']=call_in_money_mu

        result_tbl['c out-money ratio']=call_out_money_ratio
        result_tbl['c out-money gain']=call_out_money_mu
        
        result_tbl['p implied/hist vol']=put_imp_to_hist_ratio
        result_tbl['p implied/forward vol']=np.power(put_forward_to_imp,-1)
        
        mean_put_gain=((1+np.mean(put_gain_mu))**(365/horizon))-1
        result_tbl['p %gain']=put_gain_mu

        result_tbl['p in-money ratio']=put_in_money_ratio
        result_tbl['p in-money gain']=put_in_money_mu

        result_tbl['p out-money ratio']=put_out_money_ratio
        result_tbl['p out-money gain']=put_out_money_mu

        # Testing call against put for implied/historical and forward/implied

        test_Res1=ttest_ind(call_imp_to_hist_ratio, put_imp_to_hist_ratio)
        p_val_ttest1=test_Res1[1]

        test_Res2=ttest_ind(call_forward_to_imp, put_forward_to_imp)
        p_val_ttest2=test_Res2[1]

        ## Now plotting 

        # First plot basics – various ratios
        fig, ax = plt.subplots()
        X_axis=result_tbl['year']
        X_axis=pd.to_datetime(X_axis,format='%Y')
        ax.plot(X_axis,result_tbl['forward/hist vol'],'s-y',label='forward/historical')

        ax.plot(X_axis,(result_tbl['c implied/hist vol']+\
                        result_tbl['p implied/hist vol'])/2,
                'o-c',label='implied/historical')
        ax.plot(X_axis,(result_tbl['c implied/forward vol']+\
                        result_tbl['p implied/forward vol'])/2,'p-g',label='implied/forward')
        ax.axhline(1,c='k',ls='--',lw=1)
        ax.set_xlabel('Time')
        ax.set_ylabel('Ratio')
        #ax.set_yscale('log')
        ax.set_title(' Various volatility ratios, h='+str(horizon))
        ax.legend(loc='best',fontsize='small',ncol=1)

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
        ax.set_title('% In/Out-of Money Options by Type, h='+str(horizon))
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
        #ax.set_ylim(-10,10)
        ax.set_title('Buy-side Average %Gain for Options by Type, h='+str(horizon))
        ax.legend(loc=0,fontsize='small',ncol=3)

        ## Report on the averages

        print('average annualised gain to BUY CALL is '+str(np.around(mean_call_gain*100,2))+'% for '
              +str(study_period[0])+' - '+str(study_period[-1]))

        
        print('average annualised gain to BUY PUT is '+str(np.around(mean_put_gain*100,2))+'% for '
              +str(study_period[0])+' - '+str(study_period[-1]))
        print('The results are stored in a DataFrame and returned with this method')
        return result_tbl
         
    # END OF THIRD PROCEDURE
    def analyse_sell(self,market_cap_count=100,horizon=None,study_period=None):
        db=self.db        
        if type(horizon)!=int:
            horizon=self.h
        else:
            self.h=horizon
            
        if study_period==None:
            study_period=self.s
        else:
            self.s=study_period
        
        sql_query_init="""select dsf.cusip, dsf.permno, dsf.date, dsf.prc, dsf.shrout,
        dsfhdr.hshrcd, dsfhdr.htick, dsfhdr.hcomnam from crsp.dsf join crsp.dsfhdr on dsfhdr.cusip=dsf.cusip
        where date='1995-12-31' and dsf.hexcd>=1 and dsf.hexcd<=3 and dsfhdr.hshrcd>=10
        and dsfhdr.hshrcd<=11"""
        
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
            flname='Study_table_'+str(year_sel)+'_'+str(horizon)+'_proc.csv'
            if isfile(flname):
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
                db_top=db_top.drop(db_top[db_top.rv_d_hist==0].index)
                db_top=db_top.reset_index(drop=True)
                
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
            else:
                print('Processed dataset missing for year '+str(year_sel)+' ...')
        ## Store results in a DataFrame
        result_tbl=pd.DataFrame()
        result_tbl['year']=study_period

        result_tbl['count p/c']=count_call
        result_tbl['forward/hist vol']=forward_to_hist_ratio

        result_tbl['c implied/hist vol']=call_imp_to_hist_ratio
        result_tbl['c implied/forward vol']=np.power(call_forward_to_imp,-1)

        result_tbl['c %gain']=call_gain_mu

        mean_call_gain=((1+np.mean(call_gain_mu))**(365/horizon))-1

        result_tbl['c in-money ratio']=call_in_money_ratio
        result_tbl['c in-money gain']=call_in_money_mu

        result_tbl['c out-money ratio']=call_out_money_ratio
        result_tbl['c out-money gain']=call_out_money_mu

        result_tbl['p implied/hist vol']=put_imp_to_hist_ratio
        result_tbl['p implied/forward vol']=np.power(put_forward_to_imp,-1)

        result_tbl['p %gain']=put_gain_mu
        mean_put_gain=((1+np.mean(put_gain_mu))**(365/horizon))-1

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
        ax.set_title('Sell-side % In/Out-of Money Options by Type, h='+str(horizon))
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
        #ax.set_ylim(-10,10)
        ax.set_title('Sell-side Average %Gain for Options by Type, h='+str(horizon))
        ax.legend(loc=0,fontsize='small',ncol=3)

        ## Report on the averages

        print('average annualised gain to SELL CALL is '+str(np.around(mean_call_gain*100,2))+'% for '
              +str(study_period[0])+' - '+str(study_period[-1]))

        print('average annualised gain to SELL PUT is '+str(np.around(mean_put_gain*100,2))+'% for '
              +str(study_period[0])+' - '+str(study_period[-1]))
        print('The results are stored in a DataFrame and returned with this method')
        return result_tbl
         
         
