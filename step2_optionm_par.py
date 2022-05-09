#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed May  4 12:38:39 2022

This is a parallel version of the script "step2_optionm.py".

This script adds three columns to the OptionMetrics dataset:
    – rv_d_hist:          30-day  historical realised volatility 
       calculated as sum of squared daily close-to-close returns
       from 30 days prior to date to date of record. 
    – rv_d_forward:       30-day  forward realised volatility calculated
            as sum of squared daily close-to-close returns
        from date of record to 30 days after the date.  
    – real_forward_price: Closing price at the expiry date of the option

Common disclaimers apply.

Script by Arman Hassanniakalager GitHub @hkalager

Last reviewed 05/05/2022
"""

from multiprocessing import Pool
from helper_step2_par import gen_db
from datetime import datetime
now=datetime.now()

study_period=range(2001,now.year-1)

if __name__ == '__main__':
    p=Pool()
    p.map(gen_db,study_period)
    p.terminate()
   







