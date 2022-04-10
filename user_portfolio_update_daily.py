'''
    This file will daily update all the calculations of user like:
    inv_amount,inv_value,total_return,total_realized_return,
    total_unrealized_return,todays_return
'''
import sys
import math
from collections import OrderedDict
from datetime import datetime, timedelta
from datetime import datetime
from dateutil.relativedelta import relativedelta, TU

import pandas as pd
import numpy as np

import psycopg2

# from renameFiles import file_rename
from get_latest_tuesday import latest_tuesday_func
from sqlalchemy import create_engine

from email.mime.text import MIMEText
import psycopg2
from sqlalchemy import create_engine

# CONNSTRING = "host='finre.cgk4260wbdoi.ap-south-1.rds.amazonaws.com' dbname='finre' user='whitedwarf' password='#finre123#'"
# engine = create_engine('postgresql://whitedwarf:#finre123#@finre.cgk4260wbdoi.ap-south-1.rds.amazonaws.com:5432/finre')

'''
Algorithm Rebalancing and Reposistioning:
1.Save user portfolio in psql every week (col:date,_id:uuid_date)
2.Get the last amount of the portfolio
3.Pass that amount and get the latest portfolio for the user
4.Perform Rebalancing and Repositioning on that.
'''
class UserPortfolio():
    def __init__(self):
        self.inv_amount = 0 
        self.inv_val = 0
        self.total_return = 0 
        self.total_realized_return = 0 
        self.total_unrealized_return = 0 
        self.total_unrealized_return_per = 0 
        self.todays_return = 0 
        self.todays_return_per = 0 

        self.cash_eq = 0 

        self.mf_inv_amount = 0 
        self.mf_total_units = 0 
        self.mf_inv_val = 0 
        self.mf_return_val = 0 
        self.mf_unrealized_return = 0 
        self.mf_realized_return = 0 
    
        self.index_inv_amount = 0 
        self.index_inv_val = 0 
        self.total_return_index = 0 
        self.total_return_per_index = 0 
        self.total_unrealized_return_index = 0 
        self.total_unrealized_return_per_index = 0 
        self.todays_return_index = 0 
        self.todays_return_per_index = 0 
    

class MomentumRebalancing():
    def __init__(self):
        pass
    
