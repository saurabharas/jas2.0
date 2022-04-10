import pandas as pd
import os
from pathlib import Path
from postgresql_conn import Database
from sqlalchemy import create_engine
import logging
from datetime import datetime
import configparser
# conn_string = "host='localhost' dbname='jas' user='postgres' password='sau651994'"
# conn_string_pd = 'postgresql://postgres:sau651994@localhost:5432/jas'
#'postgresql://whitedwarf:#finre123#@finre.cgk4260wbdoi.ap-south-1.rds.amazonaws.com:5432/finre'
config = configparser.ConfigParser()
config.read('config.ini')

conn_string = "host={0} dbname={1} user={2} password={3}".format(
              config['pgdb']['host'],config['pgdb']['db'],config['pgdb']['user'],config['pgdb']['pass'])

conn_string_pd = 'postgresql://{2}:{3}@{0}:5432/{1}'.format(
                  eval(config['pgdb']['host']),eval(config['pgdb']['db']),eval(config['pgdb']['user']),eval(config['pgdb']['pass']))

logging.basicConfig(filename = "error_files_log/mapping_data_{0}.log".format(datetime.now().strftime("%Y_%m_%d")), filemode = 'w')
db_conn = Database(logging, url=conn_string)
db_conn.connect()

engine = create_engine(conn_string_pd)

print("Connection Successfull ....")

def create_mapping_df(excel_mapping):
    df_excel = pd.read_excel(excel_mapping, sheet_name= 'Stock List')
    # print(df_excel)
    # df_excel.rename({'FR Token':'jas_token', 
    #                  'NSE Instrument Token': 'nse_instrument_token',
    #                  'NSE Exchange Token': 'nse_exchange_token',
    #                  'BSE Instrument Token': 'bse_instrument_token',
    #                  'BSE Exchange Token': 'bse_exchange_token',
    #                  'BSE Script Code': 'bse_script_code',
    #                  'BSE Symbol': 'bse_symbol',
    #                  'NSE Symbol': 'nse_symbol',
    #                  'ISIN Code Equity': 'isin_code_equity',
    #                  'Name' : 'name',
    #                  'FR Sector 4': 'fr_sector_4', 
    #                  'FR Sector 3': 'fr_sector_3', 
    #                  'FR Sector 2': 'fr_sector_2', 
    #                  'FR Sector 1': 'fr_sector_1', 
    #                  'Mcap (Crore)': 'mcap_crore', 
    #                  'M Cap Category': 'm_cap_category', 
    #                  'FR Sector Final': 'fr_sector_final', 
    #                  'Sub Sector' : 'sub_sector', 
    #                  'Industry' : 'industry',
    #                  'Industry Segment' : 'industry_segment', 
    #                  'Sector Benchmark' : 'sector_benchmark', 
    #                  'M Cap Benchmark' : 'mcap_benchmark', 
    #                  'Sub Sector' : 'sub_sector', 
    #                  'M Cap BenchmarkSub Sector' : 'm_cap_benchmark', 
    #                  'Peer 1 FR Token' : 'peer_1_jas_token', 
    #                  'Peer 2 FR Token' : 'peer_2_jas_token', 
    #                  'Peer 3 FR Token' : 'peer_3_jas_token', 
    #                  'Peer 4 FR Token' : 'peer_4_jas_token', 
    #                  'Nifty 50 member' : 'nifty_50', 
    #                  'Nifty 500 member' : 'nifty_500' 
    #                 }, axis=1, inplace=True)

    df_excel.rename({'JAS Token':'jas_token', 
                     'NSE Instrument Token': 'nse_instrument_token',
                     'NSE Exchange Token': 'nse_exchange_token',
                     'BSE Instrument Token': 'bse_instrument_token',
                     'BSE Exchange Token': 'bse_exchange_token',
                     'BSE Script Code': 'bse_script_code',
                     'BSE Symbol': 'bse_symbol',
                     'NSE Symbol': 'nse_symbol',
                     'ISIN Code Equity': 'isin_code_equity',
                     'Name' : 'name',
                    #  'FR Sector 4': 'fr_sector_4', 
                    #  'FR Sector 3': 'fr_sector_3', 
                    #  'FR Sector 2': 'fr_sector_2', 
                    #  'FR Sector 1': '_sector_1', 
                     'Mcap (Crore)': 'mcap_crore', 
                     'M Cap Category': 'm_cap_category', 
                     'JAS Sector Final': 'jas_sector_final', 
                     'Sub Sector' : 'sub_sector', 
                     'Industry' : 'industry',
                     'Industry Segment' : 'industry_segment', 
                     'Sector Benchmark' : 'sector_benchmark', 
                     'M Cap Benchmark' : 'mcap_benchmark', 
                     'Sub Sector' : 'sub_sector', 
                     'M Cap BenchmarkSub Sector' : 'm_cap_benchmark', 
                     'Peer 1 JAS Token' : 'peer_1_jas_token', 
                     'Peer 2 JAS Token' : 'peer_2_jas_token', 
                     'Peer 3 JAS Token' : 'peer_3_jas_token', 
                     'Peer 4 JAS Token' : 'peer_4_jas_token', 
                     'Nifty 50 member' : 'nifty_50', 
                     'Nifty 500 member' : 'nifty_500' 
                    }, axis=1, inplace=True)


    df_excel.drop(['JAS token','JAS token.1'], axis = 1, inplace = True)
    # print(df_excel)
    df_excel.to_sql('mapping_data_jas', engine, if_exists='replace') ## Uncomment to write to the database
    db_conn.closeConn()

    # print(df_excel)
    # list_of_dict_df = df_excel.to_dict(orient = 'records')
    # # print("--------------------------------------")
    # # print(dict_val)
    # # columns = dict_val.keys()
    # # values = [dict_val[column] for column in columns]
    # # print(c)
    # cnt = 0-
    # try:
    #     for dict_row in list_of_dict_df:
    #         if cnt == 0:
    #             # insert_statement = "insert into mapping_data  "
    #             cols = dict_row.keys()
    #             row_val = [dict_row[col] for col in cols]
    #             print(cols)
    #             print(row_val)
    #             insert_statement = "insert into mapping_data (%s) values %s"
    #             db_conn.insertQueryDict(insert_statement, cols, row_val)
    #             print("Data Inserted for {0} ------------- {1}".format(dict_row['jas_token'], dict_row['nse_name']))
    #             print("***************************************************")
    #             cnt = cnt + 1
    
    # except Exception as e:
    #     print(e)
    
    # finally:
    #     db_conn.closeConn()
    #     print("**************  Connection Object Closed  ***************")

def index_mapping(excel_mapping):
    df_excel = pd.read_excel(excel_mapping, sheet_name= 'Index List')
    df_excel.rename({'JAS Token':'jas_token', 
                        'Index Name' : 'index_name' 
                }, axis=1, inplace=True)


    print(df_excel)
    df_excel.to_sql('mapping_data_jas_index', engine, if_exists='replace') ## Uncomment to write to the database
    db_conn.closeConn()




# excel_file_path = "../excel_data_input/Sector Industry Peer Tagging - TechTeam Version.xlsx"
excel_file_path = "excel_data_input/SectorIndustryPeerTaggingTechTeamVersionJAS.xlsx"

create_mapping_df(excel_file_path) 
# index_mapping(excel_file_path)
