# basic libs
import time
from datetime import datetime, timedelta

# data process libs
import pandas as pd
import numpy as np

# Scraping libraries
import requests as req
import io
from io import BytesIO, StringIO
import zipfile
from zipfile import ZipFile

# #send Mail
# from testMailPython import mailDesc

# loging libs
import logging

# PostgresDB connection
import psycopg2
from sqlalchemy import create_engine

# Relative imports
from postgresql_conn import Database

import configparser

error_log_data_name = "error_files_log/nse_daily_index_data_{0}.log".format(
    datetime.now().strftime("%Y_%m_%d")
)
logging.basicConfig(filename=error_log_data_name, filemode="w")

# conn_string = "host='localhost' dbname='jas' user='postgres' password='sau651994'"
config = configparser.ConfigParser()
config.read("config.ini")

conn_string = "host={0} dbname={1} user={2} password={3}".format(
    config["pgdb"]["host"],
    config["pgdb"]["db"],
    config["pgdb"]["user"],
    config["pgdb"]["pass"],
)

conn_string_pd = "postgresql://{2}:{3}@{0}:5432/{1}".format(
    eval(config["pgdb"]["host"]),
    eval(config["pgdb"]["db"]),
    eval(config["pgdb"]["user"]),
    eval(config["pgdb"]["pass"]),
)

db_obj = Database(logging, url=conn_string)
db_obj.connect()


def createCandleDF(lists):
    df = pd.DataFrame.from_records(
        lists,
        columns=[
            "Date",
            "Close",
            "volume",
            "high",
            "low",
            "open",
            "nameMarket",
            "tradingSymbol",
        ],
        index="Date",
    )
    df.index = pd.to_datetime(df.index)
    df.Close = df.Close.astype("float")
    df = df.sort_index()
    return df


def getIndexDailyUrl(rawUrl, paramsArr):
    # Eg: '#https://nseindia.com/content/indices/ind_close_all_02022018.csv'
    return rawUrl % (paramsArr[2], paramsArr[1], paramsArr[0])


def zipToPandas(bhavFinalUrl):
    try:
        # read from zip file
        resp = req.get(bhavFinalUrl)  # getting response from url requests
        my_zip_file = zipfile.ZipFile(
            io.BytesIO(resp.content)
        )  # converting byte content to string and passing to zipfile
        nameFileCsv = (
            my_zip_file.namelist()
        )  # Now from zipfile use method namelist to get array of file names['cm.csv']
        csvFile = my_zip_file.open(
            nameFileCsv[0]
        )  # opening a file in zip folder with filename
        dfBhav = pd.read_csv(csvFile)
        # print(dfBhav)
        return dfBhav

    except Exception as e:
        print("Pandas Read Csv Error ", flush=True)


def read_excel_for_mapping_fr_token():
    excel_path = (
        "/home/saurabh/excelfilesfolder/excelNseIdx_3/Indices Portfolio FR mapping.xlsx"
    )
    df = pd.read_excel(excel_path)
    df["indexTradingSymbol"] = df[
        "tradingSymbol"
    ].str.upper()  # Make all letter uppercase and remove all spaces
    df["indexTradingSymbol"] = df["indexTradingSymbol"].str.replace(
        " ", ""
    )  # Make all letter uppercase and remove all spaces
    # print(df)
    return df


def postgre_sql_read_df(query):
    """Creates postresql_conn object and closes connection immidiately after usage.
    This will help tackle connection pool exceed issue.

    Arguments:
        query {[String]} -- [SQL query to read data from postgresql and
                            create a pandas dataframe]

    """
    #  "host='localhost' dbname='jas' user='postgres' password='sau651994'"
    # conn_string_alchemy = "postgresql://whitedwarf:#finre123#@finre.cgk4260wbdoi.ap-south-1.rds.amazonaws.com/finre"
    # conn_string_pd = 'postgresql://postgres:sau651994@localhost:5432/jas'

    engine = create_engine(conn_string_pd)
    df = pd.read_sql_query(query, con=engine)
    engine.dispose()
    return df


def data_update_nse(dfNseIndexDaily, df_mapper):
    # df_mapper = mapper_dataframe_ubuntu()
    dateVal = ""
    fr_token = ""
    # print(df_mapper)
    # print("*******************************************")
    # print(dfNseIndexDaily)
    # print("*******************************************")

    dfNseIndexDaily = dfNseIndexDaily.replace("-", np.nan)
    print(dfNseIndexDaily, flush=True)

    for (idxPandas, row) in dfNseIndexDaily.iterrows():
        try:
            jsonObjNseDaily = {}
            """
            { "_id" : "2008-11-26", "high" : 8.82, 
            "volume" : 22260, "tradingSymbol" : "20MICRONS", "instrument_token" : 4331777,
            "fr_token" : 0, "close" : 8.4, "timestamp" : "2008-11-26"
            , "nameMarket" : "20 Microns Limited", "open" : 8.62, "low" : 8.28 }
            """
            # print(idxPandas,row)

            df_mapper_condition = df_mapper[
                df_mapper["index_name"] == row["Index Name"]
            ]
            p_date = row["Index Date"]

            m_date_val = datetime.strptime(p_date, "%d-%m-%Y").date()

            m_date_final = datetime.strftime(m_date_val, "%Y-%m-%d")
            m_new_date_final = datetime.strftime(m_date_val, "%d-%b-%y")

            timeObj = datetime.strptime(m_date_final, "%Y-%m-%d")
            timestampObj = time.mktime(timeObj.timetuple())
            timestampNew = datetime.utcfromtimestamp(timestampObj)

            # print(df_mapper_condition.iloc[0]['jas_token'])
            jas_token = int(df_mapper_condition.iloc[0]["jas_token"])
            _id = str(jas_token) + "_" + m_date_final

            jsonObjNseDaily["nse_id"] = _id
            jsonObjNseDaily["jas_token"] = jas_token
            jsonObjNseDaily["name_market"] = df_mapper_condition.iloc[0]["index_name"]
            jsonObjNseDaily["timestamp_string"] = m_date_final
            jsonObjNseDaily["trading_symbol"] = df_mapper_condition.iloc[0][
                "index_name"
            ]
            jsonObjNseDaily["date_new"] = m_new_date_final
            jsonObjNseDaily["timestamp_date"] = timestampNew

            jsonObjNseDaily["open"] = row["Open Index Value"]
            jsonObjNseDaily["high"] = row["High Index Value"]
            jsonObjNseDaily["low"] = row["Low Index Value"]
            jsonObjNseDaily["close"] = row["Closing Index Value"]
            jsonObjNseDaily["volume"] = row["Volume"]
            jsonObjNseDaily["pe"] = row["P/E"]
            jsonObjNseDaily["pb"] = row["P/B"]
            jsonObjNseDaily["div_yield"] = row["Div Yield"]

            print(jsonObjNseDaily)
            print("------------------------------------------------------", flush=True)

            # Inserting data into database
            columns = jsonObjNseDaily.keys()
            values = [jsonObjNseDaily[column] for column in columns]
            insert_statement = "insert into nse_index_data (%s) values %s"

            if jas_token > 0:
                db_obj.insertQueryDict(insert_statement, columns, values)
                print(
                    "running done for {0} -----   nse ----- {1}".format(
                        jas_token, m_date_final
                    ),
                    flush=True,
                )

        except Exception as e:
            print("Error in NSE Update Daily data for JAS Token: ", flush=True)
            print(e, flush=True)
            logging.error("historical_date_to_date function {0}".format(e))
            print(
                "---------------------------------------------------------------",
                flush=True,
            )


def getIndexDailyParamsUrl(todaysDate):
    try:
        s_date_final = todaysDate
        paramsArr = s_date_final.split("-")
        # print(paramsArr)
        return paramsArr

    except Exception as e:
        print(e)


def readIndexDailyFromPandas(s_date):

    # https://nseindia.com/content/indices/ind_close_all_02022018.csv

    rawIndexDailyUrl = (
        "https://www1.nseindia.com/content/indices/ind_close_all_%s%s%s.csv"
    )
    paramsArr = getIndexDailyParamsUrl(s_date)

    indexDailyFinalUrl = getIndexDailyUrl(rawIndexDailyUrl, paramsArr)
    # print(indexDailyFinalUrl)
    # #dfBhav=zipToPandas(indexDailyFinalUrl)
    # #print(dfBhav)
    # finalUrl='https://nseindia.com/content/indices/ind_close_all_02022018.csv'
    # csvToPanda(indexDailyFinalUrl,s_date)

    resp = req.get(indexDailyFinalUrl)
    csvFile = io.BytesIO(resp.content)
    # val=io.StringIO(s.content.decode('utf-8'))
    # print(val)
    dfVal = pd.read_csv(csvFile)
    return dfVal
    # print(dfVal)
    # print("-------------------------------------------------")


def historical_nse_index_update():
    ### For Past Update Firebase ####

    start = datetime(2022, 2, 19)
    end = datetime(2022, 4, 9)
    delta = timedelta(days=1)
    d = start
    weekend = set([5, 6])

    while d <= end:
        if d.weekday() not in weekend:
            try:
                dStr = datetime.strftime(d, "%Y-%m-%d")
                print(dStr, flush=True)
                df_index_daily = readIndexDailyFromPandas(dStr)

                query = "Select * FROM mapping_data_jas_index"
                df_mapper = postgre_sql_read_df(query)
                data_update_nse(df_index_daily, df_mapper)

            except Exception as e:
                print(e, flush=True)
        d = d + delta


def daily_nse_index_update():
    # todaysDate = datetime.now().date()-timedelta(7) ##change
    todaysDate = datetime.now().date()
    todaysDateVal = datetime.strftime(todaysDate, "%Y-%m-%d")
    print(todaysDateVal, flush=True)
    df_index_daily = readIndexDailyFromPandas(todaysDateVal)

    query = "Select * FROM mapping_data_jas_index"
    df_mapper = postgre_sql_read_df(query)
    data_update_nse(df_index_daily, df_mapper)


# daily_nse_index_update()
# logging.info("Entire File Execution Complete............................")
# print("Entire File Execution Complete............................", flush=True)
historical_nse_index_update()
### Run This For Single File ###
# mailSend()
