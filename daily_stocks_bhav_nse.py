# pandas to read csv
import pandas as pd
import time
from datetime import datetime, timedelta

import requests as req
import io
from io import BytesIO, StringIO
import zipfile

from zipfile import ZipFile


import logging
import configparser

# #send Mail
# from testMailPython import mailDesc

# PostgresDB connection
import psycopg2
from sqlalchemy import create_engine

# Relative imports
from postgresql_conn import Database

error_log_data_name = "error_files_log/nse_daily_data_{0}.log".format(
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


# https://www.nseindia.com/content/historical/EQUITIES/%s/%s/cm%s%s%sbhav.csv
# https://www.nseindia.com/content/historical/EQUITIES/2018/MAR/cm05MAR2018bhav.csv.zip
# df_nse_stock_bhav = pd.read_csv(url, compression='gzip', header=0, sep=',', quotechar='"')
# print(df_nse_stock_bhav)


# def writeToMongo(jObjDailyIndex):
#     try:
#         col = nse_daily_stocks[str(nse_daily_stocks["fr_token"])]
#         #db_10_y.jsonObjNseDaily["fr_token"]
#         #col = str(jsonObjNseDaily["fr_token"])
#         col.insert(jObjDailyIndex)
#         #print("Success")
#     except Exception as e:
#         print("Mongo write error : "+str(e))

#         global row_number
#         row_number=row_number+1
#         worksheet.write(row_number, 0, str(jObjDailyIndex["tradingSymbol"]) )
#         worksheet.write(row_number, 1, str(jObjDailyIndex["timestamp"]) )
#         worksheet.write(row_number, 2, str(jObjDailyIndex["timestamp"]) )
#         worksheet.write(row_number, 3, 'Mongo Write Error '+str(e))


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


def getStocksDailyParamsUrl(todaysDate_1, todaysDate_2):
    try:

        paramsArr_1 = todaysDate_1.split("-")
        paramsArr_2 = todaysDate_2.split("-")

        return [paramsArr_1, paramsArr_2]

    except Exception as e:
        print("Pandas Read Csv__zip Error ", flush=True)


def getStocksDailyUrl(rawUrl, paramsArr, paramsArr_2):
    # Eg: '#https://nseindia.com/content/indices/ind_close_all_02022018.csv'
    # https://www.nseindia.com/content/historical/EQUITIES/2018/MAR/cm05MAR2018bhav.csv.zip
    print(
        rawUrl
        % (paramsArr[0], paramsArr_2[1], paramsArr[2], paramsArr_2[1], paramsArr[0])
    )
    ##https://www.nseindia.com/content/historical/EQUITIES/2019/FEB/cm15FEB2019bhav.csv.zip
    return rawUrl % (
        paramsArr[0],
        paramsArr_2[1],
        paramsArr[2],
        paramsArr_2[1],
        paramsArr[0],
    )


def zipToPandas(bhavFinalUrl):
    try:
        # read from zip file
        resp = req.get(bhavFinalUrl)  # getting response from url requests
        # print(io.BytesIO(resp.content))
        my_zip_file = zipfile.ZipFile(
            io.BytesIO(resp.content)
        )  # converting byte content to string and passing to zipfile
        nameFileCsv = (
            my_zip_file.namelist()
        )  # Now from zipfile use method namelist to get array of file names['cm.csv']
        # print(nameFileCsv)
        # for idx, csv_file in enumerate(nameFileCsv):
        #     print(idx, csv_file)
        # if idx in [1, 2, 4, 5, 6, 7, 8, 9, 11, 12]:
        # try:
        #     csvFile = my_zip_file.open(nameFileCsv[idx])
        #     dfBhav = pd.read_csv(csvFile)
        #     dfBhav.to_csv(f"bhav_{idx}.csv")
        # except Exception as e:
        #     print(e)

        csvFile = my_zip_file.open(nameFileCsv[0])
        print(csvFile, flush=True)
        # opening a file in zip folder with filename
        dfBhav = pd.read_csv(csvFile)
        # dfBhav.to_csv("test2.csv")
        # print(dfBhav)

        return dfBhav

    except Exception as e:
        print("Pandas Read Csv Error ", flush=True)
        print(str(e), flush=True)


def postgre_sql_read_df(query):
    """Creates postresql_conn object and closes connection immidiately after usage.
    This will help tackle connection pool exceed issue.

    Arguments:
        query {[String]} -- [SQL query to read data from postgresql and
                            create a pandas dataframe]

    """
    #  "host='localhost' dbname='jas' user='postgres' password='sau651994'"
    # conn_string_alchemy = "postgresql://whitedwarf:#finre123#@finre.cgk4260wbdoi.ap-south-1.rds.amazonaws.com/finre"

    engine = create_engine(conn_string_pd)
    df = pd.read_sql_query(query, con=engine)
    engine.dispose()
    return df


def data_update_nse(dfNseStockDaily, df_mapper):
    # df_mapper = mapper_dataframe_ubuntu()
    dateVal = ""
    fr_token = ""
    # print(df_mapper)
    # print("*******************************************")
    dfNseStockDaily["SERIES"] = pd.Categorical(dfNseStockDaily["SERIES"], ["EQ", "BE"])
    # print(dfNseStockDaily)
    # print("*******************************************")
    dfNseStockDaily = dfNseStockDaily.sort_values("SERIES")
    # print(dfNseStockDaily)
    dfNseStockDaily.drop_duplicates(subset=["SYMBOL"], keep="first", inplace=True)
    # print("*******************************************")

    for (idxPandas, row) in dfNseStockDaily.iterrows():
        try:
            # print("*********Series EQ*********")
            if row["SERIES"] != "":

                # print("*********Series EQ*********")
                jsonObjNseDaily = {}
                """
                { "_id" : "2008-11-26", "high" : 8.82,
                "volume" : 22260, "tradingSymbol" : "20MICRONS", "instrument_token" : 4331777,
                "fr_token" : 0, "close" : 8.4, "timestamp" : "2008-11-26"
                , "nameMarket" : "20 Microns Limited", "open" : 8.62, "low" : 8.28 }
                """
                # print(idxPandas,row)

                df_mapper_condition = df_mapper[
                    df_mapper["nse_symbol"] == row["SYMBOL"]
                ]
                # print(df_mapper_condition)
                p_date = row["TIMESTAMP"]

                m_date_val = datetime.strptime(p_date, "%d-%b-%Y").date()

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
                jsonObjNseDaily["name_market"] = df_mapper_condition.iloc[0]["name"]
                jsonObjNseDaily["timestamp_string"] = m_date_final
                jsonObjNseDaily["trading_symbol"] = df_mapper_condition.iloc[0][
                    "nse_symbol"
                ]
                jsonObjNseDaily["date_new"] = m_new_date_final
                jsonObjNseDaily["timestamp_date"] = timestampNew

                jsonObjNseDaily["open"] = row["OPEN"]
                jsonObjNseDaily["high"] = row["HIGH"]
                jsonObjNseDaily["low"] = row["LOW"]
                jsonObjNseDaily["close"] = row["CLOSE"]
                jsonObjNseDaily["volume"] = row["TOTTRDQTY"]

                # Inserting data into database
                columns = jsonObjNseDaily.keys()
                values = [jsonObjNseDaily[column] for column in columns]
                insert_statement = "insert into nse_data (%s) values %s"

                if jas_token >= 0:
                    db_obj.insertQueryDict(insert_statement, columns, values)
                    print(
                        "running done for {0} -----   nse ----- {1}".format(
                            jas_token, m_date_final
                        ),
                        flush=True,
                    )

        except Exception as e:
            print(
                "Error in NSE Update Daily data for JAS Token: {0}".format(
                    row["SYMBOL"]
                ),
                flush=True,
            )
            print(e, flush=True)
            logging.error("historical_date_to_date function {0}".format(e))
            print(
                "---------------------------------------------------------------",
                flush=True,
            )


def moving_average(df):
    # print(df)
    df["mv_5"] = df["Close"].rolling(window=5).mean().round(2)
    df["mv_50"] = df["Close"].rolling(window=50).mean().round(2)
    df["mv_20"] = df["Close"].rolling(window=20).mean().round(2)
    df["mv_100"] = df["Close"].rolling(window=100).mean().round(2)
    df["mv_200"] = df["Close"].rolling(window=200).mean().round(2)

    return df


def historical_bhav_nse_update():
    # https://www.nseindia.com/content/historical/EQUITIES/2018/MAR/cm05MAR2018bhav.csv.zip
    # url='https://www.nseindia.com/content/historical/EQUITIES/2018/MAR/cm05MAR2018bhav.csv.zip'

    ## For Past Update Firebase/Mongo ########
    start = datetime(2022, 6, 20)
    end = datetime(2022, 8, 7)
    delta = timedelta(days=1)
    d = start
    weekend = set([5, 6])

    while d <= end:
        if d.weekday() not in weekend:
            try:
                dStr = datetime.strftime(d, "%Y-%m-%d")
                print(dStr, flush=True)
                # readIndexDailyFromPandas(dStr)
                todaysDate = d

                todaysDateVal_1 = datetime.strftime(todaysDate, "%Y-%m-%d")
                todaysDateVal_2 = datetime.strftime(todaysDate, "%Y-%b-%d").upper()
                print(dStr, todaysDateVal_1, todaysDateVal_2)

                paramsArr_1, paramsArr_2 = getStocksDailyParamsUrl(
                    todaysDateVal_1, todaysDateVal_2
                )

                # rawStocksDailyUrl = "https://www1.nseindia.com/content/historical/EQUITIES/%s/%s/cm%s%s%sbhav.csv.zip"
                rawStocksDailyUrl = "https://archives.nseindia.com/content/historical/EQUITIES/%s/%s/cm%s%s%sbhav.csv.zip"
                stocksDailyUrlFinal = getStocksDailyUrl(
                    rawStocksDailyUrl, paramsArr_1, paramsArr_2
                )

                # rawStocksDailyUrl = "https://archives.nseindia.com/archives/equities/bhavcopy/pr/PR190221.zip"

                print(stocksDailyUrlFinal, flush=True)
                df_stocks_daily = zipToPandas(stocksDailyUrlFinal)
                print("**********************************************", flush=True)
                print(df_stocks_daily, flush=True)
                print("**********************************************", flush=True)

                # query = "Select * FROM mapping_data_jas"
                query = "Select * FROM mapping_data_jas where jas_token <= 5164;"

                df_mapper = postgre_sql_read_df(query)
                data_update_nse(df_stocks_daily, df_mapper)

            except Exception as e:
                print(e, flush=True)
        d = d + delta


def maindailyBhav():
    ### For Daily Firebase/Mongo ####

    # todaysDate = datetime.now().date() - timedelta(1) ## change here

    todaysDate = datetime.now().date()

    todaysDateVal_1 = datetime.strftime(todaysDate, "%Y-%m-%d")
    todaysDateVal_2 = datetime.strftime(todaysDate, "%Y-%b-%d").upper()
    print(todaysDateVal_1, todaysDateVal_2, flush=True)
    print("************************************", flush=True)
    logging.info(todaysDateVal_1, todaysDateVal_2)
    logging.info("************************************")

    paramsArr_1, paramsArr_2 = getStocksDailyParamsUrl(todaysDateVal_1, todaysDateVal_2)

    # rawStocksDailyUrl = "https://www1.nseindia.com/content/historical/EQUITIES/%s/%s/cm%s%s%sbhav.csv.zip"
    # stocksDailyUrlFinal = (
    #     "https://archives.nseindia.com/archives/equities/bhavcopy/pr/PR010121.zip"
    # )
    # stocksDailyUrlFinal = "https://archives.nseindia.com/content/historical/EQUITIES/2021/FEB/cm19FEB2021bhav.csv.zip"

    rawStocksDailyUrl = "https://archives.nseindia.com/content/historical/EQUITIES/%s/%s/cm%s%s%sbhav.csv.zip"
    stocksDailyUrlFinal = getStocksDailyUrl(rawStocksDailyUrl, paramsArr_1, paramsArr_2)

    print(stocksDailyUrlFinal, flush=True)
    df_stocks_daily = zipToPandas(stocksDailyUrlFinal)
    print("**********************************************", flush=True)
    print(df_stocks_daily, flush=True)
    print("**********************************************", flush=True)
    # df_stocks_daily.to_csv("test file.csv")

    query = "Select * FROM mapping_data_jas"
    df_mapper = postgre_sql_read_df(query)
    data_update_nse(df_stocks_daily, df_mapper)
    # pandaItr(df_stocks_daily)


# maindailyBhav()
# logging.info("Entire File Execution Complete............................")
# print("Entire File Execution Complete............................")
historical_bhav_nse_update()
# closeMongoConnection()
