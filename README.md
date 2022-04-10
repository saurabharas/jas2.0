# Momentum Strategy Swing Trading
This is a python project with momentum trading strategy. Momentum Trading Strategy is a swing trading strategy which looks for price action and rebalances on the basis of ATR (volatality matrics).

## Features 
- Phase I (Live)
    1. Historical Database of daily nifty 500 stocks from zerodha.
    2. Mapping Database for mapping instrument token of NSE and BSE stocks.
    3. Momentum strategy excel sheet generation and Database of Stocks to buy on basis of User Investment amount.

- Phase II (TODO)
    1. Create a User Portfolio Database for every user trading with momentum strategy.
    2. Link with Mutual Fund Data for placing Inv amount in Liquid Funds when strategy in downtrend.

- Phase III (TODO)
    1. Allow user to place orders with zerodha from app/website.
    2. Chnage the User portfolio database on the basis of prices placed during market execution.
    3. Create a Unittest Framework for testing the all the systems.


## Files and Flow of Project
- zerodha_kite_connect.py : It takes the historical data from zerodha kite connect API and places in the database.
- mapping_data.py - creates a mapping database on the basis of mapping file.
- stockmomentumcalc.py : Main file with code regarding momentum strategy. Creates a excel sheet of stocks to buy.
- pstgresql_conn.py : Database file for postgresql database connection.
- user_profolio_update_daily.py : updates the user portfolio on the basis of stocks bought. Updates Info such as Inv Value, Profit etc.
- protfolio_management_update.py : updates the user portfolio and sends what data to buy and sell (rebalancing). ** Not Required If order placing through zerodha is achieved **

