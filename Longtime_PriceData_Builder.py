import configparser
import os
from alpha_vantage.timeseries import TimeSeries
import time
import pandas as pd



config = configparser.ConfigParser()
config.read(os.getcwd() + '\\' +'config.ini')
api_key = config['Settings']['alpha_vantage_api_key']
ticker_list = config['Settings']['ticker_list']
interval = config['Settings']['interval']

if len(api_key) == 16:
    ts = TimeSeries(key=api_key, output_format='pandas')
else:
    print('ERROR: Enter valid Alpha Vantage API key.')
    exit()

try:
    txt = open(os.getcwd() + '\\' + ticker_list, "r")
except FileNotFoundError:
    print('ERROR: Enter valid ticker_list.txt.')
    exit()

possible_intervals = ['1min', '5min', '15min', '30min', '60min']
if interval not in possible_intervals:
    print('ERROR: Enter valid interval.')
    exit()

if not os.path.exists('Price_Data'):
    os.makedirs('Price_Data')


tickers = txt.readlines()

exchange = False
j = 0
for ticker in tickers:
    if '.' in ticker:
        j += 1

if j == len(tickers):
    exchange = True

for i, ticker in enumerate(tickers):
    if '\n' in ticker:
        ticker = ticker.strip('\n')
    if ticker.count('.') == 2:
        ticker = ticker.replace('.', '-', 1)
    if exchange == False and '.' in ticker:
        ticker = ticker.replace('.', '-')

    df_new, meta = ts.get_intraday(symbol=ticker, interval=interval, outputsize='full')
    name = meta['2. Symbol']

    df_new = pd.DataFrame(df_new)

    if not os.path.exists('Price_Data'):
        os.makedirs('Price_Data')

    contents = os.listdir(os.getcwd() + '\\' + 'Price_Data')
    if name + '.csv' not in contents:
        for column in df_new.columns:
            if '' == column or 'Unnamed' in column:
                df.drop(column)
        df_new.to_csv(os.getcwd() + '\\' + 'Price_Data' +  '\\' + name + '.csv')
        print(name + ' created.  ' + str(i + 1) + '/' + str(len(tickers)))
        time.sleep(15)

    else:
        df_old = pd.read_csv(os.getcwd() + '\\' + 'Price_Data' +  '\\' + name + '.csv')
        df_old = pd.DataFrame(df_old)
        df_old.set_index('date', inplace=True)
        df_old = df_old.append(df_new, sort=True)

        for column in df_old.columns:
            if '' == column or 'Unnamed' in column:
                df_old.drop(column, 1, inplace = True)

        df_old['date'] = df_old.index
        df_old.index.names = ['toDelete']
        df_old['date'] = pd.to_datetime(df_old['date'], format='%Y-%m-%d %H:%M:%S')
        df_old.drop_duplicates('date', inplace=True)
        df_old.sort_values('date', ascending=False, inplace=True)
        df_old.set_index('date', inplace=True)

        df_old.to_csv(os.getcwd() + '\\' + 'Price_Data' +  '\\' + name + '.csv')

        print(name + ' updated.  ' + str(i+1) + '/' + str(len(tickers)))
        time.sleep(15)