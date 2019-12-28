import configparser
import os
from alpha_vantage.timeseries import TimeSeries
from alpha_vantage.foreignexchange import ForeignExchange
import time
import pandas as pd
from datetime import datetime as dt
import datetime

config = configparser.ConfigParser()
config.read(os.getcwd() + '\\' +'config.ini')
api_key = config['Settings']['alpha_vantage_api_key']
ticker_list = config['Settings']['ticker_list']
interval = config['Settings']['interval']
get_type = config['Settings']['get_type']

if len(api_key) == 16 and get_type == 'Index':
    ts = TimeSeries(key=api_key, output_format='pandas')
elif len(api_key) == 16 and get_type == 'Forex':
    fe = ForeignExchange(key='api_key', output_format='pandas')
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

possible_get_types = ['Index', 'Forex']
if get_type not in possible_get_types:
    print('ERROR: Enter valid Get_Type.')

tickers = txt.readlines()

exchange = False
j = 0
for ticker in tickers:
    if '.' in ticker:
        j += 1

if j == len(tickers):
    exchange = True

for i, ticker in enumerate(tickers):
    skip = False

    if '\n' in ticker:
        ticker = ticker.strip('\n')
    if ticker.count('.') == 2:
        ticker = ticker.replace('.', '-', 1)
    if exchange == False and '.' in ticker:
        ticker = ticker.replace('.', '-')

    if ticker + '.csv' in os.listdir(os.getcwd() + '\\' + 'Price_Data'):
        last_modification = os.stat(os.getcwd() + '\\' + 'Price_Data\\' + ticker + '.csv')
        time_since_modification = dt.today() - dt.fromtimestamp(last_modification.st_mtime)
        if time_since_modification < datetime.timedelta(hours=12):
            skip = True
            print('No Update necessary for ' + ticker + '.  ' + str(i + 1) + '/' + str(len(tickers)))

    if not skip:
        try:
            if get_type == 'Index':
                df_new, meta = ts.get_intraday(symbol=ticker, interval=interval, outputsize='full')
                name = meta['2. Symbol']
            elif get_type == 'Forex':
                df_new, meta = fe.get_currency_exchange_intraday(from_symbol=ticker[:3], to_symbol=ticker[3:], interval=interval, outputsize='full')
                name = ticker

            # Deal with Windows CON.DE buy
            if name == 'CON.DE':
                name = 'C0N.DE'
                ticker = 'C0N.DE'
            df_new = pd.DataFrame(df_new)

        except ValueError:
            skip = True
            log_file = open(os.getcwd() + '\\log.txt', 'a')
            log_file.write('\n' + str(dt.strptime(str(dt.today())[:str(dt.today()).index('.')-1], '%Y-%m-%d %H:%M:%S')) + '. ERROR: No Update possible for ' + ticker )
            print('ERROR: No Update possible for ' + ticker + '.  ' + str(i + 1) + '/' + str(len(tickers)))
            log_file.close()

    if not os.path.exists('Price_Data'):
        os.makedirs('Price_Data')

    contents = os.listdir(os.getcwd() + '\\' + 'Price_Data')
    if ticker + '.csv' not in contents and skip == False:
        for column in df_new.columns:
            if '' == column or 'Unnamed' in column:
                df_new.drop(column)
        df_new.to_csv(os.getcwd() + '\\' + 'Price_Data' + '\\' + ticker + '.csv')
        print(ticker + ' created.  ' + str(i + 1) + '/' + str(len(tickers)))
        time.sleep(25)

    elif skip == False:
        df_old = pd.read_csv(os.getcwd() + '\\' + 'Price_Data' + '\\' + ticker + '.csv')
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

        print(ticker + ' updated.  ' + str(i+1) + '/' + str(len(tickers)))
        time.sleep(25)


