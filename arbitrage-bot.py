## By Kejvi Cupa

# Import necessary libraries/packages
import requests
import json
import numpy as np
import math
import pandas as pd
import sys
import os
from itertools import combinations
from discordwebhook import Discord
import concurrent.futures
import time
from datetime import datetime
import random
import csv
import ccxt
#import psycopg2

# Initialize discord Webook for Discord alerts
url = "Your Webhook"
discord = Discord(url=url)

# Initialize DB (Timescale Db)
#db_pass = ''
#db_connection = ""
#connect = psycopg2.connect(db_connection)
#cursor = connect.cursor()

#*************** GLOBAL VARIABLES ******************#
EXCHANGES = ['MEXC', 'GATEIO', 'BINANCE', 'OKX', 'BITMART', 'BITTREX', 'KUCOIN', 'FTX']
EXCHANGES_URL = {'MEXC': "https://www.mexc.com/open/api/v2/market/symbols", 'GATEIO': "https://data.gateapi.io/api2/1/pairs", 
                 'BITMART': "https://api-cloud.bitmart.com/spot/v1/symbols", 'BITTREX': "https://api.bittrex.com/v3/markets", 'BITTREX_USD': "https://api.bittrex.com/v3/markets", 'OKX': "http://www.okx.com/api/v5/market/tickers?instType=SPOT",
                 'KUCOIN': "https://api.kucoin.com/api/v1/symbols", 'BITFINEX': "https://api-pub.bitfinex.com/v2/conf/pub:list:pair:exchange"} # 'BINANCE': "https://api.binance.com/api/v3/exchangeInfo"

#SWITCH_FLAG = 0
# Initialize exchange objects
bitmart = ccxt.bitmart()
bittrex = ccxt.bittrex()
gateio = ccxt.gateio()
#binance = ccxt.binance({'apiKey': '', 'secret': ''})
mexc = ccxt.mexc({"options": {'defaultType': 'spot' }})
okx = ccxt.okx({'apiKey': '', 'secret': '', 'password': ''})
kucoin = ccxt.kucoin()
bitfinex = ccxt.bitfinex()
#ftx = ccxt.ftx()

# Exchange pair cleaner. Get rid of unwanted pairs

def clean_pair_list(pairs):
  unwanted_pairs = ['DAIUSDT', 'BTTUSDT', 'FAMEUSDT', 'QANXUSDT', 'NRFBUSDT', 'BCHSVUSDT', 'SUSDUSDT', 'LMCHUSDT', 'LUNAUSDT', 'VLXUSDT', 'IOTXUSDT', 'DATAUSDT', 'STRUSDT', 'AERGOUSDT', 'KAIUSDT', 'KAI_USDT', 'MDTUSDT', 'MDT_USDT', 'MDT-USDT']
  cleaned_pairs = [x for x in pairs if x not in unwanted_pairs]
  return cleaned_pairs

def format_pairs_list(formatted_pairs):
  pairs_t = formatted_pairs
  for idx, pair in enumerate(pairs_t):
    if str(pair).find('_') > 1:
      pairs_t[idx] = str(pair).replace('_', '') 
    if str(pair).find('-') > 1:
      pairs_t[idx] = str(pair).replace('-', '')
    
  return pairs_t

# Collection functions
def collect_pairs_MEXC():
  url = EXCHANGES_URL['MEXC']
  pairs = []
  resp = requests.get(url)
  data = resp.json()
  for i in range(len(data['data'])):
    if str(data['data'][i]['symbol']).count('3') == 0 and str(data['data'][i]['symbol']).count('5') == 0 and str(data['data'][i]['symbol']).count('4') == 0 and str(data['data'][i]['symbol']).count('2') == 0:
      pairs.append(data['data'][i]['symbol'])
    else:
      continue
  return pairs

def collect_pairs_GATEIO():
  url = EXCHANGES_URL['GATEIO']
  headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
  pairs = []
  resp = requests.get(url, headers = headers)
  data = resp.json()
  for k in range(len(data)):
    if str(data[k]).find('USDT') > 3:
      pairs.append(data[k])
  return pairs

def collect_pairs_BITMART():
  url = EXCHANGES_URL['BITMART']
  pairs = []
  resp = requests.get(url)
  data = resp.json()
  for i in range(len(data['data']['symbols'])):
    if str(data['data']['symbols'][i]).find('USDT') > 3:
      pairs.append(data['data']['symbols'][i])
  return pairs
  
def collect_pairs_BITTREX():
  url = EXCHANGES_URL['BITTREX']
  pairs = []
  resp = requests.get(url)
  data = resp.json()
  for i in range(len(data)):
    if data[i]['quoteCurrencySymbol'] == 'USDT':
      pairs.append(data[i]['symbol'])
  return pairs


def collect_pairs_BINANCE():
  url = EXCHANGES_URL['BINANCE']
  pairs = []
  resp = requests.get(url)
  data = resp.json()
  for i in range(len(data['symbols'])):
    temp = data['symbols'][i]['quoteAsset']
    if temp == 'USDT' and str(data['symbols'][i]['symbol']).find('BEAR') == -1 and str(data['symbols'][i]['symbol']).find('BULL') == -1:
      pairs.append(data['symbols'][i]['symbol'])
  return pairs

def collect_pairs_OKX():
  url = EXCHANGES_URL['OKX']
  pairs = []
  resp = requests.get(url)
  data = resp.json()
  for i in range(len(data['data'])):
    temp = str(data['data'][i]['instId']).split('-')
    if temp[1] == 'USDT':
      pairs.append(data['data'][i]['instId'])
  return pairs

def collect_pairs_KUCOIN():
  url = EXCHANGES_URL['KUCOIN']
  pairs = []
  resp = requests.get(url)
  data = resp.json()
  for i in range(len(data['data'])):
    if data['data'][i]['quoteCurrency'] == 'USDT':
      if str(data['data'][i]['symbol']).count('3') == 0 and str(data['data'][i]['symbol']).count('5') == 0 and str(data['data'][i]['symbol']).count('4') == 0 and str(data['data'][i]['symbol']).count('2') == 0:
        pairs.append(data['data'][i]['symbol'])
  return pairs

def collect_pairs_BITFINEX():
  url = EXCHANGES_URL['BITFINEX']
  pairs = []
  resp = requests.get(url)
  data = resp.json()
  for i in range(len(data[0])):
    if str(data[0][i]).find('USD') > 2:
      loc_1 = str(data[0][i]).find(':')
      loc_2 = str(data[0][i]).find('USD')
      if loc_1 > 2:
        pair = str(data[0][i])[:loc_1] + '-' + str(data[0][i])[loc_1+1:] + 'T'
      else:
        pair = str(data[0][i])[:loc_2] + '-' + str(data[0][i])[loc_2:] + 'T'
  pairs.append(pair)
  return pairs

# URLs and Request Handling

# List of Exchanges to use
# Exchanges to add: FTX, Kucoin, Bitfinex, Huobi Global, Bitstamp, Bybit, Poloniex, Bithump, WhiteBit, Xt.com, Upbit, Bittrue, btcex, aax, digifinex, coinw, bkex

def collect_pairs():
    pairs = []
    all_pairs = {}
    #all_pairs_formatted = {}
    exchange_pair_dict = {'MEXC': collect_pairs_MEXC(), 'GATEIO': collect_pairs_GATEIO(), 'BITMART': collect_pairs_BITMART(),'BITTREX': collect_pairs_BITTREX(), 'KUCOIN': collect_pairs_KUCOIN(), 'BITFINEX': collect_pairs_BITFINEX(), 'OKX': collect_pairs_OKX()} #'BINANCE': collect_pairs_BINANCE()
    #   Call the collect pairs
    #pairs = exchange_pair_dict[current_exchange]
    for i in range(len(EXCHANGES)):
        try:
            exchange_name = EXCHANGES[i]
            pairs = exchange_pair_dict[exchange_name]
            # all_pairs[exchange_name], all_pairs_formatted[exchange_name] = clean_pair_list(pairs), format_pairs_list(pairs)
            formatted_pairs = format_pairs_list(pairs)
            all_pairs[exchange_name] = clean_pair_list(formatted_pairs)  
        except: 
            print('Exchange ' + f'{exchange_name}'+' not currently incorporated. It will be added in the future.')

    return all_pairs

# Functions to collect qoutes on each exchange
def get_quote_MEXC(ticker_p):
    url = "https://www.mexc.com//open/api/v2/market/depth?symbol=" + ticker_p + '&depth=5'
    resp = requests.get(url)

    # Error or Bad Request Handling
    if (str(resp).find('400') > 1 or str(resp).find('404') > 1):
      print(f'Bad Request on MEXC. Pair {ticker_p}')
      mexc_bid, mexc_bid_size, mexc_ask, mexc_ask_size = 0, 0, 0, 0
      return mexc_bid, mexc_bid_size, mexc_ask, mexc_ask_size

    ticker_data = resp.json()
    # Collect all available bids and asks price/quantity
    orderbook_bid_table = [(float(ticker_data['data']['bids'][i]['price']), float(ticker_data['data']['bids'][i]['quantity'])) for i in range(len(ticker_data['data']['bids']))]
    orderbook_ask_table = [(float(ticker_data['data']['asks'][i]['price']), float(ticker_data['data']['asks'][i]['quantity'])) for i in range(len(ticker_data['data']['asks']))]
    
    mexc_bid = orderbook_bid_table[0][0]                            
    mexc_bid_size = orderbook_bid_table[0][1]                       
    mexc_ask = orderbook_ask_table[0][0]                             
    mexc_ask_size = orderbook_ask_table[0][1]                     


    #   Adding delay to prevent API call bottleneck
    #time.sleep(1)
    return mexc_bid, mexc_ask, mexc_bid_size, mexc_ask_size, orderbook_bid_table, orderbook_ask_table

def get_quote_GATEIO(ticker_p):
   
    url = "https://api.gateio.ws/api/v4/spot/order_book?currency_pair=" + ticker_p
    headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
    resp = requests.get(url, headers=headers)
    
    # Error or Bad Request Handling
    if (str(resp).find('400') > 1 or str(resp).find('404') > 1):
      print(f'Bad Request on GateIO')
      gateio_bid, gateio_bid_size, gateio_ask, gateio_ask_size = 0, 0, 0, 0
      return gateio_bid, gateio_bid_size, gateio_ask, gateio_ask_size

    ticker_data = resp.json()

    
    # Collect all available bids and asks price/quantity
    orderbook_bid_table = [(float(ticker_data['bids'][i][0]), float(ticker_data['bids'][i][1])) for i in range(len(ticker_data['bids']))]
    orderbook_ask_table = [(float(ticker_data['asks'][i][0]), float(ticker_data['asks'][i][1])) for i in range(len(ticker_data['asks']))]
    
    gateio_bid = orderbook_bid_table[0][0]                            
    gateio_bid_size = orderbook_bid_table[0][1]                       
    gateio_ask = orderbook_ask_table[0][0]                             
    gateio_ask_size = orderbook_ask_table[0][1]       
    
    #   Adding delay to prevent API call bottleneck
    

    return  gateio_bid, gateio_ask, gateio_bid_size, gateio_ask_size, orderbook_bid_table, orderbook_ask_table


def get_quote_BINANCE(ticker_p):
    #url = "https://api.binance.com/api/v3/ticker/bookTicker?symbol=" + str(ticker_p).replace('_', '') # BTCUSDT  # Format BTCUSDT
    url = "https://www.binance.com/api/v3/depth?symbol=" + str(ticker_p).replace('_', '') # BTCUSDT  # Format BTCUSDT
    resp = requests.get(url)
    
    # Error or Bad Request Handling
    if (str(resp).find('400') > 1 or str(resp).find('404') > 1):
      print(f'Bad Request on Binance')
      binance_bid, binance_bid_size, binance_ask, binance_ask_size = 0, 0, 0, 0
      return binance_bid, binance_bid_size, binance_ask, binance_ask_size

    ticker_data = resp.json()
    # Collect all available bids and asks price/quantity
    orderbook_bid_table = [(float(ticker_data['bids'][i][0]), float(ticker_data['bids'][i][1])) for i in range(len(ticker_data['bids']))]
    orderbook_ask_table = [(float(ticker_data['asks'][i][0]), float(ticker_data['asks'][i][1])) for i in range(len(ticker_data['asks']))]
    
    binance_bid = orderbook_bid_table[0][0]                            
    binance_bid_size = orderbook_bid_table[0][1]                       
    binance_ask = orderbook_ask_table[0][0]                             
    binance_ask_size = orderbook_ask_table[0][1]    

    return binance_bid, binance_ask, binance_bid_size, binance_ask_size, orderbook_bid_table, orderbook_ask_table

def get_quote_OKX(ticker_p):


    ticker = str(ticker_p).replace('_', '-')
    #ticker_data = resp.json()
    try:
      ticker_data = okx.fetch_order_book(ticker)
    except:
      print(f'Error in fetching the orderbook for {ticker} in exchange OKX!')

    # Collect all available bids and asks price/quantity
    orderbook_bid_table = [(float(ticker_data['bids'][i][0]), float(ticker_data['bids'][i][1])) for i in range(len(ticker_data['bids']))]
    orderbook_ask_table = [(float(ticker_data['asks'][i][0]), float(ticker_data['asks'][i][1])) for i in range(len(ticker_data['asks']))]
    
    okx_bid = orderbook_bid_table[0][0]                            
    okx_bid_size = orderbook_bid_table[0][1]                       
    okx_ask = orderbook_ask_table[0][0]                             
    okx_ask_size = orderbook_ask_table[0][1]   
    
    #   Adding delay to prevent API call bottleneck
    
    return okx_bid, okx_ask, okx_bid_size, okx_ask_size, orderbook_bid_table, orderbook_ask_table

def get_quote_BITMART(ticker_p):

    try:
      ticker_data = bitmart.fetch_order_book(ticker_p)
    except:
      print(f'Error in fetching the orderbook for {ticker_p} in exchange BITMART!')

    # Collect all available bids and asks price/quantity
    orderbook_bid_table = [(float(ticker_data['bids'][i][0]), float(ticker_data['bids'][i][1])) for i in range(len(ticker_data['bids']))]
    orderbook_ask_table = [(float(ticker_data['asks'][i][0]), float(ticker_data['asks'][i][1])) for i in range(len(ticker_data['asks']))]
    
    bitmart_bid = orderbook_bid_table[0][0]                            
    bitmart_bid_size = orderbook_bid_table[0][1]                       
    bitmart_ask = orderbook_ask_table[0][0]                             
    bitmart_ask_size = orderbook_ask_table[0][1]   
     

    #   Adding delay to prevent API call bottleneck
    
    return bitmart_bid, bitmart_ask, bitmart_bid_size, bitmart_ask_size, orderbook_bid_table, orderbook_ask_table

def get_quote_BITTREX(ticker_p):
    ticker = str(ticker_p).replace('_', '-')
    try:
      ticker_data = bittrex.fetch_order_book(ticker)
    except:
      print(f'Error in fetching the orderbook for {ticker} in exchange BITTREX!')

    # Collect all available bids and asks price/quantity
    orderbook_bid_table = [(float(ticker_data['bids'][i][0]), float(ticker_data['bids'][i][1])) for i in range(len(ticker_data['bids']))]
    orderbook_ask_table = [(float(ticker_data['asks'][i][0]), float(ticker_data['asks'][i][1])) for i in range(len(ticker_data['asks']))]
    
    bittrex_bid = orderbook_bid_table[0][0]                            
    bittrex_bid_size = orderbook_bid_table[0][1]                       
    bittrex_ask = orderbook_ask_table[0][0]                             
    bittrex_ask_size = orderbook_ask_table[0][1] 
    
    return bittrex_bid, bittrex_ask, bittrex_bid_size, bittrex_ask_size, orderbook_bid_table, orderbook_ask_table
    
def get_quote_KUCOIN(ticker_p):
  
  ticker = str(ticker_p).replace('_', '-')
  try:
    ticker_data = kucoin.fetch_order_book(ticker)
  except:
    print(f'Error in fetching the orderbook for {ticker} in exchange OKX!')

  # Collect all available bids and asks price/quantity
  orderbook_bid_table = [(float(ticker_data['bids'][i][0]), float(ticker_data['bids'][i][1])) for i in range(len(ticker_data['bids']))]
  orderbook_ask_table = [(float(ticker_data['asks'][i][0]), float(ticker_data['asks'][i][1])) for i in range(len(ticker_data['asks']))]
    
  kucoin_bid = orderbook_bid_table[0][0]                            
  kucoin_bid_size = orderbook_bid_table[0][1]                       
  kucoin_ask = orderbook_ask_table[0][0]                             
  kucoin_ask_size = orderbook_ask_table[0][1]   

  
  return kucoin_bid, kucoin_ask, kucoin_bid_size, kucoin_ask_size, orderbook_bid_table, orderbook_ask_table

def get_quote_BITFINEX(ticker_p):
  url = "https://api-pub.bitfinex.com/v2/tickers?symbols=t" + str(ticker_p)[:len(str(ticker_p))-1].replace('_','') #BTCUSDT actually tBTCUSD
  #print(url)
  resp = requests.get(url)
  ticker_data = resp.json()
  
  # Error or Bad Request Handling
  if (str(resp).find('400') > 1 or str(resp).find('404') > 1):
    print(f'Bad Request on Bitfinex')
    bitfinex_bid, bitfinex_ask, bitfinex_bid_size, bitfinex_ask_size  = 0, 0, 0, 0
    return bitfinex_bid, bitfinex_ask, bitfinex_bid_size, bitfinex_ask_size 

  #print(ticker_data)
  try:
    bitfinex_bid, bitfinex_ask = float(ticker_data[0][1]), float(ticker_data[0][3])
    bitfinex_bid_size, bitfinex_ask_size = float(ticker_data[0][2]), float(ticker_data[0][4])
  except:
    url = "https://api-pub.bitfinex.com/v2/tickers?symbols=t" + str(ticker_p)[:len(str(ticker_p))-1].replace('_',':') #BTCUSDT actually tBTCUSD
    #print(url)
    resp = requests.get(url)
    ticker_data = resp.json()
    bitfinex_bid, bitfinex_ask = float(ticker_data[0][1]), float(ticker_data[0][3])
    bitfinex_bid_size, bitfinex_ask_size = float(ticker_data[0][2]), float(ticker_data[0][4])
  return bitfinex_bid, bitfinex_ask, bitfinex_bid_size, bitfinex_ask_size


# **** FUNCTIONS TO GENERATE BID/ASK OF ALL TICKERS OF AN EXCHANGE FOR LATER USE *******

def tickers_bid_ask_MEXC():
  ticker_data = mexc.fetch_tickers()
  exchange_tickers_data = {}
  for idx, pair in enumerate(ticker_data):
    exchange_tickers_data[pair] = {'bid': ticker_data[pair]['info']['bid'], 'ask': ticker_data[pair]['info']['ask']}
  return exchange_tickers_data

def tickers_bid_ask_BITMART():
  ticker_data = bitmart.fetch_tickers()
  #print(ticker_data)
  exchange_tickers_data = {}
  for idx, pair in enumerate(ticker_data):
    exchange_tickers_data[pair] = {'bid': ticker_data[pair]['info']['best_bid'], 'ask': ticker_data[pair]['info']['best_ask']}
    #print(exchange_tickers_data[pair])
  return exchange_tickers_data

def tickers_bid_ask_BITTREX():
  ticker_data = bittrex.fetch_tickers()
  exchange_tickers_data = {}
  for idx, pair in enumerate(ticker_data):
    exchange_tickers_data[pair] = {'bid': ticker_data[pair]['info']['bidRate'], 'ask': ticker_data[pair]['info']['askRate']}
  return exchange_tickers_data

def tickers_bid_ask_GATEIO():
  ticker_data = gateio.fetch_tickers()
  exchange_tickers_data = {}
  for idx, pair in enumerate(ticker_data):
    exchange_tickers_data[pair] = {'bid': ticker_data[pair]['info']['highest_bid'], 'ask': ticker_data[pair]['info']['lowest_ask']}
  return exchange_tickers_data

'''
def tickers_bid_ask_BINANCE():
  ticker_data = binance.fetch_tickers()
  exchange_tickers_data = {}
  for idx, pair in enumerate(ticker_data):
    exchange_tickers_data[pair] = {'bid': ticker_data[pair]['info']['bidPrice'], 'ask': ticker_data[pair]['info']['askPrice']}
  return exchange_tickers_data
'''


def tickers_bid_ask_KUCOIN():
  ticker_data = kucoin.fetch_tickers()
  exchange_tickers_data = {}
  for idx, pair in enumerate(ticker_data):
    exchange_tickers_data[pair] = {'bid': ticker_data[pair]['info']['buy'], 'ask': ticker_data[pair]['info']['sell']}
  return exchange_tickers_data

def tickers_bid_ask_OKX():
  ticker_data = okx.fetch_tickers()
  exchange_tickers_data = {}
  for idx, pair in enumerate(ticker_data):
    exchange_tickers_data[pair] = {'bid': ticker_data[pair]['info']['bidPx'], 'ask': ticker_data[pair]['info']['askPx']}
  return exchange_tickers_data

''' Function to check whether the Deposit/Withdrawal are Enabled and the Arbitage can be initiated. '''

# Collection Function Declarations

def collect_currency_transferStatus_BITMART():
  bitmart_data_table = {}
  bitmart_data = bitmart.fetch_currencies()
  for idx, pair in enumerate(bitmart_data):
    #bitmart_data_table.append((bitmart_data[pair]['info']['id'], bitmart_data[pair]['info']['withdraw_enabled'], bitmart_data[pair]['info']['deposit_enabled']))
    bitmart_data_table[pair] = {'withdraw_enabled': bitmart_data[pair]['info']['withdraw_enabled'], 'deposit_enabled': bitmart_data[pair]['info']['deposit_enabled']}
  return bitmart_data_table

def collect_currency_transferStatus_BITTREX():
  bittrex_data_table = {}
  bittrex_data = bittrex.fetch_currencies()
  for idx, pair in enumerate(bittrex_data):
    withdraw_flag, deposit_flag = False, False
    if(str(bittrex_data[pair]['info']['notice']).find('Deposits and withdrawals are temporarily offline')) < 0:
      withdraw_flag, deposit_flag = True, True
    bittrex_data_table[pair] = {'withdraw_enabled': withdraw_flag, 'deposit_enabled': deposit_flag}
  return bittrex_data_table

def collect_currency_transferStatus_MEXC():
  mexc_data_table = {}
  mexc_data = mexc.fetch_currencies()
  for idx, pair in enumerate(mexc_data):
    mexc_data_table[pair] = {'withdraw_enabled': mexc_data[pair]['info']['coins'][0]['is_withdraw_enabled'], 'deposit_enabled': mexc_data[pair]['info']['coins'][0]['is_deposit_enabled']}
  return mexc_data_table

def collect_currency_transferStatus_GATEIO():
  gateio_data_table = {}
  gateio_data = gateio.fetch_currencies()
  for idx, pair in enumerate(gateio_data):
    gateio_data_table[pair] = {'withdraw_enabled': not gateio_data[pair]['info']['withdraw_disabled'], 'deposit_enabled': not gateio_data[pair]['info']['deposit_disabled']}
  return gateio_data_table

def collect_currency_transferStatus_KUCOIN():
  kucoin_data_table = {}
  kucoin_data = kucoin.fetch_currencies()
  for idx, pair in enumerate(kucoin_data):
    kucoin_data_table[pair] = {'withdraw_enabled': kucoin_data[pair]['info']['isWithdrawEnabled'], 'deposit_enabled': kucoin_data[pair]['info']['isDepositEnabled']}
  return kucoin_data_table

def collect_currency_transferStatus_OKX():
  okx_data_table = {}
  okx_data = okx.fetch_currencies()
  for idx, pair in enumerate(okx_data):
    okx_data_table[pair] = {'withdraw_enabled': okx_data[pair]['withdraw'], 'deposit_enabled': okx_data[pair]['deposit']}
  return okx_data_table
'''
def collect_currency_transferStatus_BINANCE():
  binance_data_table = {}
  binance_data = binance.fetch_currencies()
  for idx, pair in enumerate(binance_data):
    binance_data_table[pair] = {'withdraw_enabled': binance_data[pair]['info']['withdrawAllEnable'], 'deposit_enabled': binance_data[pair]['info']['depositAllEnable']}
  return binance_data_table
'''
# Store the Data Tables for later use

BITMART_DATA_TABLE = collect_currency_transferStatus_BITMART()
BITTREX_DATA_TABLE = collect_currency_transferStatus_BITTREX()
MEXC_DATA_TABLE = collect_currency_transferStatus_MEXC()
GATEIO_DATA_TABLE = collect_currency_transferStatus_GATEIO()
KUCOIN_DATA_TABLE = collect_currency_transferStatus_KUCOIN()
OKX_DATA_TABLE = collect_currency_transferStatus_OKX()
#BINANCE_DATA_TABLE = collect_currency_transferStatus_BINANCE()

def check_deposit_withdrawal_BITMART(bitmart_pair):
  currency = str(bitmart_pair).split('_')[0]
  #print(currency)
  withdraw, deposit = BITMART_DATA_TABLE[currency]['withdraw_enabled'], BITMART_DATA_TABLE[currency]['deposit_enabled']
  return withdraw, deposit

def check_deposit_withdrawal_BITTREX(bittrex_pair):
  currency = str(bittrex_pair).split('_')[0]
  withdraw, deposit = BITTREX_DATA_TABLE[currency]['withdraw_enabled'], BITTREX_DATA_TABLE[currency]['deposit_enabled']
  return withdraw, deposit

'''
def check_deposit_withdrawal_BINANCE(binance_pair):
  currency = str(binance_pair).split('_')[0]
  withdraw, deposit = BINANCE_DATA_TABLE[currency]['withdraw_enabled'], BINANCE_DATA_TABLE[currency]['deposit_enabled']
  return withdraw, deposit
'''
def check_deposit_withdrawal_MEXC(mexc_pair):
  currency = str(mexc_pair).split('_')[0]
  withdraw, deposit = MEXC_DATA_TABLE[currency]['withdraw_enabled'], MEXC_DATA_TABLE[currency]['deposit_enabled']
  return withdraw, deposit

def check_deposit_withdrawal_GATEIO(gateio_pair):
  currency = str(gateio_pair).split('_')[0]
  withdraw, deposit = GATEIO_DATA_TABLE[currency]['withdraw_enabled'], GATEIO_DATA_TABLE[currency]['deposit_enabled']
  return withdraw, deposit

def check_deposit_withdrawal_KUCOIN(kucoin_pair):
  currency = str(kucoin_pair).split('_')[0]
  withdraw, deposit = KUCOIN_DATA_TABLE[currency]['withdraw_enabled'], KUCOIN_DATA_TABLE[currency]['deposit_enabled']
  return withdraw, deposit

def check_deposit_withdrawal_OKX(okx_pair):
  currency = str(okx_pair).split('_')[0]
  withdraw, deposit = OKX_DATA_TABLE[currency]['withdraw_enabled'], OKX_DATA_TABLE[currency]['deposit_enabled']
  return withdraw, deposit

def create_exchange_pairs(collected_pairs): 
    intersected_pairs = {}
    all_pairs_formatted = collected_pairs
    result = list(combinations(all_pairs_formatted, 2))
    #print(result)
    for i in range(len(result)):
        temp1 = all_pairs_formatted[result[i][0]]
        temp2 = all_pairs_formatted[result[i][1]]
        
        intersection = set(temp1).intersection(set(temp2))
        intersection_list = list(intersection)
        
        for j in range(len(intersection_list)):
            pair = str(intersection_list[j])
            #print(pair)
            loc = pair.find('USDT')
            new_pair = pair[:loc] + '_' + pair[loc:]
            intersection_list[j] = new_pair
    
    
        final_intersection = set(intersection_list)
        pair_name = str(result[i][0]) + '-' + str(result[i][1])
        #print(pair_name)
        intersected_pairs[pair_name] = final_intersection
    
    return intersected_pairs

def display_send(pair, cex1, cex1_bid, cex2, cex2_ask, imbalance, potential_profit, total_profit):
  message = '-'*33 + '\n' + '| Arbitrage Opportunity Found!  |\n' + '-'*33 + '\n' + '| Pair: '+ f'{pair}\t\t\t\t\t\n' + f'| {cex1}' + ' Bid: ' + f'{cex1_bid}\t\t\t\t\n' +  f'| {cex2}'+ ' Ask: ' + f'{cex2_ask}\t\t\t\t\n' +'| Imbalance is ' + f'{round(imbalance,2)}%.\t\t\t\t\n' + '| Potential Profit is: ' + f'{round(potential_profit,2)}$ \t\t\n' + '\n' + '| Total Profit is: ' + f'{round(total_profit,2)}$ \t\t\n' + '-'*33 + '\n'
  print(message)
  discord.post(content=message)
  

def display_send2(pair, cex1, cex2_bid, cex2, cex1_ask, imbalance, potential_profit, total_profit):
  message = '-'*33 + '\n' + '| Arbitrage Opportunity Found!  |\n' + '-'*33 + '\n' + '| Pair: '+ f'{pair}\t\t\t\t\t\n' + f'| {cex2}' + ' Bid: ' + f'{cex2_bid}\t\t\t\t\n' +  f'| {cex1}'+ ' Ask: ' + f'{cex1_ask}\t\t\t\t\n' +'| Imbalance is ' + f'{round(imbalance,2)}%.\t\t\t\t\n' + '| Potential Profit is: ' + f'{round(potential_profit,2)}$ \t\t\n' + '\n' + '| Total Profit is: ' + f'{round(total_profit,2)}$ \t\t\n' + '-'*33 + '\n'
  print(message)
  discord.post(content=message)

# ***************************List Manipulation and Transformation ************************
def fix_data(pairs_d): 
  all_pairs = pairs_d
  list_intersected_pairs = list(pairs_d)
  #list_intersected_pairs = list_intersected_pairs[:2]
  data_collection = []
  FIXED_LIST = [(list_intersected_pairs[i], str(list_intersected_pairs[i]).split('-')[0], str(list_intersected_pairs[i]).split('-')[1]) for i in range(len(list_intersected_pairs))]
  return all_pairs, FIXED_LIST

FUNCTIONS = {'get_quote_MEXC': get_quote_MEXC, 'get_quote_GATEIO': get_quote_GATEIO, 
             'get_quote_BINANCE': get_quote_BINANCE, 'get_quote_OKX': get_quote_OKX, 
             'get_quote_BITMART': get_quote_BITMART, 'get_quote_BITTREX': get_quote_BITTREX, 
             'get_quote_KUCOIN': get_quote_KUCOIN,  'get_quote_BITFINEX': get_quote_BITFINEX}
#COUNTER = 0
def exchange_quote_collector(cex1, cex2, pair):
  
  if cex1 in EXCHANGES:
    call = 'get_quote_'+ str(cex1)
    try:
      cex1_bid, cex1_ask, cex1_bid_size, cex1_ask_size, cex1_bid_orderbook, cex1_ask_orderbook = FUNCTIONS[call](pair)
    except:
      #print('Setting quotes to 0')
      cex1_bid, cex1_ask, cex1_bid_size, cex1_ask_size, cex1_bid_orderbook, cex1_ask_orderbook = 0, 0, 0, 0, 0, 0
  
  if cex2 in EXCHANGES:
    call = 'get_quote_'+ str(cex2)
    try:
      cex2_bid, cex2_ask, cex2_bid_size, cex2_ask_size, cex2_bid_orderbook, cex2_ask_orderbook = FUNCTIONS[call](pair)
    except:
      #print('Setting quotes to 0')
      cex2_bid, cex2_ask, cex2_bid_size, cex2_ask_size, cex2_bid_orderbook, cex2_ask_orderbook = 0, 0, 0, 0, 0, 0

  
  return pair, cex1, cex2, cex1_bid, cex1_ask, cex1_bid_size, cex1_ask_size, cex2_bid, cex2_ask, cex2_bid_size, cex2_ask_size

def data_aggregator(pairs, cex1, cex2):

  AGGREGATOR_FUNCTIONS = {'MEXC': MEXC_ASK_BID_DATA, 'BITMART': BITMART_ASK_BID_DATA, 'OKX': OKX_ASK_BID_DATA, 'BITTREX': BITTREX_ASK_BID_DATA,
                          'KUCOIN': KUCOIN_ASK_BID_DATA, 'GATEIO': GATEIO_ASK_BID_DATA}
  data_collector = []
  for idx, pair in enumerate(pairs):
    pair_new = str(pair)
    if pair_new.find('_') < 0:
      pair_new = pair_new.replace('-', '/')
    else:
      pair_new = pair_new.replace('_', '/')

    if cex1 in EXCHANGES:
      try:
        cex1_bid, cex1_ask = float(AGGREGATOR_FUNCTIONS[cex1][pair_new]['bid']), float(AGGREGATOR_FUNCTIONS[cex1][pair_new]['ask'])
      except:
        cex1_bid, cex1_ask = 0, 0
    
    if cex2 in EXCHANGES:
      try:
        cex2_bid, cex2_ask = float(AGGREGATOR_FUNCTIONS[cex2][pair_new]['bid']), float(AGGREGATOR_FUNCTIONS[cex2][pair_new]['ask'])
      except:
        cex2_bid, cex2_ask = 0, 0
    #if pair == 'BTC_USDT':
    #print(pair, cex1, cex2, cex1_bid, cex1_ask, cex2_bid, cex2_ask)
    if cex1_bid == 0 or cex1_ask == 0 or cex2_bid == 0 or cex2_ask == 0:
      continue
    else:
      data_collector.append((pair, cex1, cex2, cex1_bid, cex1_ask, cex2_bid, cex2_ask))

  return data_collector



def send_data_to_db(data):
    data_push = data
    print('Pushing Alert Data to DB')
    query = "INSERT INTO arbitrage_alerts_data (date, pair, exchange1, exchange2, imbalancepercentage, minimumprofit, totalprofit) VALUES(%s, %s, %s, %s, %s, %s, %s)"
    #cursor.execute(query, data_push)
    #connect.commit()
     


def arb(data):
  
  CHECKER_FUNCTIONS = {'MEXC': check_deposit_withdrawal_MEXC, 'GATEIO': check_deposit_withdrawal_GATEIO, 
                       'OKX': check_deposit_withdrawal_OKX, 
                      'BITMART': check_deposit_withdrawal_BITMART, 'BITTREX': check_deposit_withdrawal_BITTREX, 
                      'KUCOIN': check_deposit_withdrawal_KUCOIN}

  QUOTER_FUNCTIONS = {'MEXC': get_quote_MEXC, 'GATEIO': get_quote_GATEIO, 
              'OKX': get_quote_OKX, 
             'BITMART': get_quote_BITMART, 'BITTREX': get_quote_BITTREX, 
             'KUCOIN': get_quote_KUCOIN, 'BITFINEX': get_quote_BITFINEX} 

  stored_data = []
 
  for idx, pair in enumerate(data):
    
    pair_t, cex1, cex2, cex1_bid, cex1_ask, cex2_bid, cex2_ask = str(pair[0]), str(pair[1]), str(pair[2]), float(pair[3]), float(pair[4]), float(pair[5]), float(pair[6])

    try:
            if cex1_bid < 100 and cex1_bid > cex2_ask and cex1_bid / cex2_ask > 1.02: # and pair_cex1_deposit_flag == True and pair_cex2_withdraw_flag == True:
              
              # Once the first condition is met we will check whether deposit/withdrawal is possible
              pair_cex1_withdraw_flag, pair_cex1_deposit_flag = False, False
              pair_cex2_withdraw_flag, pair_cex2_deposit_flag = False, False
              try:
                pair_cex1_withdraw_flag, pair_cex1_deposit_flag = CHECKER_FUNCTIONS[cex1](pair_t)
                pair_cex2_withdraw_flag, pair_cex2_deposit_flag = CHECKER_FUNCTIONS[cex2](pair_t)
              except:
                print(f'Error computing the deposit/withdraw flags for pair {pair_t} on exchange 1 {cex1} or exchange 2{cex2}! Look into it!')

              # Second condition  
              if pair_cex1_deposit_flag == True and pair_cex2_withdraw_flag == True:
                
                # Call for the latest prices, sizes and orderbooks
                try:
                  cex1_bid, cex1_ask, cex1_bid_size, cex1_ask_size, cex1_bid_orderbook, cex1_ask_orderbook = QUOTER_FUNCTIONS[cex1](pair_t)
                except:
                  print(f'Error fetching the orderbook on pair {pair_t} on exchange {cex1}')
                try:
                  cex2_bid, cex2_ask, cex2_bid_size, cex2_ask_size, cex2_bid_orderbook, cex2_ask_orderbook = QUOTER_FUNCTIONS[cex2](pair_t)
                except:
                  print(f'Error fetching the orderbook on pair {pair_t} on exchange {cex2}')

                # Calculate Potential Profit
                potential_profit = (cex1_bid - cex2_ask) * min(cex1_bid_size, cex2_ask_size)
                
                # Calculate Total Profit. Note: The logic is not completely correct but it provides a better estimation of potential profits.

                total_profit = 0
                #profit = 0
                
                # WALK THE BOOK
                for i in range(min(len(cex1_bid_orderbook), len(cex2_ask_orderbook))):
                  current_bid_size = cex1_bid_orderbook[i][1]
                  #print(f'The current bid size is: {current_bid_size}')
                  for j in range(min(len(cex1_bid_orderbook), len(cex2_ask_orderbook))):
                    #print(j)
                    while cex1_bid_orderbook[i][0] > cex2_ask_orderbook[j][0] and current_bid_size > 0:
                      #print(f'The current bid price is: {cex1_bid_orderbook[i][0]}')
                      min_size = min(current_bid_size, cex2_ask_orderbook[j][1])
                      #print(min_size)
                      total_profit += (cex1_bid_orderbook[i][0] - cex2_ask_orderbook[j][0]) * min_size
                      #print(f'The current profit is: {profit}')
                    
                      #print(cex2_ask_orderbook)
                      current_bid_size -= min_size
    
                      #print(f'The current_bid_size is: {current_bid_size}')      
                      if current_bid_size == 0:
                      # Once the current best bid has been exhausted track the index of the ask level
                      
                        new_ask_size = cex2_ask_orderbook[j][1] - min_size
                        orig_len = len(cex2_ask_orderbook)
                        cex2_ask_orderbook = cex2_ask_orderbook[j::]
                        
                        temp_ask_price = cex2_ask_orderbook[0][0]
                        cex2_ask_orderbook.remove(cex2_ask_orderbook[0])
                        
                        cex2_ask_orderbook.insert(0, (temp_ask_price, new_ask_size))
                        
                        for k in range(orig_len-len(cex2_ask_orderbook)):
                            cex2_ask_orderbook.append((1000,1000))
                        break
                      break

                imbalance = ((cex1_bid / cex2_ask) * 100 ) - 100
                
                if float(imbalance) < 20 and cex1_bid > 0.01 and potential_profit > 5 and total_profit > 15:
                  
                # Confirm by requoting again

                  #arbitrage_list.append((list_intersected_pairs[i], pair, cex1_bid, imbalance))
                  display_send(pair_t, cex1, cex1_bid, cex2, cex2_ask, imbalance, potential_profit, total_profit)
                  data_send = ((pair_t, cex1, cex1_bid, cex2, cex2_ask, imbalance, potential_profit, total_profit))
                  data_push = ((datetime.now(), pair_t, cex1, cex2, imbalance, potential_profit, total_profit))
                 # send_data_to_db(data_push)
                  stored_data.append(data_send)

            if cex2_bid < 100 and cex2_bid > cex1_ask and cex2_bid / cex1_ask > 1.02:# and pair_cex2_deposit_flag == True and pair_cex1_withdraw_flag == True:
              
              # Once the first condition is met we will check whether deposit/withdrawal is possible
              pair_cex1_withdraw_flag, pair_cex1_deposit_flag = False, False
              pair_cex2_withdraw_flag, pair_cex2_deposit_flag = False, False
              try:
                pair_cex1_withdraw_flag, pair_cex1_deposit_flag = CHECKER_FUNCTIONS[cex1](pair_t)
                pair_cex2_withdraw_flag, pair_cex2_deposit_flag = CHECKER_FUNCTIONS[cex2](pair_t)
              except:
                print(f'Error computing the deposit/withdraw flags for pair {pair_t} on exchange 1 {cex1} or exchange 2{cex2}! Look into it!')

              # Second condition  
              if pair_cex2_deposit_flag == True and pair_cex1_withdraw_flag == True:
                
                # Call for the latest prices, sizes and orderbooks
                try:
                  cex1_bid, cex1_ask, cex1_bid_size, cex1_ask_size, cex1_bid_orderbook, cex1_ask_orderbook = QUOTER_FUNCTIONS[cex1](pair_t)
                except:
                  print(f'Error fetching the orderbook on pair {pair_t} on exchange {cex1}')
                try:
                  cex2_bid, cex2_ask, cex2_bid_size, cex2_ask_size, cex2_bid_orderbook, cex2_ask_orderbook = QUOTER_FUNCTIONS[cex2](pair_t)
                except:
                  print(f'Error fetching the orderbook on pair {pair_t} on exchange {cex2}')
                # Calculate Potential Profit
                potential_profit = (cex2_bid - cex1_ask) * min(cex2_bid_size, cex1_ask_size)
                # Calculate Total Profit. Note: The logic is not completely correct but it provides a better estimation of potential profits.

                total_profit = 0
                #profit = 0
                for i in range(min(len(cex2_bid_orderbook), len(cex1_ask_orderbook))):
                  current_bid_size = cex2_bid_orderbook[i][1]
                  #print(f'The current bid size is: {current_bid_size}')
                  for j in range(min(len(cex2_bid_orderbook), len(cex1_ask_orderbook))):
                    #print(j)
                    while cex2_bid_orderbook[i][0] > cex1_ask_orderbook[j][0] and current_bid_size > 0:
                      #print(f'The current bid price is: {cex1_bid_orderbook[i][0]}')
                      min_size = min(current_bid_size, cex1_ask_orderbook[j][1])
                      #print(min_size)
                      total_profit += (cex2_bid_orderbook[i][0] - cex1_ask_orderbook[j][0]) * min_size
                      #print(f'The current profit is: {profit}')
                    
                      #print(cex1_ask_orderbook)
                      current_bid_size -= min_size
    
                      #print(f'The current_bid_size is: {current_bid_size}')      
                      if current_bid_size == 0:
                      # Once the current best bid has been exhausted track the index of the ask level
                      
                        new_ask_size = cex1_ask_orderbook[j][1] - min_size
                        orig_len = len(cex1_ask_orderbook)
                        cex1_ask_orderbook = cex1_ask_orderbook[j::]
                        
                        temp_ask_price = cex1_ask_orderbook[0][0]
                        cex1_ask_orderbook.remove(cex1_ask_orderbook[0])
                        
                        cex1_ask_orderbook.insert(0, (temp_ask_price, new_ask_size))
                        
                        for k in range(orig_len-len(cex1_ask_orderbook)):
                            cex1_ask_orderbook.append((1000,1000))
                        break
                      break

                imbalance = ((cex2_bid / cex1_ask) * 100 ) - 100
                
                if float(imbalance) < 20 and cex2_bid > 0.01 and potential_profit > 5 and total_profit > 15:
                  
                  #arbitrage_list.append((list_intersected_pairs[i], pair, cex2_bid, imbalance))
                  
                  display_send2(pair_t, cex1, cex2_bid, cex2, cex1_ask, imbalance, potential_profit, total_profit)
                  data_send_2 = ((pair_t, cex1, cex2_bid, cex2, cex1_ask, imbalance, potential_profit, total_profit))
                  data_push = ((datetime.now(), pair_t, cex1, cex2, imbalance, potential_profit, total_profit))
                  #send_data_to_db(data_push)
                  stored_data.append(data_send_2)

    except:
      
      print(f'ERROR! on pair {pair_t}, between exchanges {cex1} and {cex2}. Please look into it!')
  
  return stored_data

#************ START ***********************************

if __name__ == '__main__':

  # Step 1: Collect all Pairs
  collected_pairs = collect_pairs()
  print('Step 1 Done.')
  # Step 2: Create Intersected Pairs
  pairs_data = create_exchange_pairs(collected_pairs)
  print('Step 2 Done.')
  # Step 3: Fix Data
  pairs_, fixed_data = fix_data(pairs_data)
  print('Step 3 Done.')
  # Step 3: Run 
  #run(fixed_data, pairs_)
  # Step 4: Reshuffle the order of paired exchanges to minimize API bottleneck
  random.shuffle(fixed_data)
  print('Step 4 Done. Reshuffled. ')
  print(fixed_data)
  

  while True:
    t1 = time.perf_counter()
    
    # Collect bid/asks of all pairs in all exchanges
    BITMART_ASK_BID_DATA = tickers_bid_ask_BITMART()
    print('data')
    #BINANCE_ASK_BID_DATA = tickers_bid_ask_BINANCE()
    BITTREX_ASK_BID_DATA = tickers_bid_ask_BITTREX()


    KUCOIN_ASK_BID_DATA = tickers_bid_ask_KUCOIN()
    GATEIO_ASK_BID_DATA = tickers_bid_ask_GATEIO()
    OKX_ASK_BID_DATA = tickers_bid_ask_OKX()
    MEXC_ASK_BID_DATA = tickers_bid_ask_MEXC()

    data_collection = []
    for ex_idx, exchange_tuple in enumerate(fixed_data):
      pairs = list(pairs_[exchange_tuple[0]])
      cex_1 = exchange_tuple[1]
      cex_2 = exchange_tuple[2]
      data_collection += data_aggregator(pairs, cex_1, cex_2)

    # Pass the data collection to the arb logic
    #print(f'The size of the data collection to scan over is: {len(data_collection)}')
    #print(data_collection)
    arb(data_collection)
    time.sleep(60)
    #break
    #arb(data_collection)
    
    #print(data_gen[0])
    t2 = time.perf_counter()
    print(f'The time it took was: {round(((t2-t1)/60),2)} minutes.')
    now = datetime.now()
    current_time = now.strftime("%H:%M:%S")
    print('Searching Again! Current Time: ', current_time)
    #time.sleep(1)
