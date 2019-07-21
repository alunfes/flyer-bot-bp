import websocket
import threading
import time
import json
import asyncio
import ssl
from statistics import mean, median,variance,stdev
from datetime import datetime
import pandas as pd
import pytz
import numpy as np
from OneMinData import OneMinData
from LineNotification import LineNotification


class WebsocketMaster:
    def __init__(self, channel, symbol=''):
        self.symbol = symbol
        self.ticker = None
        self.message = None
        self.exection = None
        self.channel = channel
        self.connect()
        self.time_start = time.time()

    def connect(self):
        self.ws = websocket.WebSocketApp(
            'wss://ws.lightstream.bitflyer.com/json-rpc', header=None,
            on_open = self.on_open, on_message = self.on_message,
            on_error = self.on_error, on_close = self.on_close)
        self.ws.keep_running = True
        websocket.enableTrace(False)
        #self.thread = threading.Thread(target=lambda: self.ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE}))
        self.thread = threading.Thread(target=lambda: self.ws.run_forever())
        self.thread.daemon = True
        self.thread.start()

    def is_connected(self):
        return self.ws.sock and self.ws.sock.connected

    def disconnect(self):
        print('disconnected')
        self.ws.keep_running = False
        self.ws.close()


    def on_message(self, ws, message):
        message = json.loads(message)['params']
        self.message = message['message']
        if self.channel == 'lightning_executions_' and self.symbol =='FX_BTC_JPY':
            if self.message is not None:
                self.exection = self.message
                TickData.add_exec_data(self.exection)
                TickData.add_exec_latest_data(self.exection)
        elif self.channel == 'lightning_ticker_' and self.symbol =='FX_BTC_JPY':
            if self.message is not None:
                self.ticker = self.message
                TickData.add_ticker_data(self.ticker)
        elif self.channel == 'lightning_executions_' and self.symbol =='BTC_JPY':
            if self.message is not None:
                TickData.add_btc_data(self.message)

    def on_error(self, ws, error):
        print('websocket error!')
        try:
            if self.is_connected():
                self.disconnect()
        except Exception as e:
            print('websocket - '+str(e))
            TickData.ws_down_flg  =True
        time.sleep(3)
        self.connect()

    def on_close(self, ws):
        print('Websocket disconnected')


    def on_open(self, ws):
        ws.send(json.dumps( {'method':'subscribe',
            'params':{'channel':self.channel + self.symbol}} ))
        time.sleep(1)
        print('Websocket connected for '+self.channel)


    async def loop(self):
        while True:
            await asyncio.sleep(1)



'''
TickData class
'''
class TickData:
    @classmethod
    def initialize(cls):
        cls.exec_lock = threading.Lock()
        cls.exec_latest_lock = threading.Lock()
        cls.ticker_lock = threading.Lock()
        cls.ohlc_lock = threading.Lock()
        cls.btc_lock = threading.Lock()
        cls.ws_down_flg = False
        cls.ltp = []
        cls.exec_data = []
        cls.exec_latest_data = []
        cls.ticker_data = []
        cls.btc_data = []
        cls.std_1m = 0
        cls.ohlc = OneMinData()
        cls.ohlc.initialize()
        cls.last_ohlc_min = int(datetime.now().minute) + 1 if datetime.now().minute != 59 else 0
        cls.JST = pytz.timezone('Asia/Tokyo')
        cls.send_notification_time = 0
        cls.ws_execution = WebsocketMaster('lightning_executions_', 'FX_BTC_JPY')
        cls.ws_ticker = WebsocketMaster('lightning_ticker_', 'FX_BTC_JPY')
        cls.ws_btc = WebsocketMaster('lightning_executions_', 'BTC_JPY')
        th = threading.Thread(target=cls.start_thread)
        th.start()

    @classmethod
    def stop(cls):
        cls.ws_ticker.disconnect()
        cls.ws_execution.disconnect()
        LineNotification.send_error('stopped ws threads')

    @classmethod
    def get_ltp(cls):
        with cls.exec_lock:
            if len(cls.exec_data) > 0:
                return cls.exec_data[-1]['price']
            else:
                return None

    @classmethod
    def get_btc(cls):
        with cls.btc_lock:
            if len(cls.btc_data) > 0:
                return cls.btc_data[-1]['price']
            else:
                return None

    @classmethod
    def get_sfd(cls):
        ltp = cls.get_ltp()
        btc = cls.get_btc()
        if ltp != None and btc != None:
            return (ltp - btc) / btc
        else:
            return None

    @classmethod
    def get_ohlc(cls):
        if len(cls.ohlc.unix_time) > 0:
            return cls.ohlc
        else:
            return None

    '''
    [{'id': 1046951795, 'side': 'SELL', 'price': 654622, 'size': 0.1, 'exec_date': '2019-05-07T12:03:23.1848477Z', 
    'buy_child_order_acceptance_id': 'JRF20190507-120321-516068', 'sell_child_order_acceptance_id': 'JRF20190507-120323-681291'}]
    '''
    @classmethod
    def get_exe_data(cls):
        return cls.exec_data[:]

    @classmethod
    def get_latest_exec_data(cls):
        with cls.exec_latest_lock:
            data = cls.exec_latest_data[:]
            cls.exec_latest_data = []
            return data

    @classmethod
    def get_exec_ws_status(cls):
        return cls.ws_execution.is_connected()


    @classmethod
    def get_bid_price(cls):
        with cls.ticker_lock:
            if len(cls.ticker_data) > 0:
                return cls.ticker_data[-1]['best_bid']
            else:
                return None

    @classmethod
    def get_ask_price(cls):
        with cls.ticker_lock:
            if len(cls.ticker_data) > 0:
                return cls.ticker_data[-1]['best_ask']
            else:
                return None

    @classmethod
    def start_thread(cls):
        while True:
            cls.__check_thread_status()
            cls.__send_notification()
            #cls.__calc_std(list([d.get('ltp') for d in cls.ticker_data]))
            time.sleep(1)

    @classmethod
    def __send_notification(cls):
        if time.time() - cls.send_notification_time >= 3600:
            cls.send_notification_time = time.time()
            LineNotification.send_error('ws is active')

    '''
    00:00:10 started, last_min = 00:01
    00:00:10 no action as not enough data
    00:01:00 no action as not enough data
    00:02:00 calc data for 00:01, last_min = 00:02
    00:02:20 no action as not enough data, last_min = 00:02
    '''
    @classmethod
    def __calc_ohlc(cls):
        if datetime.now(cls.JST).minute > cls.last_ohlc_min or (cls.last_ohlc_min == 59 and datetime.now(cls.JST).minute == 0):
            executions = cls.get_exe_data()
            dlist = [d.get('exec_date') for d in executions]
            p = [d.get('price') for d in executions]
            size = [d.get('size') for d in executions]
            i = 1
            target_min = int(datetime.now(cls.JST).minute - 1 if datetime.now(cls.JST).minute > 0 else 59)
            plist = []
            sizelist = []
            while int(dlist[-i].split('T')[1].split(':')[1]) != target_min:
                i+= 1
            while int(dlist[-i].split('T')[1].split(':')[1]) == target_min:
                plist.append(p[-i])
                sizelist.append(size[-i])
                i += 1
            zurashi_time = target_min + 1 if target_min != 59 else 0 # to consist with cryptowatch data
            cls.ohlc.dt.append(datetime(datetime.now(cls.JST).year,datetime.now(cls.JST).month,datetime.now(cls.JST).day,datetime.now(cls.JST).hour,zurashi_time,0))
            cls.ohlc.unix_time.append(cls.ohlc.dt[-1].timestamp())
            cls.ohlc.open.append(plist[-1])
            cls.ohlc.high.append(max(plist))
            cls.ohlc.low.append(min(plist))
            cls.ohlc.close.append(plist[0])
            cls.ohlc.size.append(sum(sizelist))
            cls.last_ohlc_min = datetime.now(cls.JST).minute
            #print('ohlc calc completion='+str(datetime.now()))
            #print('dt={},ut={},open={},high={},low={},close={}'.format(omd.dt, omd.unix_time, omd.open, omd.high,omd.low, omd.close))

    @classmethod
    def __check_thread_status(cls):
        if cls.ws_execution.is_connected == False:
            cls.ws_execution.connect()
            cls.ws_down_flg = True
        if cls.ws_ticker.is_connected == False:
            cls.ws_ticker.connect()

    @classmethod
    def add_exec_data(cls, exec):
        if len(exec) > 0:
            with cls.exec_lock:
                cls.exec_data.extend(exec)
                if len(cls.exec_data) >= 30000:
                    del cls.exec_data[:-10000]
            with cls.ohlc_lock:
                cls.__calc_ohlc()

    @classmethod
    def add_exec_latest_data(cls,exec):
        if len(exec) > 0:
            with cls.exec_latest_lock:
                cls.exec_latest_data.extend(exec)

    @classmethod
    def add_btc_data(cls, message):
        if len(message) > 0:
            with cls.btc_lock:
                cls.btc_data.extend(message)
                if len(cls.btc_data) >= 30000:
                    del cls.btc_data[:-10000]

    @classmethod
    def add_ticker_data(cls, ticker):
        if len(ticker) is not None:
            with cls.ticker_lock:
                cls.ticker_data.append(ticker)
                if len(cls.ticker_data) >= 30000:
                    del cls.ticker_data[:-10000]
            cls.ltp.append(ticker['ltp'])
            cls.__calc_sma_gradient()
            cls.__calc_sma_kairi()
        else:
            print(ticker)


if __name__ == '__main__':
    LineNotification.initialize()
    TickData.initialize()
    while True:
        time.sleep(1)
        print(TickData.get_exec_ws_status())
        #if omd is not None:
            #print('dt={},ut={},open={},high={},low={},close={}'.format(omd.dt,omd.unix_time,omd.open,omd.high,omd.low,omd.close))
        #print(str(TickData.get_ltp()))
        #print(str(TickData.get_bid_price()))
        #print(str(TickData.get_ask_price()))
        #print('std 1m='+str(TickData.get_1m_std()))
        #time.sleep(1)