import requests
import pandas as pd
import os
import time
import shutil
from datetime import datetime
from datetime import timedelta
from OneMinData import OneMinData


'''
cryptowatch return unfinalized ohlc when client send request. And also datetime is 1 minutes earlier than correct dt.
User should use this class at least after 1 sec passed every 1m and CryptowatchDataGetter remove latest data as unfinalized.
'''
class CryptowatchDataGetter:
    @classmethod
    def initialize_for_bot(cls):
        cls.num_update = 0
        cls.get_and_add_to_csv()
        shutil.copy('./Data/one_min_data.csv', './Data/bot_ohlc_data.csv')


    @classmethod
    def check_csv_data(cls):
        df = pd.read_csv('./Data/one_min_data.csv')
        dt = df['dt']
        ut = df['unix_time']
        return dt[len(dt)-1], ut[len(ut)-1]

    @classmethod
    def get_data_from_crptowatch(cls, before=0, after=0):
        url = 'https://api.cryptowat.ch/markets/bitflyer/btcfxjpy/ohlc'
        result = ''
        res = ''
        query = {
            'periods': 60,
            'before': int(before),
            'after': int(after),
        }
        try:
            res = requests.get(url, params=query)
            result = res.json()['result']['60']
        except:
            print('cryptowatch download error!'+str(res))
            print('before={},after={}'.format(query['before'], query['after']))
            print(res)
            result = ''
        return result

    #download all data after specific ut (inclusive)
    @classmethod
    def get_data_after_specific_ut(cls, target_ut):
        num_down = 0
        flg_down_success = False
        while flg_down_success == False:
            try:
                num_down += 1
                result = cls.get_data_from_crptowatch(after=int(target_ut))
                if len(result) > 0:
                    omd = cls.convert_json_to_ohlc(result)
                    if len(omd.unix_time) > 0:# and int(target_ut) in omd.unix_time:
                        flg_down_success = True
                        return 0, omd
                    else:
                        return -1, None
            except Exception as e:
                print('cryptowatch downloader - get data after specific ut: no target ut error!'+str(e))
                return -1, None
            if num_down > 5:
                print('crypto watch data download error!')
                return -1, None
            time.sleep(1)


    @classmethod
    def get_data_at_dt(cls, timestp):
        while True:
            flg = True
            ut = int(timestp // 1)
            while flg:
                res = cls.get_data_from_crptowatch()
                if res[len(res) - 1][0] == ut:
                    flg = False
                time.sleep(0.1)

    #measure diff(sec) of updating minutes ohlc data on cryptowatch
    @classmethod
    def measure_data_update_diff(cls):
        while True:
            if datetime.now().second == 0:
                flg = True
                ut = int(datetime.now().timestamp() // 1)
                while flg:
                    res = cls.get_data_from_crptowatch()
                    if res[len(res)-1][0] == ut:
                        print('@'+str(datetime.fromtimestamp(ut))+', diff='+str(datetime.now().second))
                        flg = False
                time.sleep(30)
            time.sleep(0.1)


    @classmethod
    def convert_json_to_ohlc(cls, json_data):
        omd = OneMinData()
        omd.initialize()
        for i in range(len(json_data)-1):
            omd.unix_time.append(int(json_data[i][0]//1))
            omd.dt.append(datetime.fromtimestamp(json_data[i][0]))
            omd.open.append(json_data[i][1])
            omd.high.append(json_data[i][2])
            omd.low.append(json_data[i][3])
            omd.close.append(json_data[i][4])
            omd.size.append(json_data[i][5])
        return omd

    @classmethod
    def write_data_to_csv(cls, one_min_data: OneMinData):
        df = pd.DataFrame()
        df = df.assign(unix_time=one_min_data.unix_time)
        df = df.assign(dt=one_min_data.dt)
        df = df.assign(open=one_min_data.open)
        df = df.assign(high=one_min_data.high)
        df = df.assign(low=one_min_data.low)
        df = df.assign(close=one_min_data.close)
        df = df.assign(size=one_min_data.size)
        df.to_csv('./Data/one_min_data.csv', index=False)

    @classmethod
    def read_csv_data(cls, file_name):
        df = pd.read_csv(file_name)
        return df


    @classmethod
    def get_and_add_to_csv(cls):  #about 1 sec
        if os.path.exists('./Data/one_min_data.csv'):
            dt, unix_dt = cls.check_csv_data()
            df_ori = cls.read_csv_data('./Data/one_min_data.csv')
            json_data = cls.get_data_from_crptowatch(after=unix_dt)
            omd = cls.convert_json_to_ohlc(json_data)
            from_ind = 0
            if len(omd.dt) > 0:
                for i in range(len(omd.unix_time)):
                    if omd.unix_time[i] == unix_dt:
                        from_ind = i+1
                        break
                df = pd.DataFrame()
                df = df.assign(unix_time = omd.unix_time[from_ind:])
                df = df.assign(dt=omd.dt[from_ind:])
                df = df.assign(open=omd.open[from_ind:])
                df = df.assign(high=omd.high[from_ind:])
                df = df.assign(low=omd.low[from_ind:])
                df = df.assign(close=omd.close[from_ind:])
                df = df.assign(size=omd.size[from_ind:])
                df_ori = pd.concat([df_ori, df], ignore_index=True, axis=0)
                df_ori.to_csv('./Data/one_min_data.csv', index=False)
                return df, omd
            else:
                print('no new ohlc data to the csv!')
                return pd.DataFrame(), None
        else:
            dt = datetime.now()+timedelta(minutes=-6001)
            res = cls.get_data_from_crptowatch(after=int(dt.timestamp()))
            omd = cls.convert_json_to_ohlc(res)
            cls.write_data_to_csv(omd)

    @classmethod
    def test_data_time_delay(cls):
        test = 0
        while True:
            if datetime.now().second >= 59:
                while True:
                    res,omd = cls.get_data_after_specific_ut(time.time()-60)
                    print(res)
                    if res == 0:
                        print('down ut='+str(omd.unix_time[-1]))
                        print('current ut='+str(time.time()))
                        test+=1
                        break
                    time.sleep(0.5)
            if test >=30:
                break
            time.sleep(0.2)



if __name__ == '__main__':

    start = time.time()
    CryptowatchDataGetter.get_and_add_to_csv()
    elapsed_time = time.time() - start
    print(elapsed_time)