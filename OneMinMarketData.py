from OneMinData import OneMinData
import numpy as np
import pandas as pd
import talib as ta
import time
from datetime import datetime, timedelta, timezone


def calc_all_index_wrap(term, open, high, low, close, avep):
    omd = OneMinData()
    omd.initialize()
    omd.ema[term] = OneMinMarketData.calc_ema(term, close)
    omd.ema_ave[term] = OneMinMarketData.calc_ema(term, avep)
    omd.ema_kairi[term] = OneMinMarketData.calc_ema_kairi(close, omd.ema[term])
    omd.ema_gra[term] = OneMinMarketData.calc_ema_gra(omd.ema[term])
    omd.dema[term] = OneMinMarketData.calc_dema(term, close)
    omd.dema_ave[term] = OneMinMarketData.calc_dema(term, avep)
    omd.dema_kairi[term] = OneMinMarketData.calc_dema_kairi(close, omd.dema[term])
    omd.dema_gra[term] = OneMinMarketData.calc_dema_gra(omd.dema[term])
    omd.midprice[term] = OneMinMarketData.calc_midprice(term, high, low)
    omd.momentum[term] = OneMinMarketData.calc_momentum(term, close)
    omd.momentum_ave[term] = OneMinMarketData.calc_momentum(term, avep)
    omd.rate_of_change[term] = OneMinMarketData.calc_rate_of_change(term, close)
    omd.rsi[term] = OneMinMarketData.calc_rsi(term, close)
    omd.williams_R[term] = OneMinMarketData.calc_williams_R(term, high, low, close)
    omd.beta[term] = OneMinMarketData.calc_beta(term, high, low)
    omd.tsf[term] = OneMinMarketData.calc_time_series_forecast(term, close)
    omd.correl[term] = OneMinMarketData.calc_correl(term, high, low)
    omd.linear_reg[term] = OneMinMarketData.calc_linear_reg(term, close)
    omd.linear_reg_angle[term] = OneMinMarketData.calc_linear_reg_angle(term, close)
    omd.linear_reg_intercept[term] = OneMinMarketData.calc_linear_reg_intercept(term, close)
    omd.linear_reg_slope[term] = OneMinMarketData.calc_linear_reg_slope(term, close)
    omd.stdv[term] = OneMinMarketData.calc_stdv(term, close)
    omd.var[term] = OneMinMarketData.calc_var(term, close)
    omd.linear_reg_ave[term] = OneMinMarketData.calc_linear_reg(term, avep)
    omd.linear_reg_angle_ave[term] = OneMinMarketData.calc_linear_reg_angle(term, avep)
    omd.linear_reg_intercept_ave[term] = OneMinMarketData.calc_linear_reg_intercept(term, avep)
    omd.linear_reg_slope_ave[term] = OneMinMarketData.calc_linear_reg_slope(term, avep)
    omd.stdv_ave[term] = OneMinMarketData.calc_stdv(term, avep)
    omd.var_ave[term] = OneMinMarketData.calc_var(term, avep)
    omd.adx[term] = OneMinMarketData.calc_adx(term, high, low, close)
    omd.aroon_os[term] = OneMinMarketData.calc_aroon_os(term, high, low)
    omd.cci[term] = OneMinMarketData.calc_cci(term, high, low, close)
    omd.dx[term] = OneMinMarketData.calc_dx(term, high, low, close)
    if term >= 10:
        omd.macd[term], omd.macdsignal[term], omd.macdhist[term] = OneMinMarketData.calc_macd(close,int(float(term) / 2.0),term,int(float(term) / 3.0))
        omd.macd[term] = list(omd.macd[term])
        omd.macdsignal[term] = list(omd.macdsignal[term])
        omd.macdhist[term] = list(omd.macdhist[term])
        omd.macd_ave[term], omd.macdsignal_ave[term], omd.macdhist_ave[term] = OneMinMarketData.calc_macd(avep, int(float(term) / 2.0), term, int(float(term) / 3.0))
        omd.macd_ave[term] = list(omd.macd_ave[term])
        omd.macdsignal_ave[term] = list(omd.macdsignal_ave[term])
        omd.macdhist_ave[term] = list(omd.macdhist_ave[term])
    return {term: omd}


def calc_all_index_wrap2(func, open, high, low, close, avep):
    omd = OneMinData()
    omd.initialize()
    if 'normalized_ave_true_range' in str(func):
        omd.normalized_ave_true_range = func(high, low, close)
    elif 'three_outside_updown' in str(func):
        omd.three_outside_updown = func(open, high, low, close)
    elif 'breakway' in str(func):
        omd.breakway = func(open, high, low, close)
    elif 'dark_cloud_cover' in str(func):
        omd.dark_cloud_cover = func(open, high, low, close)
    elif 'dragonfly_doji' in str(func):
        omd.dragonfly_doji = func(open, high, low, close)
    elif 'updown_sidebyside_white_lines' in str(func):
        omd.updown_sidebyside_white_lines = func(open, high, low, close)
    elif 'haramisen' in str(func):
        omd.haramisen = func(open, high, low, close)
    elif 'hikkake_pattern' in str(func):
        omd.hikkake_pattern = func(open, high, low, close)
    elif 'neck_pattern' in str(func):
        omd.neck_pattern = func(open, high, low, close)
    elif 'upsidedownside_gap_three_method' in str(func):
        omd.upsidedownside_gap_three_method = func(open, high, low, close)
    elif 'sar' in str(func):
        omd.sar = func(high, low, 0.02, 0.2)
    elif 'bop' in str(func):
        omd.bop = func(open, high, low, close)
    return {str(func): omd}


class OneMinMarketData:
    @classmethod
    def initialize_for_bot(cls, num_term, window_term):
        cls.num_term = num_term
        cls.window_term = window_term
        cls.ohlc = cls.read_from_csv('./Data/one_min_data.csv')
        cls.ohlc.del_data(num_term * 2)
        cls.__calc_all_index()

    @classmethod
    def update_for_bot(cls):
        cls.__calc_all_index()

    @classmethod
    def read_from_csv(cls, file_name):
        ohlc = OneMinData()
        ohlc.initialize()
        df = pd.read_csv(file_name)
        ohlc.dt = list(map(lambda x: datetime.strptime(str(x), '%Y-%m-%d %H:%M:%S'), list(df['dt'])))
        ohlc.unix_time = list(df['unix_time'])
        ohlc.open = list(df['open'])
        ohlc.high = list(df['high'])
        ohlc.low = list(df['low'])
        ohlc.close = list(df['close'])
        ohlc.size = list(df['size'])
        return ohlc

    @classmethod
    def __calc_all_index(cls):
        start_time = time.time()
        cls.ohlc.ave_price = cls.calc_ave_price(cls.ohlc.open, cls.ohlc.high, cls.ohlc.low, cls.ohlc.close)
        num = round(cls.num_term / cls.window_term)
        if num > 1:
            for i in range(num):
                term = cls.window_term * (i + 1)
                if term > 1:
                    cls.ohlc.ema[term] = cls.calc_ema(term, cls.ohlc.close)
                    cls.ohlc.ema_ave[term] = cls.calc_ema(term, cls.ohlc.ave_price)
                    cls.ohlc.ema_kairi[term] = cls.calc_ema_kairi(cls.ohlc.close, cls.ohlc.ema[term])
                    cls.ohlc.ema_gra[term] = cls.calc_ema_gra(cls.ohlc.ema[term])
                    cls.ohlc.dema[term] = cls.calc_dema(term, cls.ohlc.close)
                    cls.ohlc.dema_ave[term] = cls.calc_dema(term, cls.ohlc.ave_price)
                    cls.ohlc.dema_kairi[term] = cls.calc_dema_kairi(cls.ohlc.close, cls.ohlc.dema[term])
                    cls.ohlc.dema_gra[term] = cls.calc_dema_gra(cls.ohlc.dema[term])
                    cls.ohlc.midprice[term] = cls.calc_midprice(term, cls.ohlc.high, cls.ohlc.low)
                    cls.ohlc.momentum[term] = cls.calc_momentum(term, cls.ohlc.close)
                    cls.ohlc.momentum_ave[term] = cls.calc_momentum(term, cls.ohlc.ave_price)
                    cls.ohlc.rate_of_change[term] = cls.calc_rate_of_change(term, cls.ohlc.close)
                    cls.ohlc.rsi[term] = cls.calc_rsi(term, cls.ohlc.close)
                    cls.ohlc.williams_R[term] = cls.calc_williams_R(term, cls.ohlc.high, cls.ohlc.low, cls.ohlc.close)
                    cls.ohlc.beta[term] = cls.calc_beta(term, cls.ohlc.high, cls.ohlc.low)
                    cls.ohlc.tsf[term] = cls.calc_time_series_forecast(term, cls.ohlc.close)
                    cls.ohlc.correl[term] = cls.calc_correl(term, cls.ohlc.high, cls.ohlc.low)
                    cls.ohlc.linear_reg[term] = cls.calc_linear_reg(term, cls.ohlc.close)
                    cls.ohlc.linear_reg_angle[term] = cls.calc_linear_reg_angle(term, cls.ohlc.close)
                    cls.ohlc.linear_reg_intercept[term] = cls.calc_linear_reg_intercept(term, cls.ohlc.close)
                    cls.ohlc.linear_reg_slope[term] = cls.calc_linear_reg_slope(term, cls.ohlc.close)
                    cls.ohlc.stdv[term] = cls.calc_stdv(term, cls.ohlc.close)
                    cls.ohlc.var[term] = cls.calc_var(term, cls.ohlc.close)
                    cls.ohlc.linear_reg_ave[term] = cls.calc_linear_reg(term, cls.ohlc.ave_price)
                    cls.ohlc.linear_reg_angle_ave[term] = cls.calc_linear_reg_angle(term, cls.ohlc.ave_price)
                    cls.ohlc.linear_reg_intercept_ave[term] = cls.calc_linear_reg_intercept(term, cls.ohlc.ave_price)
                    cls.ohlc.linear_reg_slope_ave[term] = cls.calc_linear_reg_slope(term, cls.ohlc.ave_price)
                    cls.ohlc.stdv_ave[term] = cls.calc_stdv(term, cls.ohlc.ave_price)
                    cls.ohlc.var_ave[term] = cls.calc_var(term, cls.ohlc.ave_price)
                    cls.ohlc.adx[term] = cls.calc_adx(term, cls.ohlc.high, cls.ohlc.low, cls.ohlc.close)
                    cls.ohlc.aroon_os[term] = cls.calc_aroon_os(term, cls.ohlc.high, cls.ohlc.low)
                    cls.ohlc.cci[term] = cls.calc_cci(term, cls.ohlc.high, cls.ohlc.low, cls.ohlc.close)
                    cls.ohlc.dx[term] = cls.calc_dx(term, cls.ohlc.high, cls.ohlc.low, cls.ohlc.close)
                    if term >= 10:
                        cls.ohlc.macd[term], cls.ohlc.macdsignal[term], cls.ohlc.macdhist[term] = cls.calc_macd(cls.ohlc.close, int(float(term) / 2.0), term, int(float(term) / 3.0))
                        cls.ohlc.macd[term] = list(cls.ohlc.macd[term])
                        cls.ohlc.macdsignal[term] = list(cls.ohlc.macdsignal[term])
                        cls.ohlc.macdhist[term] = list(cls.ohlc.macdhist[term])
                        cls.ohlc.macd_ave[term], cls.ohlc.macdsignal_ave[term], cls.ohlc.macdhist_ave[term] = cls.calc_macd(cls.ohlc.ave_price, int(float(term) / 2.0), term,int(float(term) / 3.0))
                        cls.ohlc.macd_ave[term] = list(cls.ohlc.macd_ave[term])
                        cls.ohlc.macdsignal_ave[term] = list(cls.ohlc.macdsignal_ave[term])
                        cls.ohlc.macdhist_ave[term] = list(cls.ohlc.macdhist_ave[term])
        cls.ohlc.normalized_ave_true_range = cls.calc_normalized_ave_true_range(cls.ohlc.high, cls.ohlc.low,cls.ohlc.close)
        cls.ohlc.three_outside_updown = cls.calc_three_outside_updown(cls.ohlc.open, cls.ohlc.high, cls.ohlc.low,cls.ohlc.close)
        cls.ohlc.breakway = cls.calc_breakway(cls.ohlc.open, cls.ohlc.high, cls.ohlc.low, cls.ohlc.close)
        cls.ohlc.dark_cloud_cover = cls.calc_dark_cloud_cover(cls.ohlc.open, cls.ohlc.high, cls.ohlc.low,cls.ohlc.close)
        cls.ohlc.dragonfly_doji = cls.calc_dragonfly_doji(cls.ohlc.open, cls.ohlc.high, cls.ohlc.low, cls.ohlc.close)
        cls.ohlc.updown_sidebyside_white_lines = cls.calc_updown_sidebyside_white_lines(cls.ohlc.open, cls.ohlc.high,cls.ohlc.low, cls.ohlc.close)
        cls.ohlc.haramisen = cls.calc_haramisen(cls.ohlc.open, cls.ohlc.high, cls.ohlc.low, cls.ohlc.close)
        cls.ohlc.hikkake_pattern = cls.calc_hikkake_pattern(cls.ohlc.open, cls.ohlc.high, cls.ohlc.low, cls.ohlc.close)
        cls.ohlc.neck_pattern = cls.calc_neck_pattern(cls.ohlc.open, cls.ohlc.high, cls.ohlc.low, cls.ohlc.close)
        cls.ohlc.upsidedownside_gap_three_method = cls.calc_upsidedownside_gap_three_method(cls.ohlc.open,cls.ohlc.high, cls.ohlc.low,cls.ohlc.close)
        cls.ohlc.sar = cls.calc_sar(cls.ohlc.high, cls.ohlc.low, 0.02, 0.2)
        cls.ohlc.bop = cls.calc_bop(cls.ohlc.open, cls.ohlc.high, cls.ohlc.low, cls.ohlc.close)


    @classmethod
    def generate_raw_df(cls):
        def __change_dict_key(d, col_name):
            newd = dict(map(lambda k: (col_name + str(k), d[k][:]), d.keys()))
            return newd

        data_dict = {'dt': cls.ohlc.dt[:], 'open': cls.ohlc.open[:], 'high': cls.ohlc.high[:], 'low': cls.ohlc.low[:],
                     'close': cls.ohlc.close[:], 'size': cls.ohlc.size[:],
                     'normalized_ave_true_range': cls.ohlc.normalized_ave_true_range[:],
                     'three_outside_updown': cls.ohlc.three_outside_updown[:], 'breakway': cls.ohlc.breakway[:],
                     'dark_cloud_cover': cls.ohlc.dark_cloud_cover[:],
                     'dragonfly_doji': cls.ohlc.dragonfly_doji[:],
                     'three_oupdown_sidebyside_white_linesutside_updown': cls.ohlc.updown_sidebyside_white_lines[:],
                     'haramisen': cls.ohlc.haramisen[:], 'haramhikkake_patternisen': cls.ohlc.hikkake_pattern[:],
                     'neck_pattern': cls.ohlc.neck_pattern[:],
                     'upsidedownside_gap_three_method': cls.ohlc.upsidedownside_gap_three_method[:],
                     'sar': cls.ohlc.sar[:], 'bop': cls.ohlc.bop[:]}
        data_dict = {**data_dict, **__change_dict_key(cls.ohlc.ema, 'ema'),
                     **__change_dict_key(cls.ohlc.ema_ave, 'ema_ave'),
                     **__change_dict_key(cls.ohlc.ema_kairi, 'ema_kairi'),
                     **__change_dict_key(cls.ohlc.dema_kairi, 'dema_kairi'),
                     **__change_dict_key(cls.ohlc.ema_gra, 'ema_gra'), **__change_dict_key(cls.ohlc.dema, 'dema'),
                     **__change_dict_key(cls.ohlc.dema_ave, 'dema_ave'),
                     **__change_dict_key(cls.ohlc.dema_gra, 'dema_gra'),
                     **__change_dict_key(cls.ohlc.midprice, 'midprice'),
                     **__change_dict_key(cls.ohlc.momentum, 'momentum'),
                     **__change_dict_key(cls.ohlc.momentum_ave, 'momentum_ave'),
                     **__change_dict_key(cls.ohlc.rate_of_change, 'rate_of_change'),
                     **__change_dict_key(cls.ohlc.rsi, 'rsi'), **__change_dict_key(cls.ohlc.williams_R, 'williams_R'),
                     **__change_dict_key(cls.ohlc.beta, 'beta'), **__change_dict_key(cls.ohlc.tsf, 'tsf'),
                     **__change_dict_key(cls.ohlc.correl, 'correl'),
                     **__change_dict_key(cls.ohlc.linear_reg, 'linear_reg'),
                     **__change_dict_key(cls.ohlc.linear_reg_angle, 'linear_reg_angle'),
                     **__change_dict_key(cls.ohlc.linear_reg_intercept, 'linear_reg_intercept'),
                     **__change_dict_key(cls.ohlc.linear_reg_slope, 'linear_reg_slope'),
                     **__change_dict_key(cls.ohlc.stdv, 'stdv'), **__change_dict_key(cls.ohlc.var, 'var'),
                     **__change_dict_key(cls.ohlc.linear_reg_ave, 'linear_reg_ave'),
                     **__change_dict_key(cls.ohlc.linear_reg_angle_ave, 'linear_reg_angle_ave'),
                     **__change_dict_key(cls.ohlc.linear_reg_intercept_ave, 'linear_reg_intercept_ave'),
                     **__change_dict_key(cls.ohlc.linear_reg_slope_ave, 'linear_reg_slope_ave'),
                     **__change_dict_key(cls.ohlc.stdv_ave, 'stdv_ave'),
                     **__change_dict_key(cls.ohlc.var_ave, 'var_ave'), **__change_dict_key(cls.ohlc.adx, 'adx'),
                     **__change_dict_key(cls.ohlc.aroon_os, 'aroon_os'),
                     **__change_dict_key(cls.ohlc.cci, 'cci'), **__change_dict_key(cls.ohlc.dx, 'dx'),
                     **__change_dict_key(cls.ohlc.macd, 'macd'),
                     **__change_dict_key(cls.ohlc.macdsignal, 'macdsignal'),
                     **__change_dict_key(cls.ohlc.macdhist, 'macdhist'),
                     **__change_dict_key(cls.ohlc.macd_ave, 'macd_ave'),
                     **__change_dict_key(cls.ohlc.macdsignal_ave, 'macdsignal_ave'),
                     **__change_dict_key(cls.ohlc.macdhist_ave, 'macdhist_ave')}
        df = pd.DataFrame.from_dict(data_dict)
        return df

    '''
    dema, adx, macdはnum_term * 2くらいnanが発生する
    print(df.isnull().sum())
    '''
    @classmethod
    def generate_df(cls):
        def __change_dict_key(d, col_name):
            newd = dict(map(lambda k: (col_name + str(k), d[k][cut_size:end]), d.keys()))
            return newd
        start_time = time.time()
        cut_size = cls.num_term * 2
        end = len(cls.ohlc.close) - cls.future_side_period
        data_dict = {'dt': cls.ohlc.dt[cut_size:end], 'open': cls.ohlc.open[cut_size:end],
                     'high': cls.ohlc.high[cut_size:end], 'low': cls.ohlc.low[cut_size:end],
                     'close': cls.ohlc.close[cut_size:end], 'size': cls.ohlc.size[cut_size:end],
                     'normalized_ave_true_range': cls.ohlc.normalized_ave_true_range[cut_size:end],
                     'three_outside_updown': cls.ohlc.three_outside_updown[cut_size:end],
                     'breakway': cls.ohlc.breakway[cut_size:end],
                     'dark_cloud_cover': cls.ohlc.dark_cloud_cover[cut_size:end],
                     'dragonfly_doji': cls.ohlc.dragonfly_doji[cut_size:end],
                     'three_oupdown_sidebyside_white_linesutside_updown': cls.ohlc.updown_sidebyside_white_lines[cut_size:end],
                     'haramisen': cls.ohlc.haramisen[cut_size:end],
                     'haramhikkake_patternisen': cls.ohlc.hikkake_pattern[cut_size:end],
                     'neck_pattern': cls.ohlc.neck_pattern[cut_size:end],
                     'upsidedownside_gap_three_method': cls.ohlc.upsidedownside_gap_three_method[cut_size:end],
                     'sar': cls.ohlc.sar[cut_size:end], 'bop': cls.ohlc.bop[cut_size:end]}
        data_dict = {**data_dict, **__change_dict_key(cls.ohlc.ema, 'ema'),
                     **__change_dict_key(cls.ohlc.ema_ave, 'ema_ave'),
                     **__change_dict_key(cls.ohlc.ema_kairi, 'ema_kairi'),
                     **__change_dict_key(cls.ohlc.dema_kairi, 'dema_kairi'),
                     **__change_dict_key(cls.ohlc.ema_gra, 'ema_gra'), **__change_dict_key(cls.ohlc.dema, 'dema'),
                     **__change_dict_key(cls.ohlc.dema_ave, 'dema_ave'),
                     **__change_dict_key(cls.ohlc.dema_gra, 'dema_gra'),
                     **__change_dict_key(cls.ohlc.midprice, 'midprice'),
                     **__change_dict_key(cls.ohlc.momentum, 'momentum'),
                     **__change_dict_key(cls.ohlc.momentum_ave, 'momentum_ave'),
                     **__change_dict_key(cls.ohlc.rate_of_change, 'rate_of_change'),
                     **__change_dict_key(cls.ohlc.rsi, 'rsi'), **__change_dict_key(cls.ohlc.williams_R, 'williams_R'),
                     **__change_dict_key(cls.ohlc.beta, 'beta'), **__change_dict_key(cls.ohlc.tsf, 'tsf'),
                     **__change_dict_key(cls.ohlc.correl, 'correl'),
                     **__change_dict_key(cls.ohlc.linear_reg, 'linear_reg'),
                     **__change_dict_key(cls.ohlc.linear_reg_angle, 'linear_reg_angle'),
                     **__change_dict_key(cls.ohlc.linear_reg_intercept, 'linear_reg_intercept'),
                     **__change_dict_key(cls.ohlc.linear_reg_slope, 'linear_reg_slope'),
                     **__change_dict_key(cls.ohlc.stdv, 'stdv'), **__change_dict_key(cls.ohlc.var, 'var'),
                     **__change_dict_key(cls.ohlc.linear_reg_ave, 'linear_reg_ave'),
                     **__change_dict_key(cls.ohlc.linear_reg_angle_ave, 'linear_reg_angle_ave'),
                     **__change_dict_key(cls.ohlc.linear_reg_intercept_ave, 'linear_reg_intercept_ave'),
                     **__change_dict_key(cls.ohlc.linear_reg_slope_ave, 'linear_reg_slope_ave'),
                     **__change_dict_key(cls.ohlc.stdv_ave, 'stdv_ave'),
                     **__change_dict_key(cls.ohlc.var_ave, 'var_ave'), **__change_dict_key(cls.ohlc.adx, 'adx'),
                     **__change_dict_key(cls.ohlc.aroon_os, 'aroon_os'),
                     **__change_dict_key(cls.ohlc.cci, 'cci'), **__change_dict_key(cls.ohlc.dx, 'dx'),
                     **__change_dict_key(cls.ohlc.macd, 'macd'),
                     **__change_dict_key(cls.ohlc.macdsignal, 'macdsignal'),
                     **__change_dict_key(cls.ohlc.macdhist, 'macdhist'),
                     **__change_dict_key(cls.ohlc.macd_ave, 'macd_ave'),
                     **__change_dict_key(cls.ohlc.macdsignal_ave, 'macdsignal_ave'),
                     **__change_dict_key(cls.ohlc.macdhist_ave, 'macdhist_ave')}
        df = pd.DataFrame.from_dict(data_dict)
        print('time to generate df={}'.format(time.time() - start_time))
        return df

    @classmethod
    def generate_df_for_bot(cls):
        def __change_dict_key(d, col_name):
            newd = dict(map(lambda k: (col_name + str(k), d[k][-1:]), d.keys()))
            return newd
        data_dict = {'dt': cls.ohlc.dt[-1:], 'open': cls.ohlc.open[-1:], 'high': cls.ohlc.high[-1:],
                     'low': cls.ohlc.low[-1:],
                     'close': cls.ohlc.close[-1:], 'size': cls.ohlc.size[-1:],
                     'normalized_ave_true_range': cls.ohlc.normalized_ave_true_range[-1:],
                     'three_outside_updown': cls.ohlc.three_outside_updown[-1:], 'breakway': cls.ohlc.breakway[-1:],
                     'dark_cloud_cover': cls.ohlc.dark_cloud_cover[-1:],
                     'dragonfly_doji': cls.ohlc.dragonfly_doji[-1:],
                     'three_oupdown_sidebyside_white_linesutside_updown': cls.ohlc.updown_sidebyside_white_lines[-1:],
                     'haramisen': cls.ohlc.haramisen[-1:], 'haramhikkake_patternisen': cls.ohlc.hikkake_pattern[-1:],
                     'neck_pattern': cls.ohlc.neck_pattern[-1:],
                     'upsidedownside_gap_three_method': cls.ohlc.upsidedownside_gap_three_method[-1:],
                     'sar': cls.ohlc.sar[-1:], 'bop': cls.ohlc.bop[-1:]}
        data_dict = {**data_dict, **__change_dict_key(cls.ohlc.ema, 'ema'),
                     **__change_dict_key(cls.ohlc.ema_ave, 'ema_ave'),
                     **__change_dict_key(cls.ohlc.ema_kairi, 'ema_kairi'),
                     **__change_dict_key(cls.ohlc.dema_kairi, 'dema_kairi'),
                     **__change_dict_key(cls.ohlc.ema_gra, 'ema_gra'), **__change_dict_key(cls.ohlc.dema, 'dema'),
                     **__change_dict_key(cls.ohlc.dema_ave, 'dema_ave'),
                     **__change_dict_key(cls.ohlc.dema_gra, 'dema_gra'),
                     **__change_dict_key(cls.ohlc.midprice, 'midprice'),
                     **__change_dict_key(cls.ohlc.momentum, 'momentum'),
                     **__change_dict_key(cls.ohlc.momentum_ave, 'momentum_ave'),
                     **__change_dict_key(cls.ohlc.rate_of_change, 'rate_of_change'),
                     **__change_dict_key(cls.ohlc.rsi, 'rsi'), **__change_dict_key(cls.ohlc.williams_R, 'williams_R'),
                     **__change_dict_key(cls.ohlc.beta, 'beta'), **__change_dict_key(cls.ohlc.tsf, 'tsf'),
                     **__change_dict_key(cls.ohlc.correl, 'correl'),
                     **__change_dict_key(cls.ohlc.linear_reg, 'linear_reg'),
                     **__change_dict_key(cls.ohlc.linear_reg_angle, 'linear_reg_angle'),
                     **__change_dict_key(cls.ohlc.linear_reg_intercept, 'linear_reg_intercept'),
                     **__change_dict_key(cls.ohlc.linear_reg_slope, 'linear_reg_slope'),
                     **__change_dict_key(cls.ohlc.stdv, 'stdv'), **__change_dict_key(cls.ohlc.var, 'var'),
                     **__change_dict_key(cls.ohlc.linear_reg_ave, 'linear_reg_ave'),
                     **__change_dict_key(cls.ohlc.linear_reg_angle_ave, 'linear_reg_angle_ave'),
                     **__change_dict_key(cls.ohlc.linear_reg_intercept_ave, 'linear_reg_intercept_ave'),
                     **__change_dict_key(cls.ohlc.linear_reg_slope_ave, 'linear_reg_slope_ave'),
                     **__change_dict_key(cls.ohlc.stdv_ave, 'stdv_ave'),
                     **__change_dict_key(cls.ohlc.var_ave, 'var_ave'), **__change_dict_key(cls.ohlc.adx, 'adx'),
                     **__change_dict_key(cls.ohlc.aroon_os, 'aroon_os'),
                     **__change_dict_key(cls.ohlc.cci, 'cci'), **__change_dict_key(cls.ohlc.dx, 'dx'),
                     **__change_dict_key(cls.ohlc.macd, 'macd'),
                     **__change_dict_key(cls.ohlc.macdsignal, 'macdsignal'),
                     **__change_dict_key(cls.ohlc.macdhist, 'macdhist'),
                     **__change_dict_key(cls.ohlc.macd_ave, 'macd_ave'),
                     **__change_dict_key(cls.ohlc.macdsignal_ave, 'macdsignal_ave'),
                     **__change_dict_key(cls.ohlc.macdhist_ave, 'macdhist_ave')}
        df = pd.DataFrame.from_dict(data_dict)
        return df

    @classmethod
    def calc_pl_ls_points(cls, pl, ls, ohlc):  # both pl and ls should be plus val as abs
        buy_points = []
        sell_points = []
        for i in range(len(ohlc.close)):
            flg_buy = True
            flg_sell = True
            entry_p = ohlc.close[i]
            j = 0
            while flg_buy or flg_sell:
                if i + j < len(ohlc.close):
                    if -ls >= ohlc.close[i + j] - entry_p:  # check buy ls
                        flg_buy = False
                    if pl <= ohlc.close[i + j] - entry_p:  # check buy pl
                        buy_points.append(i)
                        flg_buy = False
                    if -ls >= entry_p - ohlc.close[i + j]:  # check sell ls
                        flg_sell = False
                    if pl <= entry_p - ohlc.close[i + j]:  # check sell pl
                        sell_points.append(i)
                        flg_sell = False
                else:
                    break
                j += 1
        return buy_points, sell_points

    @classmethod
    def calc_ave_price(cls, open, high, low, close):
        return list(ta.AVGPRICE(np.array(open, dtype='f8'), np.array(high, dtype='f8'), np.array(low, dtype='f8'),
                                np.array(close, dtype='f8')))

    @classmethod
    def calc_ema(cls, term, close):
        return list(ta.EMA(np.array(close, dtype='f8'), timeperiod=term))

    @classmethod
    def calc_ema_kairi(cls, close, ema):
        return list(map(lambda c, e: (c - e) / e, close, ema))

    @classmethod
    def calc_dema_kairi(cls, close, dema):
        return list(map(lambda c, d: (c - d) / d, close, dema))

    @classmethod
    def calc_ema_gra(cls, ema):
        return list(pd.Series(ema).diff())

    @classmethod
    def calc_dema_gra(cls, dema):
        return list(pd.Series(dema).diff())

    @classmethod
    def calc_dema(cls, term, close):
        return list(ta.DEMA(np.array(close, dtype='f8'), timeperiod=term))

    @classmethod
    def calc_adx(cls, term, high, low, close):
        return list(
            ta.ADX(np.array(high, dtype='f8'), np.array(low, dtype='f8'), np.array(close, dtype='f8'), timeperiod=term))

    @classmethod
    def calc_aroon_os(cls, term, high, low):
        return list(ta.AROONOSC(np.array(high, dtype='f8'), np.array(low, dtype='f8'), timeperiod=term))

    @classmethod
    def calc_cci(cls, term, high, low, close):
        return list(
            ta.CCI(np.array(high, dtype='f8'), np.array(low, dtype='f8'), np.array(close, dtype='f8'), timeperiod=term))

    @classmethod
    def calc_dx(cls, term, high, low, close):
        return list(
            ta.DX(np.array(high, dtype='f8'), np.array(low, dtype='f8'), np.array(close, dtype='f8'), timeperiod=term))

    @classmethod
    def calc_midprice(cls, term, high, low):
        return list(ta.MIDPRICE(np.array(high, dtype='f8'), np.array(low, dtype='f8'), timeperiod=term))

    @classmethod
    def calc_macd(cls, close, fastperiod=12, slowperiod=26, signalperiod=9):
        return ta.MACD(np.array(close, dtype='f8'), np.array(fastperiod, dtype='i8'), np.array(slowperiod, dtype='i8'),
                       np.array(signalperiod, dtype='i8'))

    @classmethod
    def calc_momentum(cls, term, close):
        return list(ta.MOM(np.array(close, dtype='f8'), timeperiod=term))

    @classmethod
    def calc_rate_of_change(cls, term, close):
        return list(ta.ROC(np.array(close, dtype='f8'), timeperiod=term))

    @classmethod
    def calc_rsi(cls, term, close):
        return list(ta.RSI(np.array(close, dtype='f8'), timeperiod=term))

    @classmethod
    def calc_williams_R(cls, term, high, low, close):
        return list(ta.WILLR(np.array(high, dtype='f8'), np.array(low, dtype='f8'), np.array(close, dtype='f8'),
                             timeperiod=term))

    @classmethod
    def calc_beta(cls, term, high, low):
        return list(ta.BETA(np.array(high, dtype='f8'), np.array(low, dtype='f8'), timeperiod=term))

    @classmethod
    def calc_time_series_forecast(cls, term, close):
        return list(ta.TSF(np.array(close, dtype='f8'), timeperiod=term))

    @classmethod
    def calc_correl(cls, term, high, low):
        return list(ta.CORREL(np.array(high, dtype='f8'), np.array(low, dtype='f8'), timeperiod=term))

    @classmethod
    def calc_linear_reg(cls, term, close):
        return list(ta.LINEARREG(np.array(close, dtype='f8'), timeperiod=term))

    @classmethod
    def calc_linear_reg_angle(cls, term, close):
        return list(ta.LINEARREG_ANGLE(np.array(close, dtype='f8'), timeperiod=term))

    @classmethod
    def calc_linear_reg_intercept(cls, term, close):
        return list(ta.LINEARREG_SLOPE(np.array(close, dtype='f8'), timeperiod=term))

    @classmethod
    def calc_linear_reg_slope(cls, term, close):
        return list(ta.LINEARREG_INTERCEPT(np.array(close, dtype='f8'), timeperiod=term))

    @classmethod
    def calc_stdv(cls, term, close):
        return list(ta.STDDEV(np.array(close, dtype='f8'), timeperiod=term, nbdev=1))

    @classmethod
    def calc_var(cls, term, close):
        return list(ta.VAR(np.array(close, dtype='f8'), timeperiod=term, nbdev=1))

    @classmethod
    def calc_normalized_ave_true_range(cls, high, low, close):
        return list(ta.NATR(np.array(high, dtype='f8'), np.array(low, dtype='f8'), np.array(close, dtype='f8')))

    @classmethod
    def calc_three_outside_updown(cls, open, high, low, close):
        return list(ta.CDL3OUTSIDE(np.array(open, dtype='f8'), np.array(high, dtype='f8'), np.array(low, dtype='f8'),
                                   np.array(close, dtype='f8')))

    @classmethod
    def calc_breakway(cls, open, high, low, close):
        return list(ta.CDLBREAKAWAY(np.array(open, dtype='f8'), np.array(high, dtype='f8'), np.array(low, dtype='f8'),
                                    np.array(close, dtype='f8')))

    @classmethod
    def calc_dark_cloud_cover(cls, open, high, low, close):
        return list(
            ta.CDLDARKCLOUDCOVER(np.array(open, dtype='f8'), np.array(high, dtype='f8'), np.array(low, dtype='f8'),
                                 np.array(close, dtype='f8'), penetration=0))

    @classmethod
    def calc_dragonfly_doji(cls, open, high, low, close):
        return list(
            ta.CDLDRAGONFLYDOJI(np.array(open, dtype='f8'), np.array(high, dtype='f8'), np.array(low, dtype='f8'),
                                np.array(close, dtype='f8')))

    @classmethod
    def calc_updown_sidebyside_white_lines(cls, open, high, low, close):
        return list(
            ta.CDLGAPSIDESIDEWHITE(np.array(open, dtype='f8'), np.array(high, dtype='f8'), np.array(low, dtype='f8'),
                                   np.array(close, dtype='f8')))

    @classmethod
    def calc_haramisen(cls, open, high, low, close):
        return list(ta.CDLHARAMI(np.array(open, dtype='f8'), np.array(high, dtype='f8'), np.array(low, dtype='f8'),
                                 np.array(close, dtype='f8')))

    @classmethod
    def calc_hikkake_pattern(cls, open, high, low, close):
        return list(ta.CDLHIKKAKEMOD(np.array(open, dtype='f8'), np.array(high, dtype='f8'), np.array(low, dtype='f8'),
                                     np.array(close, dtype='f8')))

    @classmethod
    def calc_neck_pattern(cls, open, high, low, close):
        return list(ta.CDLINNECK(np.array(open, dtype='f8'), np.array(high, dtype='f8'), np.array(low, dtype='f8'),
                                 np.array(close, dtype='f8')))

    @classmethod
    def calc_sar(cls, high, low, accelation, maximum):
        return list(ta.SAR(np.array(high, dtype='f8'), np.array(low, dtype='f8'), np.array(accelation, dtype='f8'),
                           np.array(maximum, dtype='f8')))

    @classmethod
    def calc_bop(cls, open, high, low, close):
        return list(ta.BOP(np.array(open, dtype='f8'), np.array(high, dtype='f8'), np.array(low, dtype='f8'),
                           np.array(close, dtype='f8')))

    @classmethod
    def calc_upsidedownside_gap_three_method(cls, open, high, low, close):
        return list(
            ta.CDLXSIDEGAP3METHODS(np.array(open, dtype='f8'), np.array(high, dtype='f8'), np.array(low, dtype='f8'),
                                   np.array(close, dtype='f8')))
