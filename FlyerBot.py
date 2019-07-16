from datetime import datetime
from LgbModel import LgbModel
from LogMaster import LogMaster
from Trade import Trade
from BotAccount import BotAccount
from LineNotification import LineNotification
from WebsocketMaster import TickData
from OneMinMarketData import OneMinMarketData
from SystemFlg import SystemFlg
from CryptowatchDataGetter import CryptowatchDataGetter
import time
import pytz
import math


'''
wsは結構頻繁に落ちるのでone min dataをcrytowatchからのデータで再更新するようにする。  
'''
class FlyerBot:
    def __init__(self):
        SystemFlg.initialize()
        self.ac = BotAccount()
        self.margin_rate = 120.0
        self.leverage = 4.0
        self.JST = pytz.timezone('Asia/Tokyo')
        self.last_ohlc_min = -1
        self.lgb = LgbModel()
        self.model_buy = None
        self.model_sell = None
        self.upper_kijun = 0
        self.lower_kijun = 0
        self.pl = 0
        self.ls = 0
        self.prediction = 0
        self.pred_side = ''
        self.flg_loss_cut = False #prohibit re-entry after losscut within 1m

    #cancellation shoud be used only for pt order
    def cancel_pt_order(self):
        if self.ac.pt_order_id !='':
            print('cancel order')
            status = Trade.cancel_and_wait_completion(self.ac.pt_order_id)
            if len(status) > 0:
                print('pt cancel failed, partially executed')
                oid = Trade.order(status['side'].lower(),0,status['executed_size'],'market',1)
                LogMaster.add_log('action_message - cancel pt order - cancel pt failed and partially executed. closed position.',self.prediction, self.ac)
                self.ac.initialize_pt_order()
            else:
                LogMaster.add_log('action_message - cancelled order',self.prediction, self.ac)
                self.ac.initialize_pt_order()


    def entry_limit_order(self, side, size):
            ltp = TickData.get_ltp()
            res, size, price, order_id = Trade.opt_entry_limit_order(side, size) #round(sum(exec_size), 2), ave_p, order_id
            if res == 0:
                print(side+' entry limit order has been executed.'+'price='+str(price)+', size='+str(size))
                self.ac.update_holding(side,price,size)
                #self.ac.calc_pl(side, size, price)
                self.ac.add_order_exec_price_gap(price,ltp,side)
                print('holding_side={},holding_price={},holding_size={},total_pl={},collateral={},collateral_change={},realized_pl={}'.
                      format(self.ac.holding_side,self.ac.holding_price,self.ac.holding_size,self.ac.total_pl,self.ac.collateral,self.ac.collateral_change,self.ac.realized_pl))
                LogMaster.add_log('Limit order entry has been executed. ' + ' side=' + side + ' size=' + str(size)+ ' price=' + str(price), self.prediction,self.ac)
            else:
                LogMaster.add_log('Limit order has been failed.'+' side='+side +' '+str(size),self.prediction, self.ac)
                print('entry limit order failed!')

    def entry_price_tracing_order(self, side, size):
        ltp = TickData.get_ltp()
        res, size, price, order_id = Trade.price_tracing_order(side,size)  # round(sum(exec_size), 2), ave_p, order_id
        if res == 0:
            print(side + ' entry price tracing order has been executed.' + 'price=' + str(price) + ', size=' + str(size))
            self.ac.update_holding(side, price, size)
            self.ac.add_order_exec_price_gap(price, ltp, side)
            print('holding_side={},holding_price={},holding_size={},total_pl={},collateral={},collateral_change={},realized_pl={}'.
                format(self.ac.holding_side, self.ac.holding_price, self.ac.holding_size, self.ac.total_pl,
                       self.ac.collateral, self.ac.collateral_change, self.ac.realized_pl))
            LogMaster.add_log('Price tracing order entry has been executed. ' + ' side=' + side + ' size=' + str(size) + ' price=' + str(
                    price), self.prediction, self.ac)
        else:
            LogMaster.add_log('Limit order has been failed.' + ' side=' + side + ' ' + str(size), self.prediction,self.ac)
            print('etnry price tracing order failed!')

    def entry_pt_order(self):
        side = 'buy' if self.ac.holding_side == 'sell' else 'sell'
        price = self.ac.holding_price + self.pl if self.ac.holding_side == 'buy' else self.ac.holding_price - self.pl
        res = Trade.order(side, price, self.ac.holding_size, 'limit', 1440)
        if len(res) > 10:
            self.ac.set_pt_order(res, side, self.ac.holding_size, price)
            print('pl order: side = {}, price = {}, outstanding size = {}'.format(self.ac.pt_side, self.ac.pt_price, self.ac.pt_outstanding_size))
            LogMaster.add_log('action_message - pl entry for ' + side + ' @' + str(price) + ' x' + str(self.ac.pt_outstanding_size), self.prediction,self.ac)
        else:
            LogMaster.add_log('action_message - failed pl entry!',self.prediction,self.ac)
            print('failed pl order!')

    def exit_order(self):
        if self.ac.holding_side != '':
            print('quick exit order')
            side = 'buy' if self.ac.holding_side == 'sell' else 'sell'
            ltp = TickData.get_ltp()
            res, size, price, order_id = Trade.price_tracing_order(side, self.ac.holding_size) #return 0, round(sum(exec_size), 2), ave_p, order_id
            if res == 0:
                print(side+' exit price tracing order has been executed.' + 'price=' + str(price) + ', size=' + str(size))
                self.ac.update_holding(side, price, size)
                self.ac.calc_pl(side, size, price)
                self.ac.add_order_exec_price_gap(price, ltp, side)
                LogMaster.add_log('exit order completed!', self.prediction, self.ac)
                return 0
            else:
                print(side + ' exit price tracing order has been failed.' + 'price=' + str(price) + ', size=' + str(size))
                self.ac.update_holding(side, price, size)
                self.ac.calc_pl(side, size, price)
                self.ac.add_order_exec_price_gap(price, ltp, side)
                LogMaster.add_log('exit order failed!', self.prediction, self.ac)
                return -1

    def check_and_do_lc(self):
        if self.ac.holding_side != '':
            if self.ac.holding_side == 'buy' and TickData.get_ltp() - self.ac.holding_price <= -self.ls:
                print('hit loss cut kijun')
                LogMaster.add_log('hit loss cut kijun',self.prediction, self.ac)
                LineNotification.send_error('hit loss cut kijun')
                self.flg_loss_cut = True
                self.exit_order()
                self.cancel_pt_order()
            elif self.ac.holding_side == 'sell' and self.ac.holding_price - TickData.get_ltp() <= -self.ls:
                print('hit loss cut kijun')
                LogMaster.add_log('hit loss cut kijun',self.prediction, self.ac)
                LineNotification.send_error('hit loss cut kijun')
                self.flg_loss_cut = True
                self.exit_order()
                self.cancel_pt_order()


    def start_flyer_bot(self, num_term, window_term, pl, ls, upper_kijun, lower_kijun):
        self.__bot_initializer(num_term, window_term, pl, ls, upper_kijun, lower_kijun)
        self.start_time = time.time()
        self.fixed_order_size = 0.1
        while SystemFlg.get_system_flg():
            self.__check_system_maintenance(num_term, window_term)
            self.__update_ohlc()
            if self.ac.holding_side == '' and self.ac.pt_side == '' and self.flg_loss_cut == False: #no position no pt order
                if self.prediction == 1 or self.prediction == -1:
                    #self.entry_limit_order(self.pred_side, self.fixed_order_size)
                    self.entry_price_tracing_order(self.pred_side, self.fixed_order_size)
            elif self.ac.holding_side != '' and self.ac.pt_side == '' and self.flg_loss_cut == False: #holding position and no order
                self.entry_pt_order()
            elif (self.ac.holding_side == 'buy' and self.prediction == 2) or (self.ac.holding_side == 'sell' and self.prediction == 1):  # ポジションが判定と逆の時にexit,　もしplがあればキャンセル。。
                self.cancel_pt_order()
                self.exit_order()
                self.entry_price_tracing_order(self.pred_side, self.fixed_order_size)
                #self.entry_limit_order(self.pred_side, self.fixed_order_size)
            elif self.ac.holding_side == '' and self.ac.pt_side != '': #unexpected situation, no holding position but pt order exist
                print('no position but pt order exist!')
                LogMaster.add_log('no position but pt order exist!',self.prediction, self.ac)
                LineNotification.send_error('no position but pt order exist!')
                self.cancel_pt_order()
            self.check_and_do_lc() #check and do loss cut
            if Trade.flg_api_limit:
                time.sleep(60)
                print('Bot sleeping for 60sec due to API access limitation')
            else:
                time.sleep(0.1)


    def __bot_initializer(self, num_term, window_term, pl, ls, upper_kijun, lower_kijun):
        Trade.cancel_all_orders()
        self.num_term = num_term
        self.window_term = window_term
        self.pl = pl
        self.ls = ls
        self.upper_kijun = upper_kijun
        self.lower_kijun = lower_kijun
        print('bot - updating crypto data..')
        LogMaster.add_log('action_message - bot - updating crypto data..', 0, self.ac)
        CryptowatchDataGetter.get_and_add_to_csv()
        self.last_ohlc_min = datetime.now(self.JST).minute-1
        print('bot - initializing MarketData..')
        OneMinMarketData.initialize_for_bot(num_term, window_term)
        print('bot - loading lgb model.')
        self.model_buy, self.model_sell = self.lgb.load_model()
        print('bot - started bot loop.')
        LogMaster.add_log('action_message - bot - started bot loop.', self.prediction, self.ac)

    def __check_system_maintenance(self, num_term, window_term):
        if (datetime.now(tz=self.JST).hour == 3 and datetime.now(tz=self.JST).minute >= 59):
            print('sleep waiting for system maintenance')
            #TickData.stop(30,30)
            LineNotification.send_error('sleep waiting for system maintenance')
            if self.ac.order_side != '':
                self.cancel_order()
            time.sleep(720)  # wait for daily system maintenace
            TickData.initialize()
            CryptowatchDataGetter.get_and_add_to_csv()
            OneMinMarketData.initialize_for_bot(num_term, window_term)
            LineNotification.send_error('resumed from maintenance time sleep')
            print('resumed from maintenance time sleep')

    '''
    wsのone min dataはwsが落ちた時など正確性に欠けるので、cryptowatchのデータを定期的に取得する。
    '''
    def __overwrite_one_min_data(self, num_term, window_term):
        CryptowatchDataGetter.get_and_add_to_csv()
        OneMinMarketData.initialize_for_bot(num_term, window_term)


    def __update_ohlc(self): #should download ohlc soon after when it finalized
        if self.last_ohlc_min < datetime.now(self.JST).minute or (self.last_ohlc_min == 59 and datetime.now(self.JST).minute == 0):
            flg = True
            self.flg_loss_cut = False
            num_conti = 0
            num_max_trial = 10
            time.sleep(1)
            if TickData.ws_down_flg==False:
                omd = TickData.get_ohlc()
                if omd is not None:
                    for i in range(len(omd.unix_time)):
                        if OneMinMarketData.ohlc.unix_time[-1] < omd.unix_time[i]:
                            OneMinMarketData.ohlc.add_and_pop(omd.unix_time[i], omd.dt[i], omd.open[i], omd.high[i],omd.low[i], omd.close[i], omd.size[i])
                            flg = False
                    LogMaster.add_log('updated ws ohlc at ' + str(datetime.now(self.JST)), self.prediction, self.ac)
                else:
                    while flg:
                        res, omd = CryptowatchDataGetter.get_data_after_specific_ut(OneMinMarketData.ohlc.unix_time[-1])
                        if res == 0 and omd is not None:
                            if len(omd.unix_time) > 0:
                                omd.cut_data(len(omd.unix_time)) #remove first data to exclude after ut
                                for i in range(len(omd.unix_time)):
                                    if OneMinMarketData.ohlc.unix_time[-1] < omd.unix_time[i]:
                                        OneMinMarketData.ohlc.add_and_pop(omd.unix_time[i], omd.dt[i], omd.open[i], omd.high[i], omd.low[i],omd.close[i], omd.size[i])
                                        flg = False
                                if flg == False:
                                    print('data download dt='+str(datetime.now(self.JST)))
                                    LogMaster.add_log('updated ohlc at '+str(datetime.now(self.JST)), self.prediction, self.ac)
                        num_conti += 1
                        time.sleep(1)
                        if num_conti >= num_max_trial:
                            print('data can not be downloaded from cryptowatch.')
                            LogMaster.add_log('ohlc download error', self.prediction, self.ac)
                            break
            else:
                TickData.ws_down_flg = False
                self.__overwrite_one_min_data(self.num_term, self.window_term)
                LogMaster.add_log('overwrite one min data', self.prediction, self.ac)
                print('overwrite one min data')
                LineNotification.send_error('overwrite one min data')
            if flg == False:
                self.last_ohlc_min = datetime.now(self.JST).minute
                OneMinMarketData.update_for_bot()
                df = OneMinMarketData.generate_df_for_bot()
                pred_x = self.lgb.generate_bsp_data_for_bot(df)
                self.prediction = self.lgb.prediction(self.model_buy, self.model_sell, pred_x, self.upper_kijun, self.lower_kijun)
                print('prediction=',self.prediction)
                self.pred_side = {0: 'no', 1: 'buy', -1: 'sell'}[self.prediction]
            #self.ac.sync_position_order()
            self.ac.calc_holding_pnl()
            print('dt={}, open={},high={},low={},close={}'.format(OneMinMarketData.ohlc.dt[-1],
                                                                  OneMinMarketData.ohlc.open[-1],
                                                                  OneMinMarketData.ohlc.high[-1],
                                                                  OneMinMarketData.ohlc.low[-1],
                                                                  OneMinMarketData.ohlc.close[-1]))
            print('total_pl={}, pl per min={}, collateral change={},num_trade={},win_rate={},prediction={},holding_side={},holding_price={},holding_size={},ltp={}'.
                  format(self.ac.total_pl,self.ac.total_pl_per_min,self.ac.collateral_change,self.ac.num_trade,self.ac.win_rate,self.prediction,self.ac.holding_side,self.ac.holding_price,self.ac.holding_size, TickData.get_ltp()))
            print('private access per 300sec={}'.format(Trade.total_access_per_300s))
            LogMaster.add_log('private access per 300sec = '+str(Trade.total_access_per_300s), self.prediction, self.ac)
            LineNotification.send_notification(LogMaster.get_latest_performance())
            while datetime.now().second >= 59:
                time.sleep(0.1)



if __name__ == '__main__':
    SystemFlg.initialize()
    LineNotification.initialize()
    TickData.initialize()
    LogMaster.initialize()
    Trade.initialize()
    fb = FlyerBot()
    fb.start_flyer_bot(50,10,6000,1000,0.3,0.01) #num_term, window_term, pl, ls, upper_kijun, lower_kijun)
    #'JRF20190526-142616-930215'
    #JRF20190526-143431-187560
