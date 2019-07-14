import ccxt
import time
import threading
from LogMaster import LogMaster
from datetime import datetime
from LineNotification import LineNotification
from WebsocketMaster import TickData
from SystemFlg import SystemFlg

'''
Private API の呼出は 5 分間で 500 回を上限とします。上限に達すると呼出を一定時間ブロックします。また、ブロックの解除後も呼出の上限を一定時間引き下げます。
同一 IP アドレスからの API の呼出は 5 分間で 500 回を上限とします。上限に達すると呼出を一定時間ブロックします。また、ブロックの解除後も呼出の上限を一定時間引き下げます。
https://web.archive.org/web/20181001014248/https://fwww.me/2018/09/30/bitflyer-informal-api/
'''


class Trade:
    @classmethod
    def initialize(cls):
        cls.secret_key = ''
        cls.api_key = ''
        cls.__read_keys()
        cls.bf = ccxt.bitflyer({
            'apiKey': cls.api_key,
            'secret': cls.secret_key,
        })

        cls.order_id = {}
        cls.spread_loss_profit = 0
        cls.spread_loss_profit_log = []
        cls.num_private_access = 0
        cls.num_public_access = 0
        cls.flg_api_limit = False
        cls.conti_order_error = 0
        cls.adjusting_sleep = 0
        cls.total_access_per_300s = 0
        th = threading.Thread(target=cls.monitor_api)
        th.start()

    @classmethod
    def __read_keys(cls):
        file = open('./ignore/ex.txt', 'r')  # 読み込みモードでオープン
        cls.secret_key = file.readline().split(':')[1]
        cls.secret_key = cls.secret_key[:len(cls.secret_key) - 1]
        cls.api_key = file.readline().split(':')[1]
        cls.api_key = cls.api_key[:len(cls.api_key) - 1]
        file.close()

    @classmethod
    def monitor_api(cls):
        pre_private_access = 0
        pre_public_access = 0
        cls.access_log = []
        cls.total_access_per_300s = 0
        while SystemFlg.get_system_flg():
            cls.access_log.append(cls.num_private_access + cls.num_public_access - pre_private_access - pre_public_access)
            pre_private_access =cls.num_private_access
            pre_public_access = cls.num_public_access
            if len(cls.access_log) >= 300:
                cls.total_access_per_300s = sum(cls.access_log[-300:])
                cls.access_log.pop(0)
            time.sleep(1)

    @classmethod
    def check_exception(cls, exc):
        if 'Connection reset by peer' in str(exc):
            print('detected connection reset by peer error!')
            print('initialize trade class.')
            LineNotification.send_error('detected connection reset by peer error!')
            LogMaster.add_log('api_error - detected connection reset by peer error!',0,None)
            cls.initialize()
            time.sleep(10)
            return 'error'
        if 'Over API limit per minute' in str(exc):
            print('API private access reached limitation!')
            print('initialize trade class and sleep 60sec.')
            LineNotification.send_error('API private access reached limitation!')
            LogMaster.add_log('api_error - API private access reached limitation!',0,None)
            cls.flg_api_limit = True
            time.sleep(60)
            cls.initialize()
            return 'error'
        if 'Connection aborted.' in str(exc):
            print('Connection aborted error occurred!')
            print('initialize trade class and sleep 5sec.')
            LineNotification.send_error('Connection aborted.')
            LogMaster.add_log('api_error - Connection aborted error occurred!',0,None)
            cls.initialize()
            time.sleep(5)
            return 'error'
        return 'ok'


    '''
    when margin is not sufficient - {"status":-205,"error_message":"Margin amount is insufficient for this order.","data":null}
    '''
    @classmethod
    def order(cls, side, price, size, type, expire_m) -> str:  # min size is 0.01
        if cls.flg_api_limit == False:
            order_id = ''
            if size >= 0.01:
                try:
                    if type == 'limit':
                        cls.num_private_access += 1
                        order_id = cls.bf.create_order(
                            symbol='BTC/JPY',
                            type='limit',
                            side=side,
                            price=price,
                            amount=size,
                            params={'product_code': 'FX_BTC_JPY', 'minute_to_expire': expire_m}  # 期限切れまでの時間（分）（省略した場合は30日）
                        )
                    elif type == 'market':
                        cls.num_private_access += 1
                        order_id = cls.bf.create_order(
                            symbol='BTC/JPY',
                            type='market',
                            side=side,
                            amount=size,
                            params={'product_code': 'FX_BTC_JPY'}
                        )
                    else:
                        print('Trade - order: invalid order type!')
                        LogMaster.add_log('api_error - Trade - order: invalid order type!',0,None)
                        LineNotification.send_error('Trade - order: invalid order type!')
                        return ''
                except Exception as e:
                    if 'Margin amount is insufficient' in str(e):
                        size -= 0.01
                        size = round(size,2)
                        if size >= 0.01:
                            print('margin amount is insufficient! - decrease order size to '+str(size))
                            return cls.order(side,price,size, type, expire_m)
                        else:
                            print('margin amount can not be less than 0.01! - decrease order size to 0.01')
                            LogMaster.add_log('api_error - Trade - order:margin amount can not be less than 0.01!',0,None)
                            LineNotification.send_error('Trade - order:margin amount can not be less than 0.01!')
                            return ''
                    elif 'Market state is closed.' in str(e):
                        print(str(datetime.now())+': market state is closed.')
                        time.sleep(10)
                        return cls.order(side, price, size, type, expire_m)
                    else:
                        print(e)
                        LogMaster.add_log('action_message - Trade-order error! ' + str(e),0,None)
                        cls.conti_order_error += 1
                        if cls.check_exception(e) == 'ok':
                            if cls.conti_order_error > 15:
                                SystemFlg.set_system_flg(False)
                                LogMaster.add_log('api_error - continuous order error more than 15times System Finished.',0,None)
                                print('continuous order error more than 15times System Finished.')
                            return ''
                        else: #connection reset by peer error
                            return ''
                order_id = order_id['info']['child_order_acceptance_id']
                cls.conti_order_error = 0
                print('ok order - ' + str(order_id))
                return order_id
            else: # in case order size is smaller than 0.01
                if size >0:
                    positions = Trade.get_positions()
                    ide = ''
                    size = 0
                    price = 0
                    for s in positions:
                        side = s['side'].lower()
                        size += float(s['size'])
                        price += float(s['price']) * float(s['size'])
                    price = round(price / size)
                    order_id = cls.bf.create_order(
                        symbol='BTC/JPY',
                        type='market',
                        side='buy' if side =='sell' else 'sell',
                        amount=size,
                        params={'product_code': 'FX_BTC_JPY'}
                    )
                    print('order size ' + str(size) + ' is too small. minimum order size is 0.01!')
                    LineNotification.send_error('order size ' + str(size) + ' is too small. minimum order size is 0.01!')
                    LogMaster.add_log('order size ' + str(size) + ' is too small. minimum order size is 0.01!',0,None)
                    return order_id['info']['child_order_acceptance_id']
                else:
                    print('order size =0 is not valid!')
                    LineNotification.send_error('order size =0 is not valid!')
                    return ''
        else:
            print('order is temporary exhibited due to API access limitation!')
            return ''


    '''
        {'id': 0, 'child_order_id': 'JFX20190218-133228-026751F', 'product_code': 'FX_BTC_JPY', 'side': 'BUY', 'child_order_type': 'LIMIT', 'price': 300000.0, 'average_price': 0.0, 'size': 0.01, 'child_order_state': 'ACTIVE', 'expire_date': '2019-03-20T13:32:16', 'child_order_date': '2019-02-18T13:32:16', 'child_order_acceptance_id': 'JRF20190218-133216-339861', 'outstanding_size': 0.01, 'cancel_size': 0.0, 'executed_size': 0.0, 'total_commission': 0.0}
    {'id': 729015336, 'child_order_id': 'JFX20181130-101920-984655F', 'product_code': 'FX_BTC_JPY', 'side': 'SELL', 'child_order_type': 'MARKET', 'price': 0.0, 'average_price': 459261.0, 'size': 0.2, 'child_order_state': 'COMPLETED', 'expire_date': '2019-11-30T10:19:20.167', 'child_order_date': '2018-11-30T10:19:20.167', 'child_order_acceptance_id': 'JUL20181130-101920-024232', 'outstanding_size': 0.0, 'cancel_size': 0.0, 'executed_size': 0.2, 'total_commission': 0.0}
    {'id': 727994097, 'child_order_id': 'JFX20181130-035459-398879F', 'product_code': 'FX_BTC_JPY', 'side': 'BUY', 'child_order_type': 'LIMIT', 'price': 484534.0, 'average_price': 484351.0, 'size': 0.2, 'child_order_state': 'COMPLETED', 'expire_date': '2018-12-30T03:54:59', 'child_order_date': '2018-11-30T03:54:59', 'child_order_acceptance_id': 'JRF20181130-035459-218762', 'outstanding_size': 0.0, 'cancel_size': 0.0, 'executed_size': 0.2, 'total_commission': 0.0}
    [{'id': 1151189020, 'child_order_id': 'JFX20190422-121505-060051F', 'product_code': 'FX_BTC_JPY', 'side': 'BUY', 'child_order_type': 'LIMIT', 'price': 601306.0, 'average_price': 601306.0, 'size': 0.06, 'child_order_state': 'CANCELED', 'expire_date': '2019-04-22T13:55:05', 'child_order_date': '2019-04-22T12:15:05', 'child_order_acceptance_id': 'JRF20190422-121505-247049', 'outstanding_size': 0.0, 'cancel_size': 0.02, 'executed_size': 0.04, 'total_commission': 0.0}]
    *expired / cancelled order are not shown in the order status, return []
    '''
    @classmethod
    def get_order_status(cls, id) -> []:
        if cls.flg_api_limit == False:
            res = []
            try:
                cls.num_private_access += 1
                res = cls.bf.private_get_getchildorders(
                    params={'product_code': 'FX_BTC_JPY', 'child_order_acceptance_id': id})
            except Exception as e:
                if cls.check_exception(e) == 'ok':
                    pass
                print('error in get_order_status ' + str(e))
                LogMaster.add_log('api_error - Trade-get order status error! '+str(e), None)
                LineNotification.send_error('api_error:Trade-get order status error!'+str(e))
            finally:
                return res
        else:
            print('get_order_status is temporary exhibited due to API access limitation!')
            LogMaster.add_log('get_order_status is temporary exhibited due to API access limitation!', None)
            return None

    '''
    [{'id': 'JRF20190220-140338-069226',
    'info': {'id': 0,
    'child_order_id': 'JFX20190220-140338-309092F',
    'product_code': 'FX_BTC_JPY',
    'side': 'BUY',
    'child_order_type': 'LIMIT',
    'price': 300000.0,
    'average_price': 0.0,
    'size': 0.01,
    'child_order_state': 'ACTIVE',
    'expire_date': '2019-03-22T14:03:38',
    'child_order_date': '2019-02-20T14:03:38',
    'child_order_acceptance_id': 'JRF20190220-140338-069226',
    'outstanding_size': 0.01,
    'cancel_size': 0.0,
    'executed_size': 0.0,
    'total_commission': 0.0},
    'timestamp': 1550671418000,
    'datetime': '2019-02-20T14:03:38.000Z',
    'lastTradeTimestamp': None,
    'status': 'open',
    'symbol': 'BTC/JPY',
    'type': 'limit',
    'side': 'buy',
    'price': 300000.0,
    'cost': 0.0,
    'amount': 0.01,
    'filled': 0.0,
    'remaining': 0.01,
    'fee': {'cost': 0.0, 'currency': None, 'rate': None}},
    {'id': 'JRF20190220-140705-138578',
    'info': {'id': 0,
    'child_order_id': 'JFX20190220-140705-632784F',
    'product_code': 'FX_BTC_JPY',
    'side': 'BUY',
    'child_order_type': 'LIMIT',
    'price': 300001.0,
    'average_price': 0.0,
    'size': 0.01,
    'child_order_state': 'ACTIVE',
    'expire_date': '2019-03-22T14:07:05',
    'child_order_date': '2019-02-20T14:07:05',
    'child_order_acceptance_id': 'JRF20190220-140705-138578',
    'outstanding_size': 0.01,
    'cancel_size': 0.0,
    'executed_size': 0.0,
    'total_commission': 0.0},
    'timestamp': 1550671625000,
    'datetime': '2019-02-20T14:07:05.000Z',
    'lastTradeTimestamp': None,
    'status': 'open',
    'symbol': 'BTC/JPY',
    'type': 'limit',
    'side': 'buy',
    'price': 300001.0,
    'cost': 0.0,
    'amount': 0.01,
    'filled': 0.0,
    'remaining': 0.01,
    'fee': {'cost': 0.0, 'currency': None, 'rate': None}}]
    '''

    @classmethod
    def get_orders(cls):
        try:
            cls.num_private_access += 1
            orders = cls.bf.fetch_open_orders(symbol='BTC/JPY', params={"product_code": "FX_BTC_JPY"})
        except Exception as e:
            print('error in get_orders ' + str(e))
            LogMaster.add_log('api_error - Trade-get get_orders error! ' + str(e),0,None)
            cls.check_exception(e)
            time.sleep(3)
            return cls.get_orders()
        return orders

    '''
    [{'id': 'JRF20190301-150253-171485',
  'info': {'id': 0,
   'child_order_id': 'JFX20190301-150253-315476F',
   'product_code': 'FX_BTC_JPY',
   'side': 'BUY',
   'child_order_type': 'LIMIT',
   'price': 300000.0,
   'average_price': 0.0,
   'size': 0.01,
   'child_order_state': 'ACTIVE',
   'expire_date': '2019-03-01T15:03:53',
   'child_order_date': '2019-03-01T15:02:53',
   'child_order_acceptance_id': 'JRF20190301-150253-171485',
   'outstanding_size': 0.01,
   'cancel_size': 0.0,
   'executed_size': 0.0,
   'total_commission': 0.0},
  'timestamp': 1551452573000,
  'datetime': '2019-03-01T15:02:53.000Z',
  'lastTradeTimestamp': None,
  'status': 'open',
  'symbol': 'BTC/JPY',
  'type': 'limit',
  'side': 'buy',
  'price': 300000.0,
  'cost': 0.0,
  'amount': 0.01,
  'filled': 0.0,
  'remaining': 0.01,
  'fee': {'cost': 0.0, 'currency': None, 'rate': None}}]
    '''

    @classmethod
    def get_order(cls, order_id):
        try:
            cls.num_private_access += 1
            order = cls.bf.fetch_open_orders(symbol='BTC/JPY', params={"product_code": "FX_BTC_JPY", 'child_order_acceptance_id': order_id})
        except Exception as e:
            print('error in get_order ' + str(e))
            LogMaster.add_log('api_error - Trade-get get_order error! ' + str(e), None)
            LineNotification.send_error('api_error - Trade-get get_order error! ' + str(e))
            if cls.check_exception(e) == 'ok':
                pass
            else:
                return cls.get_order(order_id)
        return order

    '''
    [{'product_code': 'FX_BTC_JPY',
    'side': 'BUY',
    'price': 434500.0,
    'size': 0.01,
    'commission': 0.0,
    'swap_point_accumulate': 0.0,
    'require_collateral': 289.6666666666667,
    'open_date': '2019-02-20T14:28:43.447',
    'leverage': 15.0,
    'pnl': -0.3,
    'sfd': 0.0}]
    '''
    @classmethod
    def get_positions(cls):  # None
        try:
            cls.num_private_access += 1
            positions = cls.bf.private_get_getpositions(params={"product_code": "FX_BTC_JPY"})
        except Exception as e:
            print('error in get_positions ' + str(e))
            LogMaster.add_log('api_error - Trade-get get_positions error! ' + str(e),0,None)
            LineNotification.send_error('api_error - Trade-get get_positions error! ' + str(e))
            if cls.check_exception(e) == 'ok':
                pass
            else:
                return cls.get_positions()
        return positions


    '''
    [{'id': 1146943205, 'side': 'SELL', 'price': 1256500.0, 'size': 3.0, 'exec_date': '2019-07-07T08:39:51.52', 
    'child_order_id': 'JFX20190707-083908-462964F', 'commission': 0.0, 'child_order_acceptance_id': 'JRF20190707-083908-190050'}, 
    {'id': 1146939794, 'side': 'SELL', 'price': 1253207.0, 'size': 0.33855978, 'exec_date': '2019-07-07T08:36:51.67', 
    'child_order_id': 'JFX20190707-083647-354400F', 'commission': 0.0, 'child_order_acceptance_id': 'JRF20190707-083647-609507'}, 
    {'id': 1146939793, 'side': 'SELL', 'price': 1253207.0, 'size': 0.08, 'exec_date': '2019-07-07T08:36:51.34', 
    'child_order_id': 'JFX20190707-083647-354400F', 'commission': 0.0, 'child_order_acceptance_id': 'JRF20190707-083647-609507'}, 
    '''
    @classmethod
    def get_executions(cls):
        cls.num_private_access += 1
        executions = ''
        try:
            executions = cls.bf.private_get_getexecutions(params={"product_code": "FX_BTC_JPY"})
        except Exception as e:
            print('error in get_executions ' + str(e))
            LogMaster.add_log('api_error - Trade-get get_executions error! ' + str(e), None)
            LineNotification.send_error('api_error - Trade get_executions error! ' + str(e))
            if cls.check_exception(e) == 'ok':
                pass
            else:
                return cls.get_positions()
        return executions


    @classmethod
    def market_order_wait_till_execution2(cls, side, total_size):
        size = []
        price = []
        wait_sec = 0.3
        max_wait = 2.1
        num = 0

        try:
            cls.num_private_access += 1
            order_id = cls.bf.create_order(
                symbol='BTC/JPY',
                type='market',
                side=side,
                amount=total_size,
                params={'product_code': 'FX_BTC_JPY'}
            )
        except Exception as e:
            print('market order failed! ' + str(e))
            LogMaster.add_log('market order failed! ' + str(e), None)
            LineNotification.send_error( 'market order failed! ' + str(e))
            cls.check_exception(e)
            return -1, 0,0,''

        order_id = order_id['info']['child_order_acceptance_id']
        print('waiting for market order execution...')
        while sum(size) < total_size:
            executions = cls.get_executions()
            for exec in executions:
                if exec["child_order_acceptance_id"] == order_id:
                    size.append(exec["size"])
                    price.append(exec["price"])
            time.sleep(wait_sec)
            num += 1
            if num * wait_sec > max_wait:
                print('can not complete Trade - check_and_wait_till_all_execution!')
                LogMaster.add_log('can not complete Trade - check_and_wait_till_all_execution!',0,None)
                LineNotification.send_error('can not complete Trade - check_and_wait_till_all_execution!')
                return -1, sum(size), round(sum( price[i] * size[i] for i in range(len(price)) ) / sum(size)), order_id
        ave_p = round(sum( price[i] * size[i] for i in range(len(price)) ) / sum(size))
        print('market order has been successfully executed.'+ 'side='+side+', ave price='+str(ave_p)+', size='+str(round(sum(size),2)))
        return 0, round(sum(size),2), ave_p, order_id

    @classmethod
    def market_order_wait_till_execution3(cls, side, total_size):
        size = []
        price = []
        max_wait = 10
        num = 0
        bid = TickData.get_bid_price()
        ask = TickData.get_ask_price()
        try:
            cls.num_private_access += 1
            order_id = cls.bf.create_order(
                symbol='BTC/JPY',
                type='market',
                side=side,
                amount=total_size,
                params={'product_code': 'FX_BTC_JPY'}
            )
        except Exception as e:
            print('market order failed! ' + str(e))
            LogMaster.add_log('market order failed! ' + str(e), None)
            LineNotification.send_error('market order failed! ' + str(e))
            cls.check_exception(e)
            return -1, 0, 0, ''

        order_id = order_id['info']['child_order_acceptance_id']
        print('waiting for market order execution...')
        start = time.time()
        while sum(size) < total_size:
            exe_data = TickData.get_exe_data()[-100:]
            if time.time() - start > max_wait:
                exe_data = cls.get_executions()
            exe_data = TickData.get_exe_data()[-100:]
            for exec in exe_data:
                if exec[side+"_child_order_acceptance_id"] == order_id:
                    size.append(exec["size"])
                    price.append(exec["price"])
            if time.time() - start > max_wait:
                print('can not complete Trade - check_and_wait_till_all_execution!')
                LogMaster.add_log('can not complete Trade - check_and_wait_till_all_execution!', None)
                LineNotification.send_error('can not complete Trade - check_and_wait_till_all_execution!')
                return -1, sum(size), round(sum(price[i] * size[i] for i in range(len(price))) / sum(size)), order_id
            num += 1
        ave_p = round(sum(price[i] * size[i] for i in range(len(price))) / sum(size))
        print('market order has been successfully executed.' + 'side=' + side + ', ave price=' + str(ave_p) + ', size=' + str(round(sum(size), 2)))
        sp = ask - ave_p if side == 'buy' else ave_p - bid
        print('market order spread loss/profit = '+str(sp))
        cls.spread_loss_profit += sp
        cls.spread_loss_profit_log.append(sp)
        return 0, round(sum(size), 2), ave_p, order_id


    '''
    bid / ask付近に指値だして、指値からbid /askが300以上ずれたら移動(canel and reentry)させる。
    '''
    @classmethod
    def opt_price_wait_till_execution2(cls, side, total_size):
        def __check_execution(order_id): #should use only 1 time for final check to avoid double count of same exec data
            exec_size = []
            exec_price = []
            executions = cls.get_executions()
            for exec in executions:
                if exec['child_order_acceptance_id'] in order_id:
                    exec_size.append(exec['size'])
                    exec_price.append(exec['price'])
            return exec_size, exec_price

        checked_exec_id = []
        def __check_execution_ws(order_id):
            exec_size = []
            exec_price = []
            exe_data = TickData.get_exe_data()[-30:]
            for exec in exe_data:
                if exec[side+'_child_order_acceptance_id'] == order_id and exec['id'] not in checked_exec_id:
                    exec_size.append(exec['size'])
                    exec_price.append(exec['price'])
                    checked_exec_id.append(exec['id'])
            return exec_size, exec_price

        def __calc_price_move():
            executions = TickData.get_exe_data()[-3:]
            change = executions[-1]['price'] - executions[0]['price']
            return change

        def __check_bid_ask_diff(side, ori_bidask, diff_kijun): #True:diff is within diff_kijun, False:diff is larger than diff_kijun
            if side == 'buy':
                ask =  TickData.get_ask_price()
                return False if ask - ori_bidask >= diff_kijun else True
            else:
                bid = TickData.get_bid_price()
                return False if ori_bidask - bid >= diff_kijun else True

        remaining_size = total_size
        order_id = ''
        exec_size = []
        exec_price = []
        s = time.time()
        while total_size > sum(exec_size) and remaining_size >= 0.01:
            exec_size  =[]
            exec_price = []
            change = __calc_price_move()
            entry_price = 0
            if side =='buy':
                if change > 0:
                    entry_price = TickData.get_ask_price()
                else:
                    entry_price = TickData.get_bid_price() + 1
            if side =='sell':
                if change > 0:
                    entry_price = TickData.get_ask_price() -1
                else:
                    entry_price = TickData.get_bid_price()
            try:
                cls.num_private_access += 1
                order_id = cls.bf.create_order(
                    symbol='BTC/JPY',
                    type='limit',
                    side=side,
                    price=entry_price,
                    amount=remaining_size,
                    params={'product_code': 'FX_BTC_JPY'}
                )
            except Exception as e:
                print('market order failed! ' + str(e))
                LogMaster.add_log('market order failed! ' + str(e), None)
                LineNotification.send_error('market order failed! ' + str(e))
                cls.check_exception(e)
                return -1, 0, 0, ''

            time.sleep(1)
            while __check_bid_ask_diff(side, entry_price, 300) == True and remaining_size >= 0.01:
                esize, eprice = __check_execution_ws(order_id)
                exec_size.extend(esize)
                exec_price.extend(eprice)
                remaining_size = round(total_size - sum(exec_size), 2)
                time.sleep(1)
            completion_status = cls.cancel_and_wait_completion(order_id) #cancel order if price diff > 300
            if len(completion_status) > 0:
                s, p = __check_execution(order_id)
                remaining_size -= s

            remaining_size = round(total_size - sum(exec_size),2)
        ave_p = round(sum(exec_price[i] * exec_size[i] for i in range(len(exec_price))) / sum(exec_size))
        print('opt price order has been successfully executed.' + 'time elapsed='+ str(time.time() - s)+'side=' + side + ', ave price=' + str(ave_p) + ', size=' + str(round(sum(exec_size), 2)))
        return 0, round(sum(exec_size), 2), ave_p, order_id



    '''
    常に一番有利なbid / askに一定のsizeのオーダーをだす。
    order sizeは0.5
    '''
    @classmethod
    def opt_price_wait_till_execution(cls, side, total_size):
        def __check_execution(order_id):
            exec_size = []
            exec_price = []
            executions = cls.get_executions()
            for exec in executions:
                if exec['child_order_acceptance_id'] in order_id:
                    exec_size.append(exec['size'])
                    exec_price.append(exec['price'])
            return exec_size, exec_price

        def __check_bid_ask_diff(side, ori_bidask, diff_kijun):
            if side == 'buy':
                ask =  TickData.get_ask_price()
                return False if ask - ori_bidask >= diff_kijun else True
            else:
                bid = TickData.get_bid_price()
                return False if ori_bidask - bid >= diff_kijun else True

        exec_size = []
        exec_price = []
        bid = TickData.get_bid_price()
        ask = TickData.get_ask_price()
        ori_bidask = ask if side == 'buy' else bid
        order_book = {}
        unit_order_size = 0.5
        num_order_split = int(total_size / unit_order_size)
        for i in range(num_order_split):
            try:
                price = TickData.get_bid_price()+1 if side =='buy' else TickData.get_ask_price()-1
                cls.num_private_access += 1
                order_id = cls.bf.create_order(
                    symbol='BTC/JPY',
                    type='limit',
                    side=side,
                    price=price,
                    amount=unit_order_size,
                    params={'product_code': 'FX_BTC_JPY', 'minute_to_expire': 1}  # 期限切れまでの時間（分）（省略した場合は30日）
                )
            except Exception as e:
                print('opt price order failed! ' + str(e))
                LogMaster.add_log('opt price order failed! ' + str(e), 0, None)
                LineNotification.send_error('opt price order failed! ' + str(e))
                cls.check_exception(e)
                return -1, 0, 0, ''
            order_id = order_id['info']['child_order_acceptance_id']
            print('waiting for opt price order execution...')
            num = 0
            ave_p = 0


        while sum(exec_size) < total_size:
            executions = cls.get_executions()
            for exec in executions:
                if exec["child_order_acceptance_id"] == order_id:
                    exec_size.append(exec["size"])
                    exec_price.append(exec["price"])
            time.sleep(loop_sec)
            num += 1
            if max_wait < loop_sec * num:
                cls.cancel_order(order_id)
                if sum(exec_size) > 0:
                    ave_p = round(sum(exec_price[i] * exec_size[i] for i in range(len(exec_price))) / sum(exec_size))
                print('opt price order has been failed.' + 'side=' + side + ', ave price=' + str(ave_p) + ', size=' + str(round(sum(exec_size), 2)))
                return -1, round(sum(exec_size), 2), ave_p, order_id
        ave_p = round(sum(exec_price[i] * exec_size[i] for i in range(len(exec_price))) / sum(exec_size))
        print('opt price order has been successfully executed.' + 'side=' + side + ', ave price=' + str(ave_p) + ', size=' + str(round(sum(exec_size), 2)))
        return 0, round(sum(exec_size), 2), ave_p, order_id


    '''
    place a limit order and cancel when sec = 50
    '''
    @classmethod
    def opt_entry_limit_order(cls, side, total_size):
        def __check_execution(order_id):
            exec_size = []
            exec_price = []
            executions = cls.get_executions()
            for exec in executions:
                if exec['child_order_acceptance_id'] in order_id:
                    exec_size.append(exec['size'])
                    exec_price.append(exec['price'])
            return exec_size, exec_price

        checked_exec_id = []
        def __check_execution_ws(order_id):
            exec_size = []
            exec_price = []
            exe_data = TickData.get_exe_data()[-30:]
            for exec in exe_data:
                if exec[side+'_child_order_acceptance_id'] == order_id and exec['id'] not in checked_exec_id:
                    exec_size.append(exec['size'])
                    exec_price.append(exec['price'])
                    checked_exec_id.append(exec['id'])
            return exec_size, exec_price

        order_id = ''
        try:
            entry_price = TickData.get_bid_price() + 1 if side == 'buy' else TickData.get_ask_price() - 1
            cls.num_private_access += 1
            order_id = cls.bf.create_order(
                symbol='BTC/JPY',
                type='limit',
                side=side,
                price=entry_price,
                amount=total_size,
                params={'product_code': 'FX_BTC_JPY', 'minute_to_expire': 1}  # 期限切れまでの時間（分）（省略した場合は30日）
            )['info']['child_order_acceptance_id']
        except Exception as e:
            print('opt entry limit order failed! ' + str(e))
            LogMaster.add_log('opt entry limit order failed! ' + str(e), 0, None)
            LineNotification.send_error('opt entry limit order failed! ' + str(e))
            cls.check_exception(e)
            return -1, 0, 0, ''

        exec_size = []
        exec_price = []
        print('oid=',order_id)
        while datetime.now().second <50 and sum(exec_size) < total_size:
            es, ep = __check_execution_ws(order_id)
            exec_size.extend(es)
            exec_price.extend(ep)
            time.sleep(0.5)
        cls.cancel_order(order_id)
        if sum(exec_size) < total_size: #final check using execution history from API
            es, ep = __check_execution(order_id)
            if sum(es) > sum(exec_size):
                exec_size = es
                exec_price = ep
        if sum(exec_size) >0:
            ave_p = round(sum(exec_price[i] * exec_size[i] for i in range(len(exec_price))) / sum(exec_size))
            print('opt entry limit order has been successfully executed.' + 'side=' + side + ', ave price=' + str(ave_p) + ', size=' + str(round(sum(exec_size), 2)))
            return round(sum(exec_size), 2), ave_p, order_id
        else:
            ave_p = 0
            print('opt entry limit order timed out.' + 'side=' + side + ', ave price=0'+', size=0')
            return 0, 0, 0, order_id


    '''
    continuously place a limit order at bid / ask.
    '''
    @classmethod
    def price_tracing_order(cls, side, total_size):
        def __check_bid_ask_diff(side, ori_bidask, diff_kijun): #True:diff is within diff_kijun, False:diff is larger than diff_kijun
            if side == 'buy':
                ask =  TickData.get_ask_price()
                return False if ask - ori_bidask >= diff_kijun else True
            else:
                bid = TickData.get_bid_price()
                return False if ori_bidask - bid >= diff_kijun else True

        checked_exec_id = []
        def __check_execution_ws(order_id):
            exec_size = []
            exec_price = []
            exe_data = TickData.get_exe_data()[-30:]
            for exec in exe_data:
                if exec[side + '_child_order_acceptance_id'] == order_id and exec['id'] not in checked_exec_id:
                    exec_size.append(exec['size'])
                    exec_price.append(exec['price'])
                    checked_exec_id.append(exec['id'])
            return exec_size, exec_price

        exec_size = []
        exec_price = []
        order_ids = []
        while sum(exec_size) < total_size:
            try:
                entry_price = TickData.get_bid_price() + 1 if side == 'buy' else TickData.get_ask_price() - 1
                cls.num_private_access += 1
                order_id = cls.bf.create_order(
                    symbol='BTC/JPY',
                    type='limit',
                    side=side,
                    price=entry_price,
                    amount=total_size,
                    params={'product_code': 'FX_BTC_JPY', 'minute_to_expire': 1}  # 期限切れまでの時間（分）（省略した場合は30日）
                )['info']['child_order_acceptance_id']
                order_ids.append(order_id)
            except Exception as e:
                print('price tracing order failed! ' + str(e))
                LogMaster.add_log('price tracing order failed! ' + str(e), 0, None)
                LineNotification.send_error('price tracing order failed! ' + str(e))
                cls.check_exception(e)
                ave_p = round(sum(exec_price[i] * exec_size[i] for i in range(len(exec_price))) / sum(exec_size))
                return -1, round(sum(exec_size), 2), ave_p, order_id
            while __check_bid_ask_diff(side, entry_price, 300) and sum(exec_size) < total_size:
                es, ep = __check_execution_ws(order_id)
                if sum(es) > 0:
                    exec_size.extend(es)
                    exec_price.extend(ep)
                time.sleep(0.1)
            cls.cancel_order(order_id)
        ave_p = round(sum(exec_price[i] * exec_size[i] for i in range(len(exec_price))) / sum(exec_size))
        print('price tracing order has been successfully executed.' + 'side=' + side + ', ave price=' + str(ave_p) + ', size=' + str(round(sum(exec_size), 2)))
        return 0, round(sum(exec_size), 2), ave_p, order_id





    @classmethod
    def cancel_order(cls, order_id):
        cancel =''
        try:
            cls.num_private_access += 1
            cancel = cls.bf.cancel_order(id=order_id, symbol='BTC/JPY', params={"product_code": "FX_BTC_JPY"})
        except Exception as e:
            print('error in cancel_order ' + str(e))
            LogMaster.add_log('api_error - Trade-get cancel_order error! ' + str(e),0,None)
            LineNotification.send_error('api_error - Trade-get cancel_order error! ' + str(e))
            cls.check_exception(e)
            if 'Order not found' in str(e):
                print('cancel order not found!')
                LogMaster.add_log('api_error - cancel order not found! ',0,None)
                LineNotification.send_error('api_error - cancel order not found! ')
                cancel = ''
        return cancel

    @classmethod
    def get_current_asset(cls):
        try:
            cls.num_private_access += 1
            res = cls.bf.fetch_balance()
        except Exception as e:
            print('error i get_current_asset ' + e)
            LogMaster.add_log('action_message - Trade-get current asset error! ' + str(e),0,None)
            LineNotification.send_error('action_message - Trade-get current asset error! ' + str(e))
            if cls.check_exception(e) == 'ok':
                pass
        finally:
            return res['total']['BTC'] * TickData.get_ltp() + res['total']['JPY']


    #{'collateral': 5094.0, 'open_position_pnl': 0.0, 'require_collateral': 0.0, 'keep_rate': 0.0}
    @classmethod
    def get_collateral(cls):
        res=''
        try:
            cls.num_private_access += 1
            res = cls.bf.fetch2(path='getcollateral', api='private', method='GET')
        except Exception as e:
            print('error i get_collateral ' + e)
            LogMaster.add_log('api_error - Trade-get get_collateral error! ' + str(e),0,None)
            LineNotification.send_error('api_error - Trade-get get_collateral error! ' + str(e))
            if cls.check_exception(e) == 'ok':
                pass
        finally:
            return res

    @classmethod
    def cancel_all_orders(cls):
        orders = cls.get_orders()
        for o in orders:
            cls.cancel_order(o['id'])

    '''
    #res['bids'][0][0] = 394027
    {'bids': [[394027.0, 0.15], [394022.0, 0.01], [394020.0, 3.22357434], [394018.0, 0.02050665], [394016.0, 0.085], [394015.0, 0.02], [394014.0, 0.025], [394013.0, 0.21195378], [394012.0, 1.67], [394011.0, 1.36], [394010.0, 0.395], [394009.0, 0.01], [394008.0, 0.021], [394007.0, 0.09018275], [394006.0, 1.4862514], [394005.0, 6.42], [394004.0, 0.79593158], [394003.0, 5.0], [394002.0, 0.34592307], [394001.0, 4.14846844], [394000.0, 173.92494563], [393999.0, 0.01], [393998.0, 0.55], [393997.0, 0.484], [393996.0,
    '''
    @classmethod
    def get_order_book(cls):
        cls.num_public_access += 1
        return cls.bf_pub.fetch_order_book(symbol='BTC/JPY', params={"product_code": "FX_BTC_JPY"})

    @classmethod
    def get_opt_price(cls):
        book = cls.get_order_book()
        bids = book['bids']
        asks = book['asks']
        bid = bids[0][0]
        ask = asks[0][0]
        return round(ask + float(ask - bid) / 2.0, 0)


    @classmethod
    def get_bid_price(cls):
        return cls.get_order_book()['bids'][0][0]

    @classmethod
    def get_ask_price(cls):
        return cls.get_order_book()['asks'][0][0]

    @classmethod
    def get_spread(cls):
        book = cls.get_order_book()
        return book['asks'][0][0] - book['bids'][0][0]

    '''
    ok orderJRF20190220-144017-685161
    waiting order execution...1 sec
    waiting order execution...2 sec
    [{'id': 967727288, 'child_order_id': 'JFX20190220-144017-948999F', 'product_code': 'FX_BTC_JPY', 'side': 'SELL', 'child_order_type': 'LIMIT', 'price': 434559.0, 'average_price': 434600.0, 'size': 0.01, 'child_order_state': 'COMPLETED', 'expire_date': '2019-03-22T14:40:17', 'child_order_date': '2019-02-20T14:40:17', 'child_order_acceptance_id': 'JRF20190220-144017-685161', 'outstanding_size': 0.0, 'cancel_size': 0.0, 'executed_size': 0.01, 'total_commission': 0.0}]
    order executed
    '''

    #should be depreciated
    @classmethod
    def order_wait_till_execution(cls, side, price, size, expire_m) -> dict:
        id = cls.order(side, price, size, 'limit', expire_m)
        i = 0
        print('waiting order execution...')
        flg_activated = False
        while True:
            status = cls.get_order_status(id)
            if len(status) > 0:
                if status[0]['child_order_state'] == 'COMPLETED':  # order executed
                    print('order has been executed')
                    return status[0]
                elif status[0]['child_order_state'] == 'ACTIVE':
                    flg_activated = True
            else:
                if flg_activated:
                    print('order has been expired')
                    return None
                i += 1
            time.sleep(0.5)


    @classmethod
    def market_order_wait_till_execution(cls, side, size) -> dict:
        id = cls.order(side, 0, size, 'market', 0)
        i = 0
        print('waiting order execution...')
        flg_activated = False
        time.sleep(1)
        while True:
            status = cls.get_order_status(id)
            if len(status) > 0:
                if status[0]['child_order_state'] == 'COMPLETED':  # order executed
                    print('order has been executed')
                    return status[0]
                elif status[0]['child_order_state'] == 'ACTIVE':
                    flg_activated = True
            else:
                if flg_activated:
                    print('order has been expired')
                    return None
            i += 1
            if i > 50:
                print('market order wait till execution - ')
                print(status[0])
                LogMaster.add_log('market order wait till execution - ', 0, None)
                LineNotification.send_error('market order wait till execution - ')
                return status[0]
            time.sleep(0.3)

    '''
    new entryしたオーダーが1秒後にもまだboardしておらず、cancel and wait orderでorder status取得できず、誤ってsuccessfully cancelledと判定されうるので、
    最初にorder statusが存在することを確認している。
    5秒経ってもorder statusが確認できない時はcancelledとして処理する。
    '''
    @classmethod
    def cancel_and_wait_completion(cls, oid) -> dict:
        status = cls.get_order_status(oid)
        if len(status) == 0:
            n = 0
            while len(status) == 0:
                time.sleep(0.2)
                n += 1
                status = cls.get_order_status(oid)
                if n > 25:
                    print('cancel_and_wait_completion -  order status is not available!')
                    return []
        cls.cancel_order(oid)
        print('waiting cancel order ' + oid)
        n = 0
        while True:  # loop for check cancel completion or execution
            status = cls.get_order_status(oid)
            if len(status) > 0:
                if (status[0]['child_order_state'] == 'COMPLETED' or status[0]['child_order_state'] == 'CANCELED') and status[0]['executed_size'] > 0:
                    print('cancel failed order has been partially executed. exe size='+str(status[0]['executed_size']))
                    return status[0]
                elif (status[0]['child_order_state'] == 'COMPLETED' or status[0]['child_order_state'] == 'CANCELED') and status[0]['executed_size'] == 0:
                    print('order has been successfully cancelled')
                    return []
            else:
                print('order has been successfully cancelled')
                return []
            n +=1
            if n > 25:
                print('5 sec passed but cancel order completion was not confirmed!')
                LineNotification.send_error('5 sec passed but cancel order completion was not confirmed!')
                return []
            time.sleep(0.2)

    @classmethod
    def order_wait_till_boarding(cls, side, price, size, expire_m) -> dict:
        oid = cls.order(side, price, size, 'limit', expire_m)
        n = 0
        while True:
            status = cls.get_order_status(oid)
            if len(status) > 0:
                if status[0]['child_order_state'] == 'ACTIVE' or status[0]['child_order_state'] == 'COMPLETED':
                    print('confirmed the order has been boarded')
                    return status[0]
            n += 1
            if n > 30:
                print('6 sec was passed but order boarding was not confirmed!')
                LineNotification.send_error('6 sec was passed but order boarding was not confirmed!')
            time.sleep(0.2)


if __name__ == '__main__':
    SystemFlg.initialize()
    TickData.initialize()
    time.sleep(5)
    LogMaster.initialize()
    Trade.initialize()
    s = time.time()
    oid = Trade.order('buy', 1000000,0.01, 'limit',1)
    print(Trade.get_orders())
    Trade.cancel_order(oid)
    time.sleep(1)
    Trade.cancel_order(oid)
    #print(Trade.price_tracing_order('sell', 0.05))
    print('done')
    #executions =Trade.get_executions()
    ##print(executions)
    #print(len(executions))
    #print(time.time() - s)
    #oid = Trade.order('sell',0,0.01,'market',0)
    #print(Trade.get_collateral())
    #test = Trade.cancel_and_wait_completion('test')
    #print(Trade.get_order_status('test'))

    '''
    Trade.initialize()
    oid = Trade.order('buy', 500000, 0.1, 1)
    print(Trade.get_order_status(oid))
    time.sleep(3)
    print(Trade.get_order_status(oid)[0])
    Trade.cancel_order(oid)
    time.sleep(5)
    print(Trade.get_order_status(oid)[0])
    '''

