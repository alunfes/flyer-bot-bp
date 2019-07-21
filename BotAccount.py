from Trade import Trade
from WebsocketMaster import TickData
from datetime import datetime
from SystemFlg import SystemFlg
import time
import threading
'''
botから提供されたorder idを元に約定確認をし、holding, plを同時に計算する。
order idとマッチしたexecution dataは削除。
1分経過したexecution dataは削除。

ptの約定確認：
pt価格に到達したと判断できる時に、pt order idでAPI経由で約定確認する。

'''


class ExecutionCheckingData():
    def __init__(self):
        self.execution_data = {}
        self.execution_ut = {}
        self.matched_execution_data = []
        self.execution_id = 0

    def add_executions(self, executions):
        if executions != None and len(executions) > 0:
            #check duplication of execution id
            id_list = [x['id'] for x in list(self.execution_data.values())[:]]
            for execution in executions:
                if len(executions) > 0 and execution['id'] not in id_list:
                    self.execution_data[self.execution_id] = execution
                    self.execution_ut[self.execution_id] = time.time()
                    self.execution_id += 1

    def remove_executions(self, ind):
        self.execution_data.pop(ind)
        self.execution_ut.pop(ind)
        self.execution_checked.pop(ind)


class ActiveOrderData():
    def __init__(self):
        self.active_order_ids = {}
        self.active_order_ut = {}
        self.active_order_outstanding_size = {}
        self.active_order_id = 0
        self.all_order_ids = []

    def add_active_order(self, order_id, size):
        if order_id not in self.active_order_ids.values():
            self.active_order_ids[self.active_order_id] = order_id
            self.active_order_ut[self.active_order_id] = time.time()
            self.active_order_outstanding_size[self.active_order_id] = size
            self.active_order_ids.append(order_id)
            self.active_order_id += 1

    def add_active_orders(self, order_ids, order_sizes):
        for i, order in enumerate(order_ids):
            self.add_active_order(order, order_sizes[i])

    def remove_active_order(self, key_ind):
        del self.active_order_ids[key_ind]
        del self.active_order_ut[key_ind]
        del self.active_order_outstanding_size[key_ind]


class BotAccount:
    def __init__(self):
        self.initialize_order()
        self.initialize_holding()
        self.initialize_pt_order()

        self.user_positions_dt = []
        self.user_orders_id = []

        #self.initial_collateral = Trade.get_collateral()['collateral']
        self.collateral = 0
        self.open_pnl = 0
        self.realized_pl = 0
        self.total_pl = 0 #realized_pl + open pnl
        self.total_pl_log = []
        self.total_pl_per_min = 0
        #self.collateral_change = 0 #collateral - initial_collateral
        #self.collateral_change_per_min = 0
        #self.collateral_change_log = []
        self.order_exec_price_gap = [0]
        self.num_trade = 0
        self.num_win = 0
        self.win_rate = 0
        self.elapsed_time = 0

        self.num_trade = 0
        self.num_win = 0
        self.win_rate = 0
        self.start_ut = time.time()

        self.exec_data = ExecutionCheckingData()
        self.active_order_data = ActiveOrderData()

        self.before_posi_side = ''
        self.before_posi_size = 0

        th = threading.Thread(target=self.account_thread)
        th.start()


    def initialize_order(self):
        self.executions_hist_log = []
        self.active_order_ids = []
        self.active_order_ut = []
        self.active_order_outstanding_size = []
        self.all_order_ids = []

    def initialize_pt_order(self):
        self.pt_order_id = ''
        self.pt_side = ''
        self.pt_checked_exec_id = []
        self.pt_outstanding_size = 0
        self.pt_total_size = 0
        self.pt_price = 0

    def initialize_holding(self):
        self.holding_side = ''
        self.holding_price = 0
        self.holding_size = 0
        self.holding_dt = ''
        self.holding_ut = 0

    '''
    ・3秒ごとに約定履歴を取得して未確認order idの約定を確認。約定データを元にholdingとplを更新。
    ・30秒ごとに一回API経由で約定履歴を取得して、order idを確認
    ・30秒ごとに一回API経由で注文一覧を取得して、現在active orderとしてaccountで持っている情報と整合しているかを確認する。（actie orderが1分以上経過してもorder listに載っていない場合はcancel扱い）
    ・order idが登録sれてから一定時間が経過しても約定履歴・注文一覧に載ってこない場合にcancelとして取り扱う。
    '''
    def account_thread(self):
        mid_check_flg = False #flg for 約30秒ごとに一回API経由で約定履歴・注文一覧
        i = 0
        while SystemFlg.get_system_flg():
            if i > 10:
                i = 0
                #30秒ごとに一回API経由で約定履歴を取得して、order idを確認
                self.exec_data.add_executions(Trade.get_executions())
                #30秒ごとに一回API経由で注文一覧を取得して、order id登録から1分経過したものをcancelとして処理
                self.check_order_cancellation()
            self.exec_data.add_executions(TickData.get_latest_exec_data())
            self.check_order_execution()
            i += 1
            time.sleep(3)


    def check_order_execution(self):
        for exec_key in list(self.exec_data.execution_data.keys())[:]:
            for order_key in list(self.active_order_data.active_order_ids.keys())[:]:
                if self.exec_data.execution_data[exec_key]['buy_child_order_acceptance_id'] == self.active_order_ids[order_key] or \
                        self.exec_data.execution_data[exec_key]['sell_child_order_acceptance_id'] == self.active_order_ids[order_key]:
                    self.active_order_data.active_order_outstanding_size[order_key] -=self.exec_data.execution_data[exec_key]['size']
                    if self.active_order_data.active_order_outstanding_size[order_key] < 0.001:
                        self.active_order_data.remove_active_order(order_key)
                    self.exec_data.matched_execution_data.append(self.exec_data.execution_data[exec_key])
                    self.calc_pl(self.exec_data.execution_data[exec_key])
                    self.update_holding(self.exec_data.execution_data[exec_key]['side'].lower(), self.exec_data.execution_data[exec_key]['price'], self.exec_data.execution_data[exec_key]['size'])
                    self.exec_data.remove_executions(exec_key)

    def check_order_cancellation(self):
        orders = Trade.get_orders()
        id_list = [x['info']['child_order_acceptance_id'] for x in orders]
        for key, active_id in enumerate(self.active_order_data.active_order_ids):
            if active_id not in id_list and time.time() - self.active_order_data.ut[key] >= 60:
                self.active_order_data.remove_active_order(key)


    def update_holding(self, side, price, size):
        self.holding_dt = datetime.now()
        self.holding_ut = time.time()
        if self.holding_side == '':
            self.holding_side = side
            self.holding_price = price
            self.holding_size = size
        elif self.holding_side == side:
            self.holding_price = (self.holding_price * self.holding_size + price * size) / (self.holding_size + size)
            self.holding_size += round(size,2)
        elif self.holding_side != side and abs(self.holding_size - size) < 0.0001:
            self.initialize_holding()
        elif self.holding_side != side and self.holding_size > size:
            self.holding_size -= round(size,2)
        elif self.holding_side != side and self.holding_size < size:
            self.holding_side = side
            self.holding_size = round(size - self.holding_size,2)
            self.holding_price = price


    def check_before_position_data(self):
        positions = Trade.get_positions()
        side = ''
        size = 0
        for p in positions:
            size += p['size']
        if len(positions) > 0:
            self.before_posi_side = positions[0]['side']
            self.before_posi_size = size
        else:
            self.before_posi_side = ''
            self.before_posi_size = 0

    def check_after_position_data(self, side, size):
        positions = Trade.get_positions()
        actual_side = ''
        actual_size = 0
        for p in positions:
            actual_size += p['size']
        actual_side = positions[0]['side']
        expected_after_side = ''
        expected_after_size = 0
        if self.before_posi_side == '':
            expected_after_side = side
            expected_after_size = size
        elif self.before_posi_side == side:
            expected_after_side = side
            expected_after_size = size + self.before_posi_size
        elif self.before_posi_side != side:
            if self.before_posi_size > size:
                expected_after_side = self.before_posi_side
                expected_after_size = self.before_posi_size - size
            else:
                expected_after_side = side
                expected_after_size = size - self.before_posi_size
        if actual_side == expected_after_side:
            if actual_size == expected_after_size:
                return



    def add_order(self, order_id, size):
        self.active_order_data.add_active_order(order_id, size)

    def add_orders(self,order_ids, order_sizes):
        self.active_order_data.add_active_orders(order_ids, order_sizes)

    def set_pt_order(self, order_id, side ,total_size, price):
        self.initialize_pt_order()
        self.pt_order_id = order_id
        self.pt_side = side
        self.pt_total_size = total_size
        self.pt_price = price
        self.pt_outstanding_size = total_size




    '''
    通常はwsでチェックして、ohlc_updateの前にAPI経由のexecutionデータでかくにんする。
    '''
    def check_pt_order_exeution(self):
        executions = None
        if datetime.now().second >=57:
            executions = Trade.get_executions()
        else:
            executions = TickData.get_exe_data()[-30:]
        if self.pt_order_id != '':
            for i in range(len(executions)):
                if executions[i]['child_order_acceptance_id'] == self.pt_order_id and executions[i]['id'] not in self.pt_checked_exec_id:
                    side = executions[i]['side'].ilower()
                    price = executions[i]['price']
                    size = executions[i]['size']
                    self.pt_checked_exec_id.append(executions[i]['id'])
                    self.executions_hist_log.append(executions[i])
                    self.calc_pl(executions[i])
                    self.update_holding(side, price, size)
                    self.pt_outstanding_size -= size
                    if self.pt_outstanding_size <= 0:
                        print('pt order has been fully executed!')
                        self.initialize_pt_order()


    def calc_pl(self, side, size, price):
        if side != self.holding_side and self.holding_side != '':
            pl = (price - self.holding_price) * size if side =='buy' else (self.holding_price - price) * size
            self.realized_pl += pl
            self.realized_pl = int(self.realized_pl)
            self.num_trade += 1
            if pl > 0:
                self.num_win += 1
        self.calc_holding_pnl()
        self.total_pl = self.realized_pl + self.open_pnl
        self.total_pl_log.append(self.total_pl)
        self.__calc_win_rate()
        self.__calc_pl_per_min()

    def calc_holding_pnl(self):
        if self.holding_side != '':
            self.open_pnl = (TickData.get_ltp() - self.holding_price) * self.holding_size if self.holding_side == 'buy' else (self.holding_price - TickData.get_ltp()) * self.holding_size
            self.open_pnl = int(self.open_pnl)

    def __calc_win_rate(self):
        if self.num_win > 0:
            self.win_rate = round(float(self.num_win) / float(self.num_trade,4))

    def __calc_pl_per_min(self):
        self.total_pl_per_min = int(self.total_pl / ((time.time() -  self.start_ut) / 60.0))

    def add_order_exec_price_gap(self, exe_price, ltp, side):
        gap = ltp - exe_price if side =='buy' else exe_price - ltp
        self.order_exec_price_gap.append(gap)


if __name__ == '__main__':
    import time
    Trade.initialize()
    ac = BotAccount()
    #start = time.time()
    #ac.sync_position_order()
    #print(time.time() -start)