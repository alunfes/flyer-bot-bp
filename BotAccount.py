from Trade import Trade
from WebsocketMaster import TickData
from datetime import datetime
from SystemFlg import SystemFlg
import time

'''
botから提供されたorder idを元に約定確認をし、holding, plを同時に計算する。
order idとマッチしたexecution dataは削除。
2分経過したexecution dataは削除。

'''


class execution_checking_data():
    def __init__(self):
        self.execution_data = []
        self.execution_ut = []
        self.execution_checked = []
        self.matched_execution_data = []

    def add_executions(self, executions):
        if len(self.execution_data) > 0:
            # check start ind
            start_ind = 0
            for i in range(len(executions)):
                if self.execution_data[-1]['id'] == executions[len(executions) -i]:
                    start_ind = len(executions) -i + 1
            add_exec = executions[start_ind:]
            self.execution_data.extend(add_exec)
            self.execution_ut.extend([int(time.time()) for i in range(len(add_exec))])
            self.execution_checked.extend([False for i in range(len(add_exec))])
        else:
            self.execution_data.extend(executions)
            self.execution_ut.extend([int(time.time()) for i in range(len(executions))])
            self.execution_checked.extend([False for i in range(len(executions))])


    def remove_executions(self, ind):
        self.execution_data.pop(ind)
        self.execution_ut.pop(ind)
        self.execution_checked.pop(ind)


class BotAccount:
    def __init__(self):
        self.initialize_order()
        self.initialize_holding()
        self.initialize_pt_order()

        self.user_positions_dt = []
        self.user_orders_id = []

        self.initial_collateral = Trade.get_collateral()['collateral']
        self.collateral = 0
        self.open_pnl = 0
        self.realized_pl = 0
        self.total_pl = 0 #realized_pl + open pnl
        self.total_pl_log = []
        self.total_pl_per_min = 0
        self.collateral_change = 0 #collateral - initial_collateral
        self.collateral_change_per_min = 0
        self.collateral_change_log = []
        self.order_exec_price_gap = [0]
        self.num_trade = 0
        self.num_win = 0
        self.win_rate = 0
        self.elapsed_time = 0

        self.num_trade = 0
        self.num_win = 0
        self.win_rate = 0
        self.start_ut = time.time()

        self.exec_data = execution_checking_data()

        self.before_posi_side = ''
        self.before_posi_size = 0


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
        mid_check_flg = False #flg for 30秒ごとに一回API経由で約定履歴・注文一覧
        i = 0
        while SystemFlg.get_system_flg():
            if i > 10:
                i = 0
                #30秒ごとに一回API経由で約定履歴を取得して、order idを確認
                executions = Trade.get_executions()
                self.exec_data.add_executions(executions)


            executions = TickData.get_exe_data()
            self.exec_data.add_executions(executions)

            i += 1
            time.sleep(3)


    def check_order_execution(self):
        for i in range(len(self.exec_data.execution_data)):
            for j in range(len(self.active_order_ids)):
                if self.exec_data.execution_checked[i] == False:
                    if self.exec_data.execution_data[i]['buy_child_order_acceptance_id'] == self.active_order_ids[j] or self.exec_data.execution_data[i]['sell_child_order_acceptance_id'] == self.active_order_ids[j]:
                        self.active_order_outstanding_size[j] -=self.exec_data.execution_data[i]['size']
                        self.exec_data.execution_checked[i] = True
                        self.executions_hist_log.append(self.exec_data.execution_data[i])
                        self.calc_pl(self.exec_data.execution_data[i])
                        self.update_holding(self.exec_data.execution_data[i]['side'].lower(), self.exec_data.execution_data[i]['price'], self.exec_data.execution_data[i]['size'])
        #remove checked executions
        target_index = [i for i, x in enumerate(self.exec_data.execution_checked) if x == True]
        

        #remove active orders



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
        self.active_order_ids.append(order_id)
        self.active_order_ut.append(time.time())
        self.all_order_ids.append(order_id)
        self.active_order_outstanding_size.append(size)

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