from Trade import Trade
from WebsocketMaster import TickData
import datetime
import time

'''
Account側：

plの計算は、order idを使って計算する(
Trade側：
エントリーオーダーは、全て約定するまで待ってholdingを更新する
ptは、目標価格に達した時点でopt price order
lcは、目標価格に達した時点で
'''


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


    def initialize_order(self):
        self.executions_hist_log = []
        self.active_order_ids = []

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

    def __update_holding(self, side, price, size):
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

    def add_order(self, order_id):
        self.active_order_ids.append(order_id)

    def set_pt_order(self, order_id, side ,total_size, price):
        self.initialize_pt_order()
        self.pt_order_id = order_id
        self.pt_side = side
        self.pt_total_size = total_size
        self.pt_price = price
        self.pt_outstanding_size = total_size

    '''
    Tradeからside, size, priceを貰えば、holding, pl自炊できるのでは?
    ->holdingは可能。plは、一回のtradeで全て同じsideであることが担保されていれば可能。
    '''
    def check_order_execution(self):
        executions = Trade.get_executions()
        for i in range(len(executions)):
            for j in range(len(self.active_order_ids)):
                if executions[i]['child_order_acceptance_id'] == self.active_order_ids[j]:
                    side = executions[i]['side'].ilower()
                    price = executions[i]['price']
                    size = executions[i]['size']
                    self.executions_hist_log.append(executions[i])
                    self.calc_pl(executions[i])
                    self.update_holding(side, price, size)
                    self.active_order_ids.pop(j)

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
        self.__calc_holding_pnl()
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