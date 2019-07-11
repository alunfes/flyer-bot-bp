from Trade import Trade
from WebsocketMaster import TickData
import datetime


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


    def initialize_order(self):
        self.executions_hist_log = []
        self.active_order_ids = []


    def initialize_holding(self):
        self.holding_side = ''
        self.holding_price = 0
        self.holding_size = 0
        self.holding_dt = ''
        self.holding_ut = 0


    def __update_holding(self, side, price, size, id):
        self.holding_dt = datetime.now()
        self.holding_ut = time.time()
        if self.holding_side == '':
            self.holding_side = side
            self.holding_price = price
            self.holding_size = size
        elif self.holding_side == side:
            self.holding_price = (self.holding_price * self.holding_size + price * size) / (self.holding_size + size)
            self.holding_size += round(size,2)
        elif self.holding_side != side and self.holding_size == size:
            self.initialize_holding()
        elif self.holding_side != side and self.holding_size > size:
            self.holding_size -= round(size,2)
        elif self.holding_side != side and self.holding_size < size:
            self.holding_side = side
            self.holding_size = round(size - self.holding_size,2)
            self.holding_price = price

    def add_order(self, order_id):
        self.active_order_ids.append(order_id)

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
    [{'id': 1146943205, 'side': 'SELL', 'price': 1256500.0, 'size': 3.0, 'exec_date': '2019-07-07T08:39:51.52', 
    'child_order_id': 'JFX20190707-083908-462964F', 'commission': 0.0, 'child_order_acceptance_id': 'JRF20190707-083908-190050'}, 
    '''
    def calc_pl(self, execution):
        side = execution['side'].ilower()
        price = execution['price']
        size = execution['size']
        if side != self.holding_side and self.holding_side != '':
            self.realized_pl += (price - self.holding_price) * size if side =='buy' else (self.holding_price - price) * size
            self.realized_pl = int(self.realized_pl)
        self.__calc_holding_pnl()

    def __calc_holding_pnl(self):
        if self.holding_side != '':
            self.open_pnl = (TickData.get_ltp() - self.holding_price) * self.holding_size if self.holding_side == 'buy' else
            (self.holding_price - TickData.get_ltp()) * self.holding_size
            self.open_pnl = int(self.open_pnl)

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