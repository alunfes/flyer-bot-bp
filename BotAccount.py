from Trade import Trade
from WebsocketMaster import TickData

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

        self.sync_initial_positions()
        self.sync_initial_orders()
        self.last_exe_checked_ind = 0


    def initialize_order(self):
        self.order_side = ''
        self.order_price = 0
        self.order_executed_size = 0
        self.order_outstanding_size = 0
        self.order_id = 0
        self.order_expire = 0
        self.order_status = '' #new entry, pl order

    def initialize_holding(self):
        self.holding_side = ''
        self.holding_price = 0
        self.holding_size = 0
        self.holding_id = 0
        self.holding_dt = ''
        self.holding_ut = 0
        '''
        botのorder / position以外を無視したい。
        botのorderはidで判別可能
        botのpositionは、オーダー前と後のpositionの差がbotのpositionとして認識すれば良い(order前にget positionしないといけない, 0.6sec）
        売買した時にbotのpositionから処理されるとは限らないので、position syncできなくなるのでは？
        position syncをしない。->小数点２桁以下の誤差が生じることがある。(np) ->  SFDが取れない。
        '''

    def combine_status_data(self, positions):
        side = ''
        size = 0
        price = 0
        for p in positions:
            if p['open_date'] not in self.user_positions_dt:
                side = p['side'].lower()
                size += float(p['size'])
                price += float(p['price']) * float(p['size'])
        price = round(price / size)
        return side, round(size,8), round(price)


    def sync_initial_positions(self):
        positions = Trade.get_positions()
        for posi in positions:
            self.user_positions_dt.append(posi['open_date'])
        print('detected '+str(len(self.user_positions_dt))+' user positions')

    def sync_initial_orders(self):
        orders = Trade.get_orders()
        for order in orders:
            self.user_orders_id.append(order['child_order_acceptance_id'])
        print('detected ' + str(len(self.user_orders_id))+ ' user orders')

    def sync_position_order(self):
        position = Trade.get_positions()
        orders = Trade.get_orders()
        if len(position) > 0 and 'error' not in str(position):
            holding_side, holding_size, holding_price = self.combine_status_data(position)
            if self.holding_side != holding_side or abs(self.holding_price - holding_price) >= 1 or abs(self.holding_size - holding_size) >= 0.01:
                self.holding_side, self.holding_size, self.holding_price = holding_side, holding_size, holding_price
                print('position unmatch was detected! Synchronize with account position data.')
                print('holding_side={},holding_price={},holding_size={}'.format(self.holding_side,self.holding_price,self.holding_size))
                print(position)
            print('synchronized position data, side='+str(self.holding_side)+', size='+str(self.holding_size)+', price='+str(self.holding_price))
        else:
            self.initialize_holding()
        if len(orders) > 0:#need to update order status
            if len(orders) > 1:
                print('multiple orders are found! Only the first one will be synchronized!')
            try:
                if orders[0]['info']['child_order_state'] == 'ACTIVE' and orders[0]['info']['child_order_state'] not in self.user_orders_id:
                    self.order_id = orders[0]['info']['child_order_acceptance_id']
                    self.order_side = orders[0]['info']['side'].lower()
                    self.order_outstanding_size = float(orders[0]['info']['outstanding_size'])
                    self.order_executed_size = float(orders[0]['info']['executed_size'])
                    self.order_price = round(float(orders[0]['info']['price']))
                    self.order_status = 'new entry' if self.holding_side=='' else 'pl order'
                    print('synchronized order data, side='+str(self.order_side)+', outstanding size='+str(self.order_outstanding_size)+', price='+str(self.order_price))
            except Exception as e:
                print('Bot-sync_position_order:sync order key error!' + str(e))
        else:
            self.initialize_order()


    def calc_collateral_change(self):
        col = Trade.get_collateral()
        self.collateral = col['collateral']
        self.open_pnl = col['open_position_pnl']
        col_change = round(self.collateral - self.initial_collateral)
        self.realized_pl += col_change - self.collateral_change
        self.num_trade += 1
        if col_change > self.collateral_change:
            self.num_win += 1
        self.win_rate = float(self.num_win) / float(self.num_trade)
        self.collateral_change = col_change
        self.collateral_change_log.append(self.collateral_change)
        if self.elapsed_time > 0:
            self.collateral_change_per_min = round(self.collateral_change / (self.elapsed_time / 60.0), 4)

    def calc_pl(self, ltp):
        if self.holding_side == '':
            self.open_pnl = 0
        else:
            self.open_pnl = round((ltp - self.holding_price) * self.holding_size if self.holding_side == 'buy' else (self.holding_price - ltp) * self.holding_size)
        self.total_pl = self.realized_pl + self.open_pnl
        self.total_pl_log.append(self.total_pl)
        if self.elapsed_time > 0:
            self.total_pl_per_min = round(self.total_pl / (self.elapsed_time / 60.0), 4)



    def update_holding(self, side, price, size, id):
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
        self.holding_id = id

    def update_order(self, side, price, exec_size, outstanding_size, id, expire, status):
        self.order_side = side
        self.order_price = price
        self.order_executed_size = exec_size
        self.order_id = id
        self.order_expire = expire
        self.order_outstanding_size = outstanding_size
        self.order_status = status

    def check_execution(self):
        exe_data = TickData.get_exe_data()
        if len(exe_data) < self.last_exe_checked_ind:
            self.last_exe_checked_ind = 0
        else:
            n = len(exe_data)
            exe_data = exe_data[self.last_exe_checked_ind:]
            self.last_exe_checked_ind = n-1
        size = []
        price = []
        for exec in exe_data:
            if exec[self.order_side + "_child_order_acceptance_id"] == self.order_id:
                size.append(exec["size"])
                price.append(exec["price"])
        ave_p = 0
        if sum(size) > 0:
            ave_p = round(sum(price[i] * size[i] for i in range(len(price))) / sum(size))
        if abs(sum(size) - self.order_executed_size + self.order_outstanding_size) <= 0.00001: #order has been fully executed
            print(self.order_side+' order has been fully executed.'+'price='+str(ave_p)+', size='+str(sum(size)))
            self.update_holding(self.order_side,ave_p,self.order_outstanding_size+self.order_executed_size,self.order_id)
            self.initialize_order()
            return self.order_side+' order has been fully executed.'+'price='+str(ave_p)+', size='+str(sum(size))
        elif sum(size) > self.order_executed_size:
            print(self.order_side + ' order has been partially executed.' + 'price=' + str(ave_p) + ', size=' + str(sum(size) - self.order_executed_size))
            self.update_holding(self.order_side, ave_p, sum(size) - self.order_executed_size, self.order_id)
            self.update_order(self.order_side,self.order_price,sum(size), self.order_outstanding_size+self.order_executed_size - sum(size),self.order_id,self.order_expire,self.order_status)
            return self.order_side + ' order has been partially executed.' + 'price=' + str(ave_p) + ', size=' + str(sum(size) - self.order_executed_size)
        return ''

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