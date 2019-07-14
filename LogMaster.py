import csv
import os
import asyncio
import threading
import pytz
from datetime import datetime
from WebsocketMaster import TickData
from S3Master import S3Master

'''
upload log file to s3 every 10 min
'''
class LogMaster:
    @classmethod
    def initialize(cls):
        S3Master.initialize()
        cls.upload_ut = datetime.now().timestamp()
        cls.lock = threading.Lock()
        cls.log_file = 'flyer_bot_log.csv'
        if os.path.isfile(cls.log_file):
            os.remove(cls.log_file)
        cls.log_list = []
        cls.key_list = ['dt', 'message', 'ltp', 'posi_side', 'prediction','posi_price', 'posi_size','pt_order_side', 'pt_order_price',
                        'pt_order_outstanding_size','pt_order_id',
                        'total_pl', 'total_pl_per_min', 'num_trade','win_rate', 'execution gap']
        cls.latest_pl_log = {}
        cls.add_log('initialized log master',0,None)
        cls.__all_log_to_csv()
        print('initialized LogMaster')

    @classmethod
    def add_log(cls, log_message, prediction, ac):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(cls.__add_log(log_message, prediction, ac))

    @classmethod
    async def __add_log(cls, log_message, prediction, ac):
        jst = pytz.timezone('Asia/Tokyo')
        d =''
        if ac is not None:
            d = {'dt':datetime.now(jst), 'message':log_message, 'prediction':prediction,'ltp':TickData.get_ltp(), 'posi_side':ac.holding_side, 'posi_price':ac.holding_price, 'posi_size':ac.holding_size,
                 'pt_order_side':ac.pt_side, 'pt_order_price':ac.pt_price, 'pt_order_outstanding_size':ac.pt_outstanding_size,
                 'pt_order_id':ac.pt_order_id, 'total_pl':ac.total_pl, 'total_pl_per_min':ac.total_pl_per_min,'num_trade':ac.num_trade,
                 'win_rate':ac.win_rate, 'execution gap':sum(ac.order_exec_price_gap) / float(len(ac.order_exec_price_gap))}
            cls.latest_pl_log = d
        else:
            d = {'dt':datetime.now(jst), 'message':log_message, 'prediction':prediction,'ltp':TickData.get_ltp(), 'posi_side':'', 'posi_price':0, 'posi_size':0,
                 'pt_order_side':'', 'pt_order_price':0, 'pt_order_outstanding_size':0,
                 'pt_order_id':'', 'total_pl':0, 'total_pl_per_min':0,'num_trade':0,
                 'win_rate':0, 'execution gap':0}
        with cls.lock:
            cls.log_list.append(d)
            if len(cls.log_list) > 1000:
                cls.log_list.pop(0)
        await cls.__add_log_to_csv()

    @classmethod
    def get_latest_performance(cls):
        if len(cls.log_list) > 1:
            return {'dt':cls.latest_pl_log['dt'], 'prediction':cls.latest_pl_log['prediction'],'posi_side':cls.latest_pl_log['posi_side'], 'total_pl':cls.latest_pl_log['total_pl'],
                    'total_pl_per_min':cls.latest_pl_log['total_pl_per_min'],'num_trade':cls.latest_pl_log['num_trade'],'win_rate':cls.latest_pl_log['win_rate'],
                    'execution gap':cls.latest_pl_log['execution gap']}
        else:
            return {}

    @classmethod
    async def __all_log_to_csv(cls):
        try:
            with open(cls.log_file, 'w') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=cls.key_list)
                writer.writeheader()
                with cls.lock:
                    for data in cls.log_list:
                        writer.writerow(data)
        except IOError as e:
            print('IO error!' + str(e))

    @classmethod
    async def __add_log_to_csv(cls):
        try:
            with open(cls.log_file, 'a') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=cls.key_list)
                with cls.lock:
                    log_data = cls.log_list[-1]
                    writer.writerow(log_data)
                    if datetime.now().timestamp() - cls.upload_ut >= 600:
                        S3Master.remove_file(cls.log_file)
                        S3Master.save_file(cls.log_file)
                        cls.upload_ut = datetime.now().timestamp()
        except IOError as e:
            print('IO error!' + str(e))


if __name__ == '__main__':
    import os
    print(os.getcwd())
