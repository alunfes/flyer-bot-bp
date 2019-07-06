from sklearn import datasets
from sklearn.model_selection import train_test_split
import numpy as np
import pandas as pd
import pickle
import lightgbm as lgb
from sklearn import datasets
from sklearn.model_selection import train_test_split
import numpy as np
import pandas as pd


class LgbModel:
    def load_model(self):
        model_buy = None
        model_sell = None
        with open('./Model/lgb_model_buy.dat', 'rb') as f:
            model_buy = pickle.load(f)
        with open('./Model/lgb_model_sell.dat', 'rb') as f:
            model_sell = pickle.load(f)
        return model_buy, model_sell

    #fire when b/s hit upper kijun and opposite b/s hit lower_kijun
    def prediction(self, model_buy, model_sell, test_x, upper_kijun, lower_kijun):
        p_buy = model_buy.predict(test_x, num_iteration=model_buy.best_iteration)
        p_sell = model_sell.predict(test_x, num_iteration=model_sell.best_iteration)
        if p_buy >= upper_kijun and p_sell <= lower_kijun:
            return 1 #buy
        elif p_sell >= upper_kijun and p_buy <= lower_kijun:
            return -1 #sell
        else:
            return 0

    def generate_bsp_data_for_bot(self, df):
        return df.drop(['dt', 'size'], axis=1)