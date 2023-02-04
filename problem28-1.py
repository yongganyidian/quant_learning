# -*- coding:utf-8 -*-
# @FileName :problem28-1.py
# @Time :2023/2/4 11:05
# @Author :'姜艳龙'
# @Function:第28章课后第一题，与原作者答案不同，有代考证
import numpy as np
import pandas as pd

data = pd.read_csv('problem28-1.csv', index_col='date')
data.index.name = 'Date'
data.index = pd.to_datetime(data.index, format='%Y-%m-%d')


def momentum(price, period):
    """
    做差法求动量，做除法与此类似
    :param price: 股票价格，收盘价
    :param period: 时间跨度
    :return:
    """

    momen = (price - price.shift(period))/price.shift(period)
    return momen


def get_signal(x):
    signal = np.where(x > 0, 1, np.where(x < 0, -1, 0))
    # 由于时间跨度，导致前period期没有数据，或者在前面momentum函数中将momen dropna
    return (signal)


def get_ret(price, period):
    """
    以收益率的作为判断准确率的标准
    :param price: 收盘价
    :param period: 时间跨度
    :return: 注意返回值因为时间跨度而丢失的数据
    """
    ret = (price - price.shift(1)) / price.shift(1)
    return ret[period:]


def get_win_rate(signal, period):
    signal = pd.Series(signal[period:], index=get_ret(close, period).index)
    momen = get_ret(close, period)[1:] * signal.shift(1)[1:]
    win_rate = sum(momen > 0) / sum(momen != 0)
    print(win_rate)


if __name__ == '__main__':
    close = data['Close']
    print('*' * 30, '输入时间跨度，判断预测准确率', '*' * 30)
    period = int(input("请选择时间跨度： "))
    signal = get_signal(momentum(close, period))
    get_win_rate(signal, period)
