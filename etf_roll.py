#!/usr/bin/python
import numpy as np
import tushare as ts
import mongo_handler as data_acquire
import time
import datetime

"""ETF ROLL BACK STRATEGY"""

__HABDLQ_tsodkaenn="API-token"

pro = ts.pro_api(__HABDLQ_tsodkaenn)

def get_etf_list():
    etf_lst = pro.fund_basic(market='E')
    return etf_lst


def get_daydata(ts_code, start_date, end_date):

    # df = pro.fund_daily(ts_code, start_date='20180101', end_date='20181029')
    time.sleep(3)
    df = pro.fund_daily(ts_code=ts_code, start_data=start_date, end_data=end_date)
    return df


def get_day_data(ts_code):
    # 拉取数据
    # df = pro.fund_nav(**{
        # "ts_code": "562320.SH",
        # "nav_date": "",
        # "offset": "",
        # "limit": "",
        # "market": "",
        # "start_date": "",
        # "end_date": ""
    # }, fields=[
        # "ts_code",
        # "ann_date",
        # "nav_date",
        # "unit_nav",
        # "accum_nav",
        # "accum_div",
        # "net_asset",
        # "total_netasset",
        # "adj_nav",
        # "update_flag"
    # ])

    df = pro.fund_nav(**{
        "ts_code": ts_code,
        "nav_date": "",
        "offset": "",
        "limit": "",
        "market": "",
        "start_date": "",
        "end_date": ""
    }, fields=[
        "ts_code",
        "ann_date",
        "nav_date",
        "unit_nav",
        "accum_nav",
        "accum_div",
        "net_asset",
        "total_netasset",
        "adj_nav",
        "update_flag"
    ])
    return df


if __name__ == "__main__":

    #每日更新数据
    lst = get_etf_list()
    for fund in lst.values:
        #股票型基金 且存续
        if fund[4] == '股票型' and fund[18] == 'L':
            data_acquire.insert_db(fund[0],fund[1],fund[2],fund[3],fund[4],fund[10],fund[14],fund[18])
            #fund[8]

            #format
            nowtm = str(datetime.datetime.now()).split()[0].split('-')[0] + str(datetime.datetime.now()).split()[0].split('-')[1] +str(datetime.datetime.now()).split()[0].split('-')[2]
            df = get_daydata(str(fund[0]), nowtm, nowtm)
            data_acquire.insert_db_daydata(df)



