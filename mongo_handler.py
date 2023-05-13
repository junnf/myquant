#!/usr/bin/python3

import pymongo as mongo
from pymongo import MongoClient
Client = MongoClient()

db = Client['quant']

#issue_mount 发行份额
#p_value 面值
# min_amount	float	Y	起点金额(万元)
# exp_return	float	Y	预期收益率
# benchmark	str	Y	业绩比较基准
# status	str	Y	存续状态D摘牌 I发行 L已上市
# invest_type	str	Y	投资风格
# type	str	Y	基金类型
# trustee	str	Y	受托人
# purc_startdate	str	Y	日常申购起始日
# redm_startdate	str	Y	日常赎回起始日
# market	str	Y	E场内O场外
def insert_db(ts_code, name, management="", custodian="", fund_type="", issue_amount="", p_value=0.0, status=""):
    post = {ts_code:[
           {'ts_code':ts_code},
           {'name':name},
           {'managemnet':management},
           {'custodian':custodian},
           {'fund_type':fund_type},
           {'issue_amount':issue_amount},
           {'p_value':p_value},
           {'status':status}]}
    posts = db.posts
    rt = posts.insert_one(post).inserted_id

def insert_db_daydata(df):
    print(df)
    db.posts.insert_many(df.to_dict('records'))

def find_group(ts_code):
    nd = db.posts.find({'ts_code':ts_code})
    return nd

