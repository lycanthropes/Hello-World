# -*- coding: utf-8 -*-
"""
Created on Sun Sep 13 10:04:38 2020

@author: Administrator
"""
import pandas as pd
import numpy as np
import pymysql
import time
import copy
from sqlalchemy import create_engine 
from dateutil.parser import parse # 可将任意形式的日期转化为时间类型
from datetime import *


# 连接sql数据库Astock的定位引擎
engine_ts = create_engine('mysql+pymysql://root:355378@localhost/Astock?charset=utf8')

# 读取表格的函数
def read_data(sql = """SELECT * FROM stock_basic LIMIT 20"""):
    df = pd.read_sql_query(sql, engine_ts)
    return df

# 首先读取日度行情数据,sql语句如下
sql_daily = """ SELECT  * FROM dailystock """
daily_stock = read_data(sql_daily)
daily_stock.head(100)
## 下载其他日度的股票数据
sql_daybasis = """ SELECT turnover_rate,total_share,ts_code,trade_date,float_share FROM dailybisic """
daily_basis = read_data(sql_daybasis)

########### 1、股票筛选  ############################################
####################################################################

# 1.1、只纳入60、30、00开头的股票。
daily_stock['trade_date'] = pd.to_datetime(daily_stock['trade_date'])
Ashare =daily_stock[ daily_stock['ts_code'].str.contains('^(?:60|30|00)')]
Ashare['ts_code'] = Ashare['ts_code'].apply(lambda x: x[:6])
Ashare['trade_date'] = pd.to_datetime(Ashare['trade_date'])
Ashare.head(100)
# 下载股票的IPO日期
ipo_date = pd.read_excel(r"E:\个人研究\factor pricing in china\data\ipo_date.xlsx")
ipo_date.columns =['ts_code','name','ipo_date']
ipo_date['ts_code'] = ipo_date['ts_code'].apply(lambda x: x[:6])

## 1.2、剔除上市不满6个月的股票行情数据
Ashare = Ashare.merge(ipo_date,on='ts_code',how='left')
Ashare['age'] = (Ashare['trade_date'] - Ashare['ipo_date'])/np.timedelta64(1,'M')
### 如果直接取大于等于6个月的，则会将ipo_date和age值为空的记录（大都为已经退市的公司）删掉。
# 故使用age非小于6的条件进行筛选
Ashare = Ashare[~(Ashare['age']<6)]
#Ashare[Ashare['ipo_date'].isna()]

## 1.3、删除在过去12个月里交易记录小于120天的， 或者在上个月交易记录小于15个的
Ashare = Ashare.sort_values(['ts_code','trade_date'])
Ashare.set_index('trade_date',inplace=True)
Ashare['ym'] = Ashare.index.to_period('M')
em_record = Ashare.groupby(['ts_code','ym'])['ts_code'].count().to_frame(name='records')
em_record = em_record.sort_index()
#计算各公司过去12个月的在交易天数
em_record['12month_count'] = em_record.groupby('ts_code',as_index=False)['records'].rolling(12).sum().reset_index(0,drop=True)
em_record['past_1y_count'] =  em_record.groupby('ts_code')['12month_count'].shift(1)
em_record['past_1y_criteria'] = np.where(em_record['past_1y_count'] >= 120, 1,0 )
# 计算上个月各公司的在交易天数
em_record['lm_count'] = em_record.groupby('ts_code')['records'].shift(1)
em_record['past_1m_criteria'] = np.where(em_record['lm_count'] >= 15, 1,0 )

Ashare = Ashare.reset_index().merge(em_record,on=['ts_code','ym'])
ashares = copy.deepcopy(Ashare)
## 如果公司在过去12个月的在交易天数小于120或过去1个月的在交易天数小于15，删除
ashares = ashares[(ashares['past_1y_criteria']==1)&(ashares['past_1m_criteria']==1)]

## 1.4、删除市值位于前30%的小公司（为避免壳公司数据的污染）
# 市值 = 收盘价 *所有股份
#sclips = daily_basis.head(100)
daily_basis['ts_code'] = daily_basis['ts_code'].apply(lambda x: x[:6])
daily_basis['trade_date'] = pd.to_datetime(daily_basis['trade_date'])
daily_basis = daily_basis.astype({"total_share":float,'turnover_rate':float,'float_share':float})
ashares =  ashares.merge(daily_basis, on=['ts_code','trade_date'],how='left')
ashares['mv'] = ashares['close'] * ashares['total_share']
ashares = ashares.sort_values(['trade_date','ts_code'])
ashares['mv30%'] = ashares.groupby('trade_date')['mv'].transform(lambda x : x.quantile(0.3))
sclips = ashares.head(300)
## 删除市值小于30%分位数的公司
ashares = ashares[ashares['mv']>ashares['mv30%']]

## 采样月度频率的数据
ashare_mon = ashares.groupby(['ts_code','ym']).last()

## 计算最近可得的报告期，以季度末日为代表

def report_date(date):   # 定义将非报告期转化为相应的报告 期的日期转换函数
    if isinstance(date,str):
        date = parse(date)
    
    year = date.year # 提取年份
    
    if datetime(year,1,1) <= date < datetime(year,4,30):  # 对于1，2，3，4月底前的财务数据采用上一年Q3数据
        rpt_date = datetime(year-1, 9,30)
        
    elif datetime(year,4,30) <= date <= datetime(year,8,30):  # 对于5，6，7，8月底前的财务数据，用本年度第一季度的数据，4月30日更新一季报
        rpt_date = datetime(year,3,31)
    elif datetime(year,8,31) <= date <= datetime(year,10,30): # 对于9，10月底前的财务数据，用本年第二季度的数据，8月31日前更新中报
        rpt_date = datetime(year,6,30)
    elif datetime(year,10,31) <= date <= datetime(year,12,31): # 对于11，12月底前的数据用本年度第三季度的数据，10月31日更新数据
        rpt_date = datetime(year,9,30)
    
    return rpt_date.strftime("%Y-%m-%d")  # 返回字符串类型的日期

## 根据上述函数，将每个交易月末的 财务报告期转化为 最近可得的报告期
ashare_mon['report_date'] = ashare_mon['trade_date'].apply(report_date)
ashare_mon['report_date'] = pd.to_datetime(ashare_mon['report_date'] )
ashare_mon = ashare_mon.reset_index()

############### 2、计算因子指标 ################################################
###############################################################################

##、首先导入财务数据，财务数据的更新统一按4、8、10月底的数据更新
# 利润表数据
sql_income = """ SELECT  n_income , ts_code ,report_type, ann_date,end_date, fv_value_chg_gain, invest_income,non_oper_income,non_oper_exp FROM incomesheet """
income_fn = read_data(sql_income)
income_fn = income_fn.dropna(subset=['n_income'])
income_fn = income_fn.fillna(0)
income_fn = income_fn.astype({'n_income':float,'fv_value_chg_gain':float,'invest_income':float, 'non_oper_income':float, 'non_oper_exp':float })
## 扣非净利润
income_fn['dedt_profit'] = income_fn['n_income'] -(income_fn['fv_value_chg_gain'] + income_fn['invest_income'] + income_fn['non_oper_income'] - income_fn['non_oper_exp'])
income_fn['report_date'] = pd.to_datetime(income_fn['end_date'])
income_fn['ts_code'] = income_fn['ts_code'].apply(lambda x:x[:6])
income_fn = income_fn.sort_values(['ts_code','report_date'])
#  Earnings = current Earning + (last yearly report earning - last corresponding earning )
income_fn = income_fn.drop_duplicates(subset=['ts_code','report_date'])

#sss = income_fn[income_fn.duplicated(subset=['ts_code','report_date'])]
fns = income_fn.set_index('report_date').groupby(['ts_code']).resample('Q').asfreq()
fns.drop('ts_code',axis=1,inplace =True)
fns['single_season'] = fns.groupby('ts_code')['dedt_profit'].apply(lambda x :x - x.shift(1))
fns['single_season'] = fns.groupby('ts_code')['single_season'].fillna(method='pad')
## 计算earning TTM,即为截至当期为止 4个季度的盈利
fns['Ettm'] = fns.groupby('ts_code',as_index=False)['single_season'].rolling(4,min_periods=4).sum().reset_index(0,drop=True)
fns = fns.reset_index()

ashare_mon = ashare_mon.merge(fns[['Ettm','ts_code','ann_date','report_date']],on = ['ts_code','report_date'],how = 'left')

# 财务表数据
sql_bs = """ SELECT  oth_eqt_tools_p_shr , ts_code ,report_type, ann_date,end_date, total_hldr_eqy_exc_min_int FROM balancesheet """
bs_fn = read_data(sql_bs)
bs_fn = bs_fn.astype({'oth_eqt_tools_p_shr':float,'total_hldr_eqy_exc_min_int':float})
bs_fn = bs_fn.dropna(subset=['total_hldr_eqy_exc_min_int'])
bs_fn = bs_fn.fillna(0)
## 账面价值
bs_fn['bookvalue'] = bs_fn['total_hldr_eqy_exc_min_int'] - bs_fn['oth_eqt_tools_p_shr']
bs_fn['report_date'] = pd.to_datetime(bs_fn['end_date'])
bs_fn['ts_code'] = bs_fn['ts_code'].apply(lambda x:x[:6])
ashare_mon = ashare_mon.merge(bs_fn[['ts_code','report_date','bookvalue']],on =['ts_code','report_date'],how='left')
## 现金流量表数据
sql_cashsheet = """SELECT c_cash_equ_beg_period,c_cash_equ_end_period, ts_code, end_date,report_type FROM cashflowsheet"""
casheet = read_data(sql_cashsheet)
casheet = casheet.astype({'c_cash_equ_beg_period':float,'c_cash_equ_end_period':float})
casheet['cash_chg'] = casheet['c_cash_equ_end_period'] - casheet['c_cash_equ_beg_period']
casheet['ts_code'] = casheet['ts_code'].apply(lambda x:x[:6])
casheet['report_date'] = pd.to_datetime(casheet['end_date'])
ashare_mon = ashare_mon.merge(casheet[['ts_code','report_date','cash_chg']],on =['ts_code','report_date'],how='left')


### 计算PE(TTM) BM CP
# BM
ashare_mon['BM'] =  ashare_mon['bookvalue'] / (ashare_mon['close'] * ashare_mon['total_share'])
# CP
ashare_mon['CP'] = ashare_mon['cash_chg'] / (ashare_mon['close'] * ashare_mon['total_share'])
# PE(TTM)  Earnings = current Earning + (last yearly report earning - last corresponding earning )
ashare_mon['EP'] = ashare_mon['Ettm'] / (ashare_mon['close'] * ashare_mon['total_share'])


# function to assign sz and bm bucket
def sz_bucket(row):
    if row['me']==np.nan:
        value=''
    elif row['me']<=row['sizemedn']:
        value='S'
    else:
        value='B'
    return value

def bm_bucket(row):
    if 0<=row['BM']<=row['bm30%']:
        value = 'L'
    elif row['BM']<=row['bm70%']:
        value='M'
    elif row['BM']>row['bm70%']:
        value='H'
    else:
        value=''
    return value

def ep_bucket(row):
    if 0<=row['EP']<=row['ep30%']:
        value = 'L'
    elif row['EP']<=row['ep70%']:
        value='M'
    elif row['EP']>row['ep70%']:
        value='H'
    else:
        value=''
    return value

### 在每个月按PE\ BM\CP排序分组
ashare_mon = ashare_mon.drop_duplicates(keep='first')
ashare_mon = ashare_mon.sort_values(['ym','ts_code'])
# size breakpoint
ashare_mon['me'] = ashare_mon['close'] * ashare_mon['float_share']
ashare_mon['sizemedn'] = ashare_mon.groupby(['ym'])['me'].transform(lambda x: x.median())

# beme breakpoint
ashare_mon['bm30%'] = ashare_mon.groupby(['ym'])['BM'].transform(lambda x: x.quantile(0.3))
ashare_mon['bm70%'] = ashare_mon.groupby(['ym'])['BM'].transform(lambda x: x.quantile(0.7))

# EP breakpoint
ashare_mon['ep30%'] = ashare_mon.groupby(['ym'])['EP'].transform(lambda x: x.quantile(0.3))
ashare_mon['ep70%'] = ashare_mon.groupby(['ym'])['EP'].transform(lambda x: x.quantile(0.7))

## 按分组点分组
ashare_mon['szport'] = np.where( (ashare_mon['BM']>0) &(ashare_mon['EP']>0),ashare_mon.apply(sz_bucket,axis=1), '')
ashare_mon['bmport'] = np.where( (ashare_mon['BM']>0) &(ashare_mon['EP']>0),ashare_mon.apply(bm_bucket,axis=1), '')
ashare_mon['epport'] = np.where( (ashare_mon['EP']>0) &(ashare_mon['EP']>0),ashare_mon.apply(ep_bucket,axis=1), '')


## 计算各股票的月度 收益率，并复权
ashare_mon = ashare_mon.sort_values(['ts_code','ym'])
ashare_mon['adj_close']  = ashare_mon['close'] * ashare_mon['hfq']
ashare_mon['mret'] = ashare_mon.groupby('ts_code')['adj_close'].apply(lambda x: x.pct_change())


# function to calculate value weighted return
def wavg(group, avg_name, weight_name):
    d = group[avg_name]
    w = group[weight_name]
    try:
        return (d * w).sum() / w.sum()
    except ZeroDivisionError:
        return np.nan
    
## 计算各组合收益率
# value-weigthed return
vwret = ashare_mon.groupby(['ym','szport','bmport']).apply(wavg, 'mret','mv').to_frame().reset_index().rename(columns={0: 'vwret'})
vwret['sbport']=vwret['szport']+vwret['bmport']

# firm count
vwret_n=ashare_mon.groupby(['jdate','szport','bmport'])['mret'].count().reset_index().rename(columns={'mret':'n_firms'})
vwret_n['sbport']=vwret_n['szport']+vwret_n['bmport']

# tranpose
ff_factors=vwret.pivot(index='jdate', columns='sbport', values='vwret').reset_index()
ff_nfirms=vwret_n.pivot(index='jdate', columns='sbport', values='n_firms').reset_index()

# create SMB and HML factors
ff_factors['WH']=(ff_factors['BH']+ff_factors['SH'])/2
ff_factors['WL']=(ff_factors['BL']+ff_factors['SL'])/2
ff_factors['WHML'] = ff_factors['WH']-ff_factors['WL']

ff_factors['WB']=(ff_factors['BL']+ff_factors['BM']+ff_factors['BH'])/3
ff_factors['WS']=(ff_factors['SL']+ff_factors['SM']+ff_factors['SH'])/3
ff_factors['WSMB'] = ff_factors['WS']-ff_factors['WB']
ff_factors=ff_factors.rename(columns={'jdate':'date'})

# n firm count
ff_nfirms['H']=ff_nfirms['SH']+ff_nfirms['BH']
ff_nfirms['L']=ff_nfirms['SL']+ff_nfirms['BL']
ff_nfirms['HML']=ff_nfirms['H']+ff_nfirms['L']

ff_nfirms['B']=ff_nfirms['BL']+ff_nfirms['BM']+ff_nfirms['BH']
ff_nfirms['S']=ff_nfirms['SL']+ff_nfirms['SM']+ff_nfirms['SH']
ff_nfirms['SMB']=ff_nfirms['B']+ff_nfirms['S']
ff_nfirms['TOTAL']=ff_nfirms['SMB']
ff_nfirms=ff_nfirms.rename(columns={'jdate':'date'})








