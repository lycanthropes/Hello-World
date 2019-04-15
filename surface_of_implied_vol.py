#   隐含波动率曲面计算

import math
import numpy as np 
import pandas as pd
from scipy.stats import norm
from scipy import interpolate     #做插值处理
from WindPy import *
w.start()

''' 
1、期权平价关系估计标的资产价格
在估计隐含波动率时，需要知晓标的资产的价格。而一般在估计波动率曲面时，不直接使用标的资产市场价格，而是利用市场上对应的看涨和看跌期权的价格，以及看涨-看跌期权平价公式，来推出标的资产的价格。并在之后估计的波动率曲面时，使用该价格作为标的资产价格。看涨-看跌期权平价公式指的是：
call+K∗e−r∗t=put+S，从而S=call+K∗e−r∗t−put
'''

option_data = w.wset("optionchain","date=2017-10-17;us_code=510050.SH;option_var=全部;call_put=全部")
option_data = pd.DataFrame(option_data.Data, index=option_data.Fields, columns=option_data.Codes).T
option_data = option_data[['us_name','option_code','exe_type','strike_price','call_put','expiredate']]
option_data = option_data[option_data['expiredate']>8]     #剔除10月到期的期权
option_data['option_price'] = w.wss(list(option_data['option_code']),'close','tradeDate=2017-10-17').Data[0]   #提取期权收盘价
option_data['expiredate'] = option_data['expiredate']/365      #年化剩余存续期
option_data['q'] = 0
option_data['SHIBOR(3M)'] = 4.3711           
option_data.columns = ['期权标的','期权代码','期权类型','行权价格','看涨/看跌','剩余存续期','期权收盘价','红利q','SHIBOR(3M)']
option_data.index = range(len(option_data))
option_data.head()
data_call = option_data[option_data['看涨/看跌']=='认购']
data_put = option_data[option_data['看涨/看跌']=='认沽']

def asset_price(c , p , K, r, t):  #计算标的资产价格
    S = c + K*math.exp(-r*t) - p
    return S

data_call['标的资产价格'] = 0.00
data_put['标的资产价格'] = 0.00
for i in data_call.index:
    for j in data_put.index:
        if data_call['行权价格'][i] == data_put['行权价格'][j] and data_call['剩余存续期'][i] == data_put['剩余存续期'][j]:
            c = data_call['期权收盘价'][i]
            p = data_put['期权收盘价'][j]
            K = data_call['行权价格'][i]
            r = 4.3711/100
            t = data_call['剩余存续期'][i]
            S = asset_price(c, p, K, r, t)      #由平价公式计算标的资产价格
            data_call['标的资产价格'][i] = S
            data_put['标的资产价格'][j] = S

'''
在估计波动率曲面时，往往一个期权品种只有一个曲面而不区分看涨和看跌期权。而通常的做法是利用处于虚值状态的看涨(行权价>标的资产价格)和看跌(行权价<标的资产价格)期权来一起估计波动率曲面。所以，在这里选取处于虚值状态的期权。
'''
call_data = data_call[data_call['行权价格']>data_call['标的资产价格']]    
put_data = data_put[data_put['行权价格']<data_put['标的资产价格']]        
            
#2、50ETF期权隐含波动率估计
'''由BSM公式，对于看涨期权的定价：
c=S∗e−q∗t∗N(d1)−K∗e−r∗t∗N(d2),d1=ln(SK)+(r−q+sigma2/2)∗tsigma∗t0.5,d2=d1−sigma∗t0.5
c=S∗e−q∗t∗N(d1)−K∗e−r∗t∗N(d2),d1=ln(SK)+(r−q+sigma2/2)∗tsigma∗t0.5,d2=d1−sigma∗t0.5

其中c为看涨期权理论价格，S为标的资产即50ETF的价格，q为标的资产红利率，t为期权的剩余期限(年)，K为期权执行价格，r为无风险利率，sigma为波动率c为看涨期权理论价格，S为标的资产即50ETF的价格，q为标的资产红利率，t为期权的剩余期限(年)，K为期权执行价格，r为无风险利率，sigma为波动率
由BSM公式，对于看跌期权的定价：
p=−S∗e−q∗t∗N(d1)+K∗e−r∗t∗N(d2),d1=ln(SK)+(r−q+sigma2/2)∗tsigma∗t0.5,d2=d1−sigma∗t0.5
p=−S∗e−q∗t∗N(d1)+K∗e−r∗t∗N(d2),d1=ln(SK)+(r−q+sigma2/2)∗tsigma∗t0.5,d2=d1−sigma∗t0.5

其中p为看跌期权理论价格，S为标的资产即50ETF的价格，q为标的资产红利率，t为期权的剩余期限(年)，K为期权执行价格，r为无风险利率，sigma为波动率p为看跌期权理论价格，S为标的资产即50ETF的价格，q为标的资产红利率，t为期权的剩余期限(年)，K为期权执行价格，r为无风险利率，sigma为波动率
从BSM公式出发，一种求解隐含波动率的思路为：

设定一个隐含波动率的初始值
根据该隐含波动率的初始值计算期权的理论价格
计算期权理论价格和实际市场期权价格的差值
计算波动率的调整量 = 理论价格和市场价格的差值/vega，其中vega=dcdsgima=S∗e−q∗t∗N(d1)vega=dcdsgima=S∗e−q∗t∗N(d1)
更新波动率 = 波动率 - 波动率调整量
当波动率调整量小于某个阈值时 或者 迭代次数大于300次时，输出波动率作为隐含波动率'''
#2.1 虚值状态看涨期权隐含波动率
def ImpVolCall(MktPrice, Strike, Expiry, Asset, IntRate, Dividend, Sigma, error):
    n = 1
    Volatility = Sigma   #初始值
    dv = error + 1
    while abs(dv) > error:
        d1 = np.log(Asset / Strike) + (IntRate - Dividend + 0.5 * Volatility **2) * Expiry
        d1 = d1 / (Volatility * np.sqrt(Expiry))
        d2 = d1 - Volatility * np.sqrt(Expiry)
        PriceError = Asset * math.exp(-Dividend * Expiry) * norm.cdf(d1) - Strike * math.exp(-IntRate * Expiry) * norm.cdf(d2) - MktPrice
        Vega1 = Asset * np.sqrt(Expiry / 3.1415926 / 2) * math.exp(-0.5 * d1 **2 )
        dv = PriceError / Vega1
        Volatility = Volatility - dv    #修正隐含波动率
        n = n + 1
        
        if n > 300:     #迭代次数过多的话
            ImpVolCall = 0.0
            break
        
        ImpVolCall = Volatility
    
    return ImpVolCall
            
# 虚值状态看跌期权隐含波动率
def ImpVolPut(MktPrice, Strike, Expiry, Asset, IntRate, Dividend, Sigma, error):
    n = 1
    Volatility = Sigma   #初始值
    dv = error + 1
    while abs(dv) > error:
        d1 = np.log(Asset / Strike) + (IntRate - Dividend + 0.5 * Volatility **2) * Expiry
        d1 = d1 / (Volatility * np.sqrt(Expiry))
        d2 = d1 - Volatility * np.sqrt(Expiry)
        PriceError = -Asset * math.exp(-Dividend * Expiry) * norm.cdf(-d1) + Strike * math.exp(-IntRate * Expiry) * norm.cdf(-d2) - MktPrice
        Vega1 = Asset * np.sqrt(Expiry / 3.1415926 / 2) * math.exp(-0.5 * d1 **2 )
        dv = PriceError / Vega1  
        Volatility = Volatility - dv      #修正隐含波动率
        n = n + 1

        if n > 300:     #迭代次数过多的话
            ImpVolPut = 0.0
            break

        ImpVolPut = Volatility

    return ImpVolPut

put_data.index = range(len(put_data))
Sigma, error = 1, 0.001
for j in range(len(put_data)):
    MktPrice = put_data.loc[j,'期权收盘价']
    Strike = put_data.loc[j,'行权价格']
    Expiry = put_data.loc[j,'剩余存续期']
    Asset = put_data.loc[j,'标的资产价格']
    IntRate = put_data.loc[j,'SHIBOR(3M)']/100
    Dividend = put_data.loc[j,'红利q']
    volatility = ImpVolPut(MktPrice, Strike, Expiry, Asset, IntRate, Dividend, Sigma, error)
    put_data.loc[j,'隐含波动率'] = volatility
    
# 看涨-看跌期权组合结果
res_df = pd.concat([put_data[['行权价格','剩余存续期','隐含波动率','看涨/看跌']],call_data[['行权价格','剩余存续期','隐含波动率','看涨/看跌']]])
res_df = res_df.sort_values(['行权价格','剩余存续期'])
res_df.index = range(len(res_df))
res_df.head()

#3、50ETF看涨期权隐含波动率插值处理(价格内部)
'''因为根据已有期权的隐含波动率，来做波动率曲面的话，数据过少。所以往往通过插值的方式来扩充数据集，这里考虑在行权价方向上进行样条插值，而在剩余存续期方向上进行线性插值。
在进行插值前，要先将波动率矩阵转化为方差矩阵，即每个波动率平方再乘以剩余期限。利用numpy下的interp函数可以实现线性插值，而利用scipy库下的interpolate模块中的spline函数可以实现样条插值。其中输入的参数为剩余期限离散点，行权价的离散点，已有的方差矩阵，以及插值形式。 要注意的是，这里插值并不外推，即只在已有的点空间内部进行插值。具体地说是在已经存在的行权价范围内，剩余存续期范围内进行插值，而不外推。
最后将经过二维插值得到的方差矩阵，再转化为波动率矩阵。'''
res = res_df
res['隐含方差'] = res['隐含波动率']**2*res['剩余存续期']
spline_data = res[(res['行权价格']<=2.90)&(2.60<=res['行权价格'])]     #采取行权价对应有多个期限的数据
vol_mat = []
for j in list(spline_data['行权价格'].unique()):
    vol_mat.append(list(spline_data[spline_data['行权价格']==j]['隐含方差']))
vol_mat = pd.DataFrame(vol_mat)
vol_mat

# 3.1 行权价方向上样条插值
vol_after_k = pd.DataFrame([])
for j in range(3):
    k = np.array(list(spline_data['行权价格'].unique()))
    kmesh = np.linspace(k.min(),k.max(),300)
    volinter = interpolate.spline(k,np.array(vol_mat[j]),kmesh)
    vol_after_k['期限'+str(j)] = volinter
vol_after_k.index = kmesh
vol_after_k.head()


#3.2剩余存续期方向上线性插值
tt = np.array(list(res['剩余存续期'].unique()))
tt.sort()
tmesh = np.linspace(tt.min(), tt.max(), 300)
res_kt = []
for j in vol_after_k.index:
    volinter = np.interp(tmesh,tt,np.array(vol_after_k.loc[j,:]))
    res_kt.append(volinter)
    
vol_after_kt = pd.DataFrame(res_kt)
vol_after_kt.index = vol_after_k.index
vol_after_kt.columns = tmesh
for j in vol_after_kt.index:
    vol_after_kt.loc[j,:] = np.sqrt(np.array(vol_after_kt.loc[j,:]) / tmesh)   #由隐含方差计算隐含波动率
vol_after_kt.head()

# 4、可视化波动率曲面
from mpl_toolkits.mplot3d import Axes3D  
from matplotlib import cm  
from matplotlib import pylab

pylab.style.use('ggplot')
maturityMesher, strikeMesher = np.meshgrid(tmesh, kmesh)  
pylab.figure(figsize = (12,7))  
ax = pylab.gca(projection = '3d')  
surface = ax.plot_surface(strikeMesher, maturityMesher, vol_after_kt*100, cmap = cm.jet)  
pylab.colorbar(surface,shrink=0.75)  
pylab.title('50ETF期权波动率曲面(2017-10-17)')  
pylab.xlabel("Strike")  
pylab.ylabel("Maturity")  
ax.set_zlabel("Volatility(%)")  
pylab.show()
            
