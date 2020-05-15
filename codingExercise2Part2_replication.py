# -*- coding: utf-8 -*-
"""
Created on Sat Feb 22 00:24:35 2020

@author: Sherry.He
"""
"""Coding Exercise 2. Part 2 - Replication - 1962 to 1991"""

import pandas as pd
import numpy as np
import wrds
conn = wrds.Connection(wrds_username='hexichen')

raw = conn.raw_sql("""
                     select gvkey, fyear, fyr, datadate, OIADP, AT, ACT, CHE, LCT, DLC, TXP, DP
                     from compa.funda WHERE
                     exchg in (11,12) and (fyear<1991 and fyear>1960)
                     """)

# Fundamental data from Compustat
lag1 = raw.copy()
lag1['fyear'] = lag1['fyear']+1
lag1 = lag1.rename(columns={'at':'at_lag1', 'act':'act_lag1', 'che':'che_lag1', 'lct':'lct_lag1', 'dlc':'dlc_lag1', 'txp':'txp_lag1', 'dp':'dp_lag1'})

data = pd.merge(raw, lag1, how='left', on=['gvkey','fyear'])

data = data.dropna()

data['avgTotalAsset'] = (data['at']+data['at_lag1'])/2 # average total assets
data['earnings'] = data['oiadp_x']/data['avgTotalAsset'] # earnings component

data['delta_ca'] = data['act']-data['act_lag1'] # change in current assets
data['delta_cash'] = data['che']-data['che_lag1'] # change in cash
data['delta_cl'] = data['lct']-data['lct_lag1'] # change in current liability
data['delta_std'] = data['dlc']-data['dlc_lag1'] # change in debt included in current liabilities
data['delta_tp'] = data['txp']-data['txp_lag1'] # change in income taxes payable
 
data['acc'] = (data['delta_ca']-data['delta_cash']) - (data['delta_cl']-data['delta_std']-data['delta_tp']) - data['dp']
data['accruals'] = data['acc']/data['avgTotalAsset'] # accrual component
data['cashFlows'] = data['earnings'] - data['accruals'] # cash flow component

fundaclean1 = data[['gvkey','fyear','fyr_x','datadate_x','earnings','accruals','cashFlows','avgTotalAsset']].copy()
fundaclean1 = fundaclean1.rename(columns={'fyr_x':'fyr', 'datadate_x':'datadate'})

fundaclean2 = pd.DataFrame()
yearlist=fundaclean1.fyear.unique().astype(int).tolist()
yearlist.sort()
for year in yearlist:
              fundacleantmp=fundaclean1[fundaclean1['fyear']==year]
              fundacleantmp['accrualDecile'] = pd.qcut(fundacleantmp['accruals'], 10, labels=[1,2,3,4,5,6,7,8,9,10])
              fundaclean2=fundaclean2.append(fundacleantmp)

# Stock return data from CRSP
crsp_m = conn.raw_sql("""
                      select a.permno, a.permco, a.date, b.shrcd, b.exchcd,
                      a.ret, a.retx, a.shrout, a.prc
                      from crsp.msf as a
                      left join crsp.msenames as b
                      on a.permno=b.permno
                      and b.namedt<=a.date
                      and a.date<=b.nameendt
                      where a.date between '01/01/1962' and '12/31/1994'
                      and b.exchcd between 1 and 2
                      and b.shrcd between 10 and 11
                      """) 

# delisting return(dlret) and delisting date(dlstdt)
dlret = conn.raw_sql("""
                     select permno, dlret, dlstdt 
                     from crsp.msedelist
                     """)
dlret['dlstdt']=pd.to_datetime(dlret['dlstdt']) # convert to datetime format
dlret['jdate']=dlret['dlstdt']+pd.offsets.MonthEnd(0) # from dlstdt to jdate: moving the date to end of each month

# add jdate, delisting-adjusted return, market value
# line up date to be end of month
crsp_m['date']=pd.to_datetime(crsp_m['date']) 
crsp_m['jdate']=crsp_m['date']+pd.offsets.MonthEnd(0)   
# adjust delisting return
crsp = pd.merge(crsp_m, dlret, how='left',on=['permno','jdate'])
crsp['dlret']=crsp['dlret'].fillna(0)
crsp['ret']=crsp['ret'].fillna(0)
crsp['retadj']=(1+crsp['ret'])*(1+crsp['dlret'])-1
# market value
crsp['me']=crsp['prc'].abs()*crsp['shrout'] 

# add size-decile return
ermport2 = conn.raw_sql("""
                      select *
                      from crsp.ermport2 as a
                      where a.date between '01/01/1962' and '12/31/1994'
                      """) 
ermport2['date']=pd.to_datetime(ermport2['date'])
# merge CRSP with ermport based on permno and date
crsp = pd.merge(crsp, ermport2, how='left', on=['permno','date'])

crsp['retadjsize'] =crsp['retadj']-crsp['decret']

crsp_final = crsp[['permno','permco','date','shrcd','exchcd','ret_x','retx','jdate','retadj','me','decret','retadjsize']].copy() 
crsp_final = crsp_final.rename(columns={'ret_x':'ret'}) # final CRSP dataset: permno - month - return, each company has a return for each month

# use CCM to merge CRSP with Compustat
ccm=conn.raw_sql("""
                  select gvkey, lpermno as permno, linktype, linkprim, 
                  linkdt, linkenddt
                  from crsp.ccmxpf_linktable
                  where substr(linktype,1,1)='L'
                  and (linkprim ='C' or linkprim='P')
                  """)
ccm['linkdt']=pd.to_datetime(ccm['linkdt'])
ccm['linkenddt']=pd.to_datetime(ccm['linkenddt'])
ccm['linkenddt']=ccm['linkenddt'].fillna(pd.to_datetime('today')) # if linkenddt is missing then set to today's date

# Portfolio Construction
ccm1 = pd.merge(fundaclean2, ccm, how='left', on=['gvkey'])
# Portfolio starting date: 4 months after fiscal year end. Return starts 5 months after fiscal year end.
ccm1['jdate0']=ccm1['datadate']+pd.offsets.MonthEnd(5)
ccm2=ccm1[(ccm1['jdate0']>=ccm1['linkdt'])&(ccm1['jdate0']<=ccm1['linkenddt'])] # jdate between linkdt and linkenddt
# Portfolio ending date: holding 12 months after starting date
ccm2['jdate1']=ccm2['jdate0']+pd.offsets.MonthEnd(11)

# merge ccm2 with CRSP by permno and date
crspcomp = pd.merge(ccm2, crsp_final, how='left', on=['permno'])
crspcomp=crspcomp[(crspcomp['jdate']>=crspcomp['jdate0'])&(crspcomp['jdate']<=crspcomp['jdate1'])]
crspcomp=crspcomp[['gvkey','datadate','fyear','accrualDecile','permno','jdate0','jdate1','retadj','retadjsize']].copy()

# Buy and Hold Return
crspcomp['1+retadj']=crspcomp['retadj']+1
crspcomp['1+retadjsize']=crspcomp['retadjsize']+1
# Group by gvkey, fyear: 

crspcomp['cumretrawyr1']=crspcomp.groupby(['gvkey','datadate'])['1+retadj'].cumprod()-1 
crspcomp['cumretsizeadjyr1']=crspcomp.groupby(['gvkey','datadate'])['1+retadjsize'].cumprod()-1 

# Final Dataset: gvkey---fyear---annualReturn
crspcomp_final = crspcomp.groupby(['gvkey','datadate']).nth(11).reset_index()
crspcomp_final = crspcomp_final[['gvkey','datadate','fyear','accrualDecile','cumretrawyr1','cumretsizeadjyr1']].copy()

# Table 6
# group by accrualDecile and calculate mean returns
portret = crspcomp_final.groupby('accrualDecile')['cumretrawyr1','cumretsizeadjyr1'].mean()

# group by year and calculate hedge returns
calHedgeReturn = crspcomp_final.groupby(['fyear','accrualDecile'])['cumretrawyr1','cumretsizeadjyr1'].mean().reset_index()
calHedgeReturn = calHedgeReturn.dropna()
years = calHedgeReturn['fyear'].unique()
hedgeReturn = pd.DataFrame(columns=['fyear','hedgerawret','hedgesizeadjret'])

for y in list(years):
    peryear = calHedgeReturn[calHedgeReturn['fyear'] == y]
    h1 = float(peryear[peryear['accrualDecile']==1]['cumretrawyr1'])-float(peryear[peryear['accrualDecile']==10]['cumretrawyr1'])
    h2 = float(peryear[peryear['accrualDecile']==1]['cumretsizeadjyr1'])-float(peryear[peryear['accrualDecile']==10]['cumretsizeadjyr1'])
    write = pd.Series([y, h1, h2], index=hedgeReturn.columns)
    hedgeReturn = hedgeReturn.append(write, ignore_index=True)

# Figure 2
import matplotlib.pyplot as plt
pos = list(range(len(hedgeReturn['fyear'])))
width = 0.38

year = [int(round(x)) for x in list(years)]
fit, ax = plt.subplots(figsize=(10,5))

plt.bar(pos, hedgeReturn['hedgerawret'], width, alpha=0.5, color='blue')
plt.bar([p + width for p in pos], 
        hedgeReturn['hedgesizeadjret'],
        width, 
        alpha=0.5, 
        color='orange') 
plt.xticks(pos, year, rotation='vertical')
ax.set_xlabel('Year')
ax.set_ylabel('Hedge Return')
ax.set_title('Annual Hedge Returns')















