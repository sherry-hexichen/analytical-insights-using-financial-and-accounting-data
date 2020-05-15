# -*- coding: utf-8 -*-
"""
Created on Fri Feb 14 22:26:55 2020

@author: Sherry.He
"""

""" Coding Exercise 2. Part 1 - Extension - 2010 to 2018"""

import pandas as pd
import numpy as np
import wrds
conn = wrds.Connection(wrds_username='hexichen')

# Sample Selection
raw = conn.raw_sql("""
                     select gvkey, fyear, OIADP, AT, ACT, CHE, LCT, DLC, TXP, DP
                     from compa.funda WHERE
                     exchg in (11,12) and (fyear<2018 and fyear>2008)
                     """)

# Variable Construction
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

fundaclean1 = data[['gvkey','fyear','earnings','accruals','cashFlows']].copy()

# Portfolio Construction
for year in fundaclean1.groupby(['fyear']):
    fundaclean1['accrualDecile'] = pd.qcut(fundaclean1['accruals'], 10, labels=[1,2,3,4,5,6,7,8,9,10]) # rank on accruals
    fundaclean1['earningDecile'] = pd.qcut(fundaclean1['earnings'], 10, labels=[1,2,3,4,5,6,7,8,9,10]) # rank on earnings
    fundaclean1['cashFlowDecile'] = pd.qcut(fundaclean1['cashFlows'], 10, labels=[1,2,3,4,5,6,7,8,9,10]) # rank on cashFlows

# Sum By Year
variableSum = pd.DataFrame()
variableSum['accrualsMean'] = fundaclean1.groupby(['accrualDecile'])['accruals'].mean()
variableSum['accrualsMedian'] = fundaclean1.groupby(['accrualDecile'])['accruals'].median()
variableSum['cashFlowsMean'] = fundaclean1.groupby(['accrualDecile'])['cashFlows'].mean()
variableSum['cashFlowsMedian'] = fundaclean1.groupby(['accrualDecile'])['cashFlows'].median()
variableSum['earningsMean'] = fundaclean1.groupby(['accrualDecile'])['earnings'].mean()
variableSum['earningsMedian'] = fundaclean1.groupby(['accrualDecile'])['earnings'].median()
table1 = variableSum.transpose() # Table 1 Panel A: Components of Earnings

# Construct the Large Panel Data with 5 years lagging and 5 years leading 
leadEarnings = fundaclean1[['gvkey','fyear']].copy()
for i in range(1,6):
    lead = fundaclean1[['gvkey','fyear','earnings']].copy()
    lead['fyear'] = lead['fyear']-i
    lead = lead.rename(columns={'earnings':'earnings_lead'+str(i)})
    leadEarnings = pd.merge(leadEarnings, lead, how='left', on=['gvkey','fyear'])

panelData = pd.merge(fundaclean1, leadEarnings, how='left', on=['gvkey','fyear'])

lagEarnings = fundaclean1[['gvkey','fyear']].copy()
for j in range(1,6):
    lag = fundaclean1[['gvkey','fyear','earnings']].copy()
    lag['fyear'] = lag['fyear']+j
    lag = lag.rename(columns={'earnings':'earnings_lag'+str(j)})
    lagEarnings = pd.merge(lagEarnings, lag, how='left', on=['gvkey','fyear'])
    
panelData = pd.merge(panelData, lagEarnings, how='left', on=['gvkey', 'fyear']) 
panelData = panelData.sort_values(['gvkey','fyear'], axis=0, ascending=True)

# Regression

# Pooled Regression
import statsmodels.formula.api as sm
#y = panelData['earnings_lead1']
#x = panelData['earnings']
table2_pooled = sm.ols(formula='earnings_lead1 ~ earnings', data=panelData).fit()
table2_pooled.summary()

table3_pooled = sm.ols(formula='earnings_lead1 ~ accruals + cashFlows', data=panelData).fit()
table3_pooled.summary()

# Collect SIC codes
sic = conn.raw_sql("""select gvkey, sic from compa.company""")
panelData = pd.merge(panelData, sic, on=['gvkey'])
panelData['2_dig_SIC'] = panelData.sic.astype(str).str[:2].astype(int) # obtain the first 2 digits of the 4-digit SIC code

eachIndus = panelData.groupby(['2_dig_SIC']).groups
indusList = list(eachIndus.keys()) # list of all Industries

'''Some industries have only one row in panelData, i.e. no earnings_lead1.
Regression models cannot be fit for these industries.
'''
# check the 2_dig_SICs with single row
only = (indusList * (panelData.groupby(['2_dig_SIC'])['fyear'].count() == 1)).nonzero() # the INDEX in indusList for indus with single row

unwant_index = []
for i in range(len(only[0])):
    unwant_index.append(only[0][i])
    
for element in sorted(unwant_index, reverse=True):
    del indusList[element]

#Industry Level Regression
alpha0 = [] # intercept
alpha1 = [] # earnings
for i in indusList: 
    indus = panelData.groupby(['2_dig_SIC']).get_group(i)
    indusReg = sm.ols(formula='earnings_lead1 ~ earnings', data=indus).fit()
    alpha0.append(indusReg.params[0])
    alpha1.append(indusReg.params[1])

table2_indus_alpha0 = pd.DataFrame(alpha0).describe()
table2_indus_alpha1 = pd.DataFrame(alpha1).describe()

gama0 = [] # intercept
gama1 = [] # accruals
gama2 = [] # cashFlows
for i in indusList: 
    indus = panelData.groupby(['2_dig_SIC']).get_group(i)
    indusReg = sm.ols(formula='earnings_lead1 ~ accruals + cashFlows', data=indus).fit()
    gama0.append(indusReg.params[0])
    gama1.append(indusReg.params[1])
    gama2.append(indusReg.params[2])
    
table3_indus_gama0 = pd.DataFrame(gama0).describe()
table3_indus_gama1 = pd.DataFrame(gama1).describe()
table3_indus_gama2 = pd.DataFrame(gama2).describe()

# Figure: High and Low Accrual Portfolio
decile1 = panelData.loc[panelData['accrualDecile'] == 1]
decile1_earnings = decile1[['earnings_lag5','earnings_lag4','earnings_lag3','earnings_lag2', 'earnings_lag1',
                   'earnings','earnings_lead1','earnings_lead2','earnings_lead3','earnings_lead4','earnings_lead5']]
decile10 = panelData.loc[panelData['accrualDecile'] == 10]
decile10_earnings = decile10[['earnings_lag5','earnings_lag4','earnings_lag3','earnings_lag2', 'earnings_lag1',
                   'earnings','earnings_lead1','earnings_lead2','earnings_lead3','earnings_lead4','earnings_lead5']]


extremeEarnings = pd.DataFrame()
extremeEarnings['decile1'] = decile1_earnings.mean(axis=0)
extremeEarnings['decile10'] = decile10_earnings.mean(axis=0)
extremeEarningsData = extremeEarnings.rename(index={'earnings_lag5':'-5','earnings_lag4':'-4','earnings_lag3':'-3','earnings_lag2':'-2', 'earnings_lag1':'-1',
                   'earnings':'0','earnings_lead1':'1','earnings_lead2':'2','earnings_lead3':'3','earnings_lead4':'4','earnings_lead5':'5'})

import matplotlib.pyplot as plt
plt.plot( 'decile1', data=extremeEarningsData, color='blue', linewidth=2)
plt.plot( 'decile10', data=extremeEarningsData, color='orange', linewidth=2)
plt.xlabel('Event Year')
plt.ylabel('Mean Earnings')
plt.title('Time Series Properties of Earnings with High and Low Accrual')
plt.legend()
plt.show() # second of Figure 1


# Figure: High and Low Earning Portfolio
decile1 = panelData.loc[panelData['earningDecile'] == 1]
decile1_earnings = decile1[['earnings_lag5','earnings_lag4','earnings_lag3','earnings_lag2', 'earnings_lag1',
                   'earnings','earnings_lead1','earnings_lead2','earnings_lead3','earnings_lead4','earnings_lead5']]
decile10 = panelData.loc[panelData['earningDecile'] == 10]
decile10_earnings = decile10[['earnings_lag5','earnings_lag4','earnings_lag3','earnings_lag2', 'earnings_lag1',
                   'earnings','earnings_lead1','earnings_lead2','earnings_lead3','earnings_lead4','earnings_lead5']]


extremeEarnings = pd.DataFrame()
extremeEarnings['decile1'] = decile1_earnings.mean(axis=0)
extremeEarnings['decile10'] = decile10_earnings.mean(axis=0)
extremeEarningsData = extremeEarnings.rename(index={'earnings_lag5':'-5','earnings_lag4':'-4','earnings_lag3':'-3','earnings_lag2':'-2', 'earnings_lag1':'-1',
                   'earnings':'0','earnings_lead1':'1','earnings_lead2':'2','earnings_lead3':'3','earnings_lead4':'4','earnings_lead5':'5'})

import matplotlib.pyplot as plt
plt.plot( 'decile1', data=extremeEarningsData, color='blue', linewidth=2)
plt.plot( 'decile10', data=extremeEarningsData, color='orange', linewidth=2)
plt.xlabel('Event Year')
plt.ylabel('Mean Earnings')
plt.title('Time Series Properties of Earnings with High and Low Earnings')
plt.legend()
plt.show() # first of Figure 1


# Figure: High and Low cashFlow Portfolio
decile1 = panelData.loc[panelData['cashFlowDecile'] == 1]
decile1_earnings = decile1[['earnings_lag5','earnings_lag4','earnings_lag3','earnings_lag2', 'earnings_lag1',
                   'earnings','earnings_lead1','earnings_lead2','earnings_lead3','earnings_lead4','earnings_lead5']]
decile10 = panelData.loc[panelData['cashFlowDecile'] == 10]
decile10_earnings = decile10[['earnings_lag5','earnings_lag4','earnings_lag3','earnings_lag2', 'earnings_lag1',
                   'earnings','earnings_lead1','earnings_lead2','earnings_lead3','earnings_lead4','earnings_lead5']]


extremeEarnings = pd.DataFrame()
extremeEarnings['decile1'] = decile1_earnings.mean(axis=0)
extremeEarnings['decile10'] = decile10_earnings.mean(axis=0)
extremeEarningsData = extremeEarnings.rename(index={'earnings_lag5':'-5','earnings_lag4':'-4','earnings_lag3':'-3','earnings_lag2':'-2', 'earnings_lag1':'-1',
                   'earnings':'0','earnings_lead1':'1','earnings_lead2':'2','earnings_lead3':'3','earnings_lead4':'4','earnings_lead5':'5'})

import matplotlib.pyplot as plt
plt.plot( 'decile1', data=extremeEarningsData, color='blue', linewidth=2)
plt.plot( 'decile10', data=extremeEarningsData, color='orange', linewidth=2)
plt.xlabel('Event Year')
plt.ylabel('Mean Earnings')
plt.title('Time Series Properties of Earnings with High and Low Cash Flows')
plt.legend()
plt.show() # third of Figure 1