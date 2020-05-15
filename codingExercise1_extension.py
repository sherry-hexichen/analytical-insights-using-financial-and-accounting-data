# -*- coding: utf-8 -*-
"""
Created on Fri Feb  7 23:38:14 2020

@author: Sherry.He
"""
# -*- coding: utf-8 -*-
"""
Created on Thu Feb  6 22:37:18 2020

@author: Sherry.He
"""

""" Coding Exercise 1. Extension """

import pandas as pd
import wrds
conn = wrds.Connection(wrds_username='hexichen')

import numpy as np
from scipy.stats.mstats import winsorize
from scipy import stats
import matplotlib.pyplot as plt

funda = conn.raw_sql("""
                     select gvkey, fyear, ni, csho, prcc_f, sich
	                  from compa.funda 
                      where (consol='C' and indfmt='INDL' 
                             and datafmt='STD' and popsrc='D') 
                      and fyear<2019 and fyear>2007
                     """)

company = conn.raw_sql("""select gvkey, sic from compa.company""") 

# using current SIC codes to replace the missing historical SIC codes
dataset = pd.merge(funda, company, on=['gvkey'])
dataset['sic1'] = np.where(dataset['sich']>0, dataset['sich'], dataset['sic'])
dataset = dataset.drop(['sich','sic'], axis=1)
dataset['sic1'] = dataset['sic1'].astype(int)

# restrict the industry to "INDL"
indexNames =dataset[ (dataset['sic1'] >= 4400) & (dataset['sic1'] <= 5000) ].index
dataset.drop(indexNames , inplace=True)
indexNames =dataset[ (dataset['sic1'] >= 6000) & (dataset['sic1'] <= 6500) ].index
dataset.drop(indexNames , inplace=True)

# adding variable Market Value(MV)
dataset.describe()
dataset['mv'] = dataset['csho'] * dataset['prcc_f']

# lag variable: mv@t-1
dataset_lag1 = dataset[['gvkey','fyear','ni','mv']].copy()
dataset_lag1['fyear'] = dataset_lag1['fyear'] + 1 # ex. 1987 changes to 1988
dataset_lag1 = dataset_lag1.rename(columns={'mv':'mv_lag1','ni':'ni_lag1'})

# dataset left join dataset_lag1
dataset = pd.merge(dataset, dataset_lag1, how='left', on=['gvkey','fyear'])

dataset = dataset.rename(columns={'ni':'earn'}) # change name from ni(netIncome) to earn
dataset=dataset.dropna() # drop the missing values
dataset[['earn', 'mv_lag1']].describe() # five-data summary before scaling

# winsorization before scaling
varlist = ['earn', 'mv_lag1']
for var in varlist:
    dataset[var] = dataset[var].replace(np.Inf, np.nan)
    dataset[var]=np.where(dataset[var].isnull(), np.nan, winsorize(dataset[var], limits=(0.01,0.01)))

dataset['earn_s']=dataset['earn']/dataset['mv_lag1']

scaledEarning = dataset[['gvkey','fyear','earn_s']]

# lag variable: mv@t-2
dataset_lag2 = dataset[['gvkey','fyear','earn','mv']].copy()
dataset_lag2['fyear'] = dataset_lag2['fyear'] + 2 # ex. 1987 changes to 1989
dataset_lag2 = dataset_lag2.rename(columns={'mv':'mv_lag2','ni':'ni_lag2'})

# dataset left join dataset_lag2
dataset = pd.merge(dataset, dataset_lag2, how='left', on=['gvkey','fyear'])


dataset['earnchg'] = dataset['earn_x'] - dataset['ni_lag1']
dataset['earnchg_s'] = dataset['earnchg'] / dataset['mv_lag2'] # scaled change in earnings

scaledChgEarning = dataset[['gvkey','fyear','earnchg_s']]

scaledEarning2=scaledEarning.dropna()
scaledChgEarning2=scaledChgEarning.dropna()

# winsorization after scaling
varlist=['earn_s']
for var in varlist:
        #scaledEarning2[var] = scaledEarning.replace(np.Inf, np.nan)
        scaledEarning2[var] = np.where(scaledEarning2[var].isnull(), np.nan, winsorize(scaledEarning2[var], limits=(0.01,0.01)))

varlist=['earnchg_s']
for var in varlist:
        #scaledChgEarning2[var] = scaledChgEarning2.replace(np.Inf, np.nan)
        scaledChgEarning2[var] = np.where(scaledChgEarning2[var].isnull(), np.nan, winsorize(scaledChgEarning2[var], limits=(0.01, 0.01)))

panelBsample=scaledEarning2.describe().transpose()  # sample line of scaled earnings

'''Table 1 Panel B: Scaled Earnings'''
panelB=scaledEarning2.groupby('fyear')['earn_s'].describe() 

panelAsample=scaledChgEarning2.describe().transpose() # sample line of scaled change in earnings

'''Table 1 Panel A: Scaled change in Earnings'''
panelA=scaledChgEarning2.groupby('fyear')['earnchg_s'].describe()

# the 2 figures need to be executed one-by-one o/w the 2 plots overlap
'''Figure 3'''
plt.hist(scaledEarning2['earn_s'], bins=50)
plt.axvline(x=0, color='k', linestyle='--')
plt.xlabel('Earnings Interval')
plt.ylabel('Frequency')
plt.title('Fig.3. The Distribution of Annual Net Income')
plt.show


'''Figure 1'''
plt.hist(scaledChgEarning2['earnchg_s'], bins=50)
plt.axvline(x=0, color='k', linestyle='--')
plt.xlabel('Change in Earnings Interval')
plt.ylabel('Frequency')
plt.title('Fig.1. The Distribution of Changes in Annual Net Income')
plt.show







