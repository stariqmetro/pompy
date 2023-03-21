# -*- coding: utf-8 -*-
"""
Created on Tue Mar 14 16:22:25 2023

@author: Shahmir.Tariq
"""
#%%
import os
from glob import iglob
from datetime import datetime as dt

from pandas import read_csv, concat, read_excel, DataFrame
from numpy import array_split

import auto_tickets as at

DF_POMS_PATH = "../Pull Sheet Data LH Team"
CLOSING_TICKETS_PATH = "../Closing Tickets"
DOWNLOAD_DIR = r'C:\Users\shahmir.tariq\Downloads\Auto'
#%%
print("\nGetting the POMs...")
all_files = iglob(os.path.join(DF_POMS_PATH, "*.csv"))
df_poms = concat((read_csv(f, encoding='ISO-8859-1', engine='c', usecols=['OrderNo']
                    ) for f in all_files), ignore_index=True)

print("\nGetting Closing Tickets...")
all_files = iglob(os.path.join(CLOSING_TICKETS_PATH, "*.csv"))
closing_tickets = concat((read_csv(f, encoding='ISO-8859-1', engine='c', usecols=['Order #']
                    ) for f in all_files), ignore_index=True)
print(closing_tickets.head())

missing_nums = df_poms.loc[~df_poms['OrderNo'].isin(closing_tickets['Order #']),
                              'OrderNo']
print(missing_nums.shape)
missing_chunks = array_split(missing_nums.to_numpy(), 1+len(missing_nums)//10000)
#%%
for num, chunk in enumerate(missing_chunks):
    print(f"Getting chunk no. {num+1} of {len(missing_chunks)}...")
    #str_chunk = '\n'.join(map(str, chunk))
    DataFrame(chunk).to_clipboard(header=None, index=False)
    at.download_closing_tickets()
    if (num+1) != len(missing_chunks):
        print("Moving on to next chunk...")
    else:
        print("Done.")
#%%
print("\nMoving from the Auto folder and possibly converting to csv...")
all_files = iglob(os.path.join(DOWNLOAD_DIR, "*.xlsx"))
df_closing = concat((read_excel(f) for f in all_files), ignore_index=True)
if len(df_closing.index) == 0:
    print("df is empty, not converting...")
else:
    df_closing.to_csv(CLOSING_TICKETS_PATH + "/" + "Closing-Tickets-from-Order#--" +
                   dt.now().strftime("%y-%h-%d--%H-%M") + ".csv",
                   index=False)

print("\nCleaning up the Auto folder...")
all_files = iglob(os.path.join(DOWNLOAD_DIR, "*"))

for f in all_files:
    os.remove(f)
