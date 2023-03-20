# -*- coding: utf-8 -*-
"""
Created on Tue Mar 14 16:22:25 2023

@author: Shahmir.Tariq
"""
#%%
import os
from glob import iglob
from datetime import datetime as dt

from pandas import read_csv, concat, read_excel
from numpy import array_split

import auto_pull as ap

DF_POMS_PATH = "../Pull Sheet Data LH Team"
MANIFEST_DETAILS_PATH = "../ManifestCSV/ManifestDetails.csv"
DOWNLOAD_DIR = r'C:\Users\shahmir.tariq\Downloads\Auto'
#%%
print("\nGetting the POMs...")
all_files = iglob(os.path.join(DF_POMS_PATH, "*.csv"))
df_poms = concat((read_csv(f, encoding='ISO-8859-1', engine='c', usecols=['Manifest No']
                    ) for f in all_files), ignore_index=True)

print("\nGetting Manifest Details...")
fact_table = read_csv(MANIFEST_DETAILS_PATH)
print(fact_table.head())

missing_nums = fact_table.loc[~fact_table['manifest_num'].isin(df_poms['Manifest No']),
                              'manifest_num']
print(missing_nums.shape)
missing_chunks = array_split(missing_nums.to_numpy(), 1+len(missing_nums)//300)
#%%
for num, chunk in enumerate(missing_chunks):
    print(f"Getting chunk no. {num+1} of {len(missing_chunks)}...")
    str_chunk = '\n'.join(map(str, chunk))
    ap.download_pull_sheet(str_chunk)
    if (num+1) != len(missing_chunks):
        print("Moving on to next chunk...")
    else:
        print("Done.")
#%%
print("\nMoving from the Auto folder and possibly converting to csv...")
all_files = iglob(os.path.join(DOWNLOAD_DIR, "*.xlsx"))
df_poms = concat((read_excel(f) for f in all_files), ignore_index=True)
if len(df_poms.index) == 0:
    print("df is empty, not converting...")
else:
    df_poms.to_csv(DF_POMS_PATH + "/" + "Pull Sheet Data--" +
                   dt.now().strftime("%y-%h-%d--%H-%M") + ".csv",
                   index=False)

print("\nCleaning up the Auto folder...")
all_files = iglob(os.path.join(DOWNLOAD_DIR, "*"))

for f in all_files:
    os.remove(f)
