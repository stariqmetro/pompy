import os
from glob import iglob
from datetime import datetime as dt
import sys

from pandas import read_csv, concat, read_excel, DataFrame
from numpy import array_split

import auto_pull as ap

DF_POMS_PATH = "C:/OneDrive - Metropolitan Warehouse/Vendor Control/Data Files/POM Level/Pull Sheet Data LH Team"
MANIFEST_DETAILS_PATH = sys.argv[1]
DOWNLOAD_DIR = r"C:\OneDrive - Metropolitan Warehouse\Vendor Control\Data Files\POM Level\Downloads\Auto"

print("\nGetting the POMs...")
all_files = iglob(os.path.join(DF_POMS_PATH, "*.csv"))
df_poms = concat((read_csv(f, encoding='ISO-8859-1', engine='c', usecols=['Manifest No']
                           ) for f in all_files), ignore_index=True)
#print(df_poms['Manifest No'].tail())

print("\nGetting Manifest Details...")
all_files = iglob(os.path.join(MANIFEST_DETAILS_PATH, "*.csv"))
try:
    fact_table = concat((read_csv(f, usecols=['manifest_num']) for f in all_files), ignore_index=True)
except:
    fact_table = DataFrame({'manifest_num': []})

fact_table['manifest_num'] = fact_table['manifest_num'].fillna(0).astype('int64')
#print(fact_table['manifest_num'].tail())
missing_nums = fact_table.loc[(~fact_table['manifest_num'].isin(df_poms['Manifest No'])) &
                              (fact_table["manifest_num"] != 0),'manifest_num'].drop_duplicates()
print(missing_nums.shape)
#print(missing_nums.head())

if len(missing_nums.index) == 0:
    print("\nNo missing nums found. Exiting...\n")
    sys.exit(0)

missing_chunks = array_split(missing_nums.to_numpy(), 1+len(missing_nums)//300)

for num, chunk in enumerate(missing_chunks):
    print(f"Getting chunk no. {num+1} of {len(missing_chunks)}...")
    #str_chunk = '\n'.join(map(str, chunk))
    DataFrame(chunk).to_clipboard(header=None, index=False)
    ap.download_pull_sheet()
    if (num+1) != len(missing_chunks):
        print("Moving on to next chunk...")
    else:
        print("Done.")

print("\nMoving from the Auto folder and possibly converting to csv...")
all_files = iglob(os.path.join(DOWNLOAD_DIR, "*.xlsx"))
df_poms = concat((read_excel(f, dtype=object) for f in all_files), ignore_index=True)
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
