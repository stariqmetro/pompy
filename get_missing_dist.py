import os
from glob import iglob
from datetime import datetime as dt

from pandas import read_csv, concat, read_excel, DataFrame
from numpy import array_split

import auto_dist as ad

DIST_PATH = "C:/OneDrive - Metropolitan Warehouse/Vendor Control/Data Files/POM Level/Helpers/ZipDistances.csv"
DF_MANIFEST_PATH = "C:/OneDrive - Metropolitan Warehouse/Vendor Control/Data Files/POM Level/TMSProduct/temp_dist.csv"
DOWNLOAD_DIR = r"C:\OneDrive - Metropolitan Warehouse\Vendor Control\Data Files\POM Level\Downloads\Auto"

print("\nGetting DISTS...")
df_dist = read_csv(DIST_PATH, encoding='ISO-8859-1', engine='c')

print("\nGetting Missing Dist...")
try:
    fact_table = read_csv(DF_MANIFEST_PATH, encoding='ISO-8859-1',
                          engine='c', usecols=['dist'], ignore_index=True)
except:
    fact_table = DataFrame({'dist': []})
print(fact_table.head())

ad.download_dist()

print("\nMoving from the Auto folder and possibly converting to csv...")
all_files = iglob(os.path.join(DOWNLOAD_DIR, "*.xlsx"))
df_dist_d = concat((read_excel(f) for f in all_files), ignore_index=True)
df_dist_d.drop(df_dist_d.loc[df_dist_d['Distance']==99999999].index)
if len(df_dist_d.index) == 0:
    print("df is empty, not converting...")
else:
    df_dist_d['Zip'] = df_dist_d['Zip_Code1'].astype(str, errors='ignore'
                                                     ) + ";" + df_dist_d["Zip_Code2"].astype(str, 
                                                     errors = 'ignore')
    df_dist_d.drop(columns=['Zip_Code1', 'Zip_Code2'], inplace=True)
    df_dist = concat([df_dist, df_dist_d])
    df_dist.drop_duplicates().to_csv(DIST_PATH, index=False)

print("\nCleaning up the Auto folder...")
all_files = iglob(os.path.join(DOWNLOAD_DIR, "*"))

for f in all_files:
    os.remove(f)
