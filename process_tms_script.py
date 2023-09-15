# %%

#exit(0)
# Import the necessary libraries, keeping the python standard ones above the non-standard.
from glob import glob
import os
import re
import subprocess
from datetime import datetime
from itertools import product

import pandas as pd
import numpy as np

# %%
# Assign path variables. This way we can easily change the path later if needed.
REPORT_PATH = "//filesrv/MercuryGate/" #"../Others/Extracted.csv"
PARQUET_PATH = "//filesrv/MercuryGate/Separated/Backup.parquet"
ORDERS_PATH = "C:/OneDrive - Metropolitan Warehouse/Vendor Control/Data Files/POM Level/Pull Sheet Data LH Team/"
TICKETS_PATH = "C:/OneDrive - Metropolitan Warehouse/Vendor Control/Data Files/KPI Reports - 2/POMS in KPI Reports/"
OTHER_TICKETS = "C:/OneDrive - Metropolitan Warehouse/Vendor Control/Data Files/KPI Reports - 2/linehaul_poms/"
OUTPUT_PATH = "C:/OneDrive - Metropolitan Warehouse/Vendor Control/Data Files/POM Level/TMSProduct/"
AUTO_PULL_PATH = "C:/OneDrive - Metropolitan Warehouse/Vendor Control/Data Files/POM Level/pompy/get_missing_pull_sheets.py"
#AUTO_TICKETS_PATH = "C:/OneDrive - Metropolitan Warehouse/Vendor Control/Data Files/POM Level/pompy/get_missing_closing_tickets.py"
AUTO_DIST_PATH = "C:/OneDrive - Metropolitan Warehouse/Vendor Control/Data Files/POM Level/pompy/get_missing_dist.py"
ZIP_DISTANCE_PATH = "C:/OneDrive - Metropolitan Warehouse/Vendor Control/Data Files/POM Level/Helpers/ZipDistances.csv"

# %%
# Get a list of all XLSX files in the folder
xlsx_files = glob(os.path.join(REPORT_PATH, '*.xlsx'))

# Sort the list of files by modification time (latest first)
xlsx_files.sort(key=os.path.getmtime, reverse=True)

# Get the latest two XLSX files
latest_files = xlsx_files[:2]

# Print the file names
#for file in latest_files:
#    print(file)

# %%
try:
    inv_file = [x for x in latest_files if "carrier" in x][0] #invoice files
    ship_file = [x for x in latest_files if "carrier" not in x][0] #shipment info files
except IndexError:
    print("No file found, exiting...")
    exit(0)

# %%
# read carrier invoices
df_inv = pd.read_excel(inv_file)
df_inv = df_inv.drop_duplicates()

# %%
# read shipment info
df = pd.read_excel(ship_file)
df = df.drop_duplicates()

# %%

# %%
#df = pd.read_csv(REPORT_PATH)
#df = df.drop_duplicates()


# %% [markdown]
# Get the columns in the cols list only.

# %%
cols = ['Primary Reference', 'Pinnacle Manifest No', 'Target Ship (Late)',
        'Target Delivery (Late)', 'Actual Delivery', 'Actual Ship', 'Origin Code',
        'Origin Zip', 'Dest Code', 'Dest Zip', 'Carrier Name', 'Carrier Total']
df = df[cols]
df.rename(columns={'Carrier Name': 'Carrier'}, inplace=True)

# %%
cutoff_date = '2023-04-01'
selected_refs = df.groupby('Primary Reference').filter(lambda x: (x['Target Ship (Late)'] >= cutoff_date).all())['Primary Reference'].unique()
df = df[df['Primary Reference'].isin(selected_refs)]

# %%
df_inv['Load ID'] = df_inv['Load ID'] + ' (Load ID)'

# %%
invoice_data = df_inv[['Load ID', 'Invoice Charge', 'Invoice Date', 'Invoice Number', 'Invoice Total Fuel', 'Invoice Total Line Haul']].set_index('Load ID')

# %%
df = df.set_index('Primary Reference')

# %%
df = df.join(invoice_data)

# %%
df = df.reset_index(names=['Primary Reference'])

# %% [markdown]
# Concat the old data to the newer one, replacing any data in the old with the new one.<br>
# Archive it in a parquet file.
# %%
if os.path.exists(PARQUET_PATH):
        pqt_df = pd.read_parquet(PARQUET_PATH)
        pqt_df = pqt_df.loc[~pqt_df['Primary Reference'].isin(df['Primary Reference'])]
        pqt_df = pd.concat([pqt_df, df], ignore_index=True)
        pqt_df.astype(str).to_parquet(PARQUET_PATH, index=False)
else:
        df.astype(str).to_parquet(PARQUET_PATH, index=False)

# %% [markdown]
# Before we can use the Carrier Total, we need to replace it with the Invoice Charge.<br>
# Before we can use Invoice Charge, we need to aggregate it over the load.<br>
# Before we can aggregate it over the load, we need to divide it over multiple loads if its loads > 1.<br>

# %%
#divide the invoices equally over their loads, no effect if only 1 load
df['Invoice Number'] = df['Invoice Number'].fillna('')
number_of_loads_against_invoice = df.groupby('Invoice Number')['Primary Reference'].transform('nunique')
df['Invoice Charge'] = df['Invoice Charge'] / number_of_loads_against_invoice
df['Invoice Total Line Haul'] = df['Invoice Total Line Haul'] / number_of_loads_against_invoice
df['Invoice Total Fuel'] = df['Invoice Total Fuel'] / number_of_loads_against_invoice

# %%
#aggregate costs against reference, summing against unique invoices
df_gb = df.groupby([
    'Primary Reference',
    'Invoice Number'])[[
        'Invoice Charge',
        'Invoice Total Line Haul',
        'Invoice Total Fuel']].max().groupby(level='Primary Reference').sum()
df = df.drop(columns=['Invoice Charge',
                      'Invoice Total Line Haul',
                      'Invoice Total Fuel'])
df = df.merge(df_gb, on='Primary Reference')

# %%
#aggregate invoice number to create a list, and aggregate invoice date to get max
df['Invoice Number'] = df.groupby('Primary Reference')['Invoice Number'].transform(lambda x: ";".join(set(x)))
df['Invoice Date'] = df.groupby('Primary Reference')['Invoice Date'].transform('max')

# %%
#replace 0s with nans to fillna later
df['Invoice Charge'] = df['Invoice Charge'].replace(to_replace=0, value=np.nan)

# %%
#replace carrier total with invoice charge if invoice charge is not na
df['Carrier Total'] = df['Invoice Charge'].fillna(df['Carrier Total'])

# %% [markdown]
# Drop duplicates from the dataframe using the subset of columns and keeping the last ones.

# %%
df.drop_duplicates(subset=['Primary Reference', 'Pinnacle Manifest No', 'Origin Code',
                           'Dest Code','Target Ship (Late)'], inplace=True, keep='last')

# %% [markdown]
# Fill any missing codes with their respective zips.

# %%
df['Origin Code'].fillna(df['Origin Zip'], inplace=True)
df['Dest Code'].fillna(df['Dest Zip'], inplace=True)

# %% [markdown]
# Join Origin Code and Dest Code using " | " to create from_to.<br>
# Join Origin Zip and Dest Zip using ";" to create from_to_zip.<br>
# Join origin code with origin zip and dest code with dest zip using ";".<br>

# %%
df["from_to"] = df["Origin Code"].astype('str') + " | " + df["Dest Code"].astype('str')
df["from_to_zip"] = df["Origin Zip"].astype('str') + ";" + df["Dest Zip"].astype('str')
df["Code;Zip"] = df["Dest Code"].astype('str') + ";" + df["Dest Zip"].astype('str')
df["OriginCode;Zip"] = df["Origin Code"].astype('str') + ";" + df["Origin Zip"].astype('str')

# %% [markdown]
# Convert datetime columns to datetime.

# %%
df["Target Ship (Late)"] = pd.to_datetime(df["Target Ship (Late)"])
df["Target Delivery (Late)"] = pd.to_datetime(df["Target Delivery (Late)"])
df["Invoice Date"] = pd.to_datetime(df["Invoice Date"])
df["Actual Delivery"] = pd.to_datetime(df['Actual Delivery'], errors='coerce')
df['Actual Ship'] = pd.to_datetime(df['Actual Ship'], errors='coerce')

# %% [markdown]
# ### create_lane(load):
# 
# This function uses the Primary Reference (the load ID) to find the ship datetimes, delivery datetimes,<br>
# origin codes + zips, and dest codes + zips against the reference.<br>
# Then, it appends the delivery dates to the ship dates and destinations to origins.<br>
# Then, it sorts them by the dates in an ascending order.<br>
# Then, it loops over the codes, adding them to a list if the current code is not the same as the previous code.<br>
# It also splits the code using ";" to get the code and the zip parts separated.<br>
# Finally, it joins the codes list using " | " and the zips list using ";".<br>
# It then returns the two strings.<br>

# %%
def create_lane(load):
    rows = df.loc[df['Primary Reference']==load, ["Target Ship (Late)", "Target Delivery (Late)",
                                                    "OriginCode;Zip", "Code;Zip"]]
    targets = pd.concat([rows["Target Ship (Late)"], rows["Target Delivery (Late)"]])
    code_zips = pd.concat([rows["OriginCode;Zip"], rows["Code;Zip"]])
    rows = pd.DataFrame({"Target": targets, "Code": code_zips}).sort_values(by="Target")
    l = ['']; li = ['']
    for x in rows["Code"]:
        if l[-1] != x.split(";")[0]:
            l.append(x.split(";")[0])
            li.append(x.split(";")[1])
    l = l[1:]
    li = li[1:]
    lane = " | ".join(l)
    zips = ";".join(li)
    return lane, zips

df['lane'], df['zips'] = zip(*df['Primary Reference'].apply(create_lane))

# %% [markdown]
# ### process_manifest(manifest):
# 
# This function takes the manifest, converts it to str, and splits it by ", ".<br>
# Then, it loops over the list created to process each manifest.<br>
# 
# > If "VEN" is found in the capitalized manifest, "Yes" is appended to the is_vendor_pickup list,<br>
# > the manifest is converted to 0, and '- 53FT' is appended to the size list. Otherwise, "No" is appended.<br>
# > If 'ft' is found in the lower-cased manifest, 7 digits of the manifest are taken, and the whole<br>
# > manifest is taken into the size list. Otherwise, 6 digits are taken, and '- 53FT' is appended.<br>
# 
# Finally, it returns the manifest, size, and is_vendor_pickup lists.<br>
# <br>
# __Note that the sizes of these lists should be the same to avoid problems later when exploding them.__

# %%
def process_manifest(manifest):
    manifest = str(manifest).strip().split(", ")
    size = []
    is_vendor_pickup = []
    for i, j in enumerate(manifest):
        if "VEN" in j.upper() or "PU" in j.upper():
            manifest[i] = 0
            is_vendor_pickup.append("Yes")
            size.append('- 53FT')
        else:
            is_vendor_pickup.append("No")
            if 'ft' in j.lower():
                manifest[i] = ''.join(filter(str.isdigit, j))[:7]
                size.append(j)
            else:
                manifest[i] = ''.join(filter(str.isdigit, j))[:6]
                size.append('- 53FT')
    return manifest, size, is_vendor_pickup

df['Pinnacle Manifest No'], df['size'], df['Vendor Pickup?'] = zip(*df['Pinnacle Manifest No'].apply(process_manifest))


# %% [markdown]
# Explode the three lists generated. This means that the lists are opened up and the elements fill rows.

# %%
df = df.explode(['Pinnacle Manifest No', 'size', 'Vendor Pickup?'])

# %% [markdown]
# Convert the manifest numbers to numeric, coercing any errors.<br>
# Coercing errors means that they're converted to NaN. We fillna with 0,<br>
# and convert the numbers to integers. Finally, we rename the column to 'manifest_num'.<br>

# %%
df['Pinnacle Manifest No'] = pd.to_numeric(df['Pinnacle Manifest No'], errors='coerce'
                                           ).fillna(0).astype(int)
df = df.rename(columns={'Pinnacle Manifest No': 'manifest_num'})

# %% [markdown]
# Show the first 10 rows of the df.

# %%
print(df.head(10))

# %% [markdown]
# Print the shape of the dataframe (rows and columns).

# %%
print(df.shape)

# %% [markdown]
# Create a temporary df with refined manifest_nums, and store it in a csv so that it<br>
# can be accessed by the auto pulling scripts.

# %%
tempdf = pd.DataFrame()
tempdf["manifest_num"] = df["manifest_num"].drop_duplicates().dropna()
tempdf.to_csv(OUTPUT_PATH + "temp.csv", index=False)

# %% [markdown]
# Run the auto pulling scripts to get the Pull Sheet Data and the Closing Tickets.

# %%
subprocess.run(["python", AUTO_PULL_PATH, OUTPUT_PATH], stdout=subprocess.PIPE)
#subprocess.run(["python", AUTO_TICKETS_PATH], stdout=subprocess.PIPE)

# %% [markdown]
# Remove the temporary csv.

# %%
os.remove(OUTPUT_PATH + "temp.csv")

# %% [markdown]
# ### get_leg(list_stops, delim):
# 
# ([A, B, C, A], " | ") => [A | B, B | C, C | A]<br>
# Takes a list of stops and a delimiter to join the stops together using the delimiter.<br>

# %%
def get_leg(list_stops, delim):
    list_legs = []
    if len(list_stops) == 1:
        list_stops.append(list_stops[0])
    for i in range(len(list_stops)-1):
        list_legs.append(list_stops[i] + delim + list_stops[i+1])
    return list_legs

# %%
def find_start_stop(start, stop):
        cross_join = product(start, stop) # cross join the two lists
        path = 99 # default value of path from one index to another (not miles)
        result = () # default value

        for join in cross_join:
            diff = join[1] - join[0]
            # if the current different between two indices is less than path, update path
            if 0 < diff < path:
                path = diff
                result = join

        return result[0], result[1]

# %% [markdown]
# ### revise_from_to(lane, zip_lane, from_to, from_to_zip, load):
# 
# Checks if the from_to is not in the lane and processes it, otherwise<br>
# returns [from_to], [from_to_zip], and [].<br>
# <br>
# Finds the index of the origin and the destination+1 in the lane's stops.<br>
# Then, it creates a list of stops from the origin to destination.<br>
# Then, it uses the get_leg function to convert the list of stops to list of legs.<br>
# Likwise for the zip_lane.<br>
# In case of a ValueError, appends the primary reference to the err list.<br>
# <br>
# Returns revised_from_to, revised_zips, and erronous Primary Reference.<br>
# <br>
# __Note that the length of the two lists, revised and revised_zips, must be the same for explosion.__

# %%
def revise_from_to(lane, zip_lane, from_to, from_to_zip, load):
    #err = []
    revised = [from_to]
    revised_zips = [from_to_zip]
    if from_to not in lane:
        stops = lane.split(" | ")
        zips = zip_lane.split(";")
        try:
            origin, destination = from_to.split(" | ")
            origin_indices = [i for i, x in enumerate(stops) if x == origin]
            dest_indices = [i for i, x in enumerate(stops) if x == destination]
            start, stop = find_start_stop(origin_indices, dest_indices)
            revised_stops = stops[start:stop+1]
            revised_zips_list = zips[start:stop+1]
            revised = get_leg(revised_stops, " | ")
            revised_zips = get_leg(revised_zips_list, ";")
        except (IndexError, ValueError):
            pass
    return revised, revised_zips#, err

#err_df = pd.DataFrame()
df['revised_from_to'], df['revised_zips'] = zip(*df.apply(lambda x: revise_from_to(
    x['lane'], x['zips'], x['from_to'], x['from_to_zip'], x['Primary Reference']), axis=1))

# %% [markdown]
# We explode the revised_from_to and revised_zips' list to access the contents inside.

# %%
df = df.explode(['revised_from_to', 'revised_zips'])

# %%
df['revised_from_to'].fillna('', inplace=True)
df['revised_zips'].fillna('', inplace=True)

# %%
# drop duplicates
df.drop_duplicates(subset=['Primary Reference', 'revised_from_to', 'manifest_num'], inplace=True)

# %%
# Function to count occurrences of search string
def count_occurrences(group):
    search_string = group['revised_from_to'].iloc[0] # get the leg
    if not search_string: # if it is empty return 1
        return 1
    search_string_escaped = re.escape(search_string) # escape special characters
    return group['lane'].str.count(search_string_escaped).max() # return max count per lane

# Apply function to each group
result = df.groupby(['Primary Reference', 'revised_from_to']).apply(count_occurrences)
result.name = 'count_leg'
df = df.merge(result, on=['Primary Reference', 'revised_from_to'], how='left')
del result

# %%
distance_table = pd.read_csv(ZIP_DISTANCE_PATH, dtype={'Zip_Code': str})
print(distance_table.head())

# %%
failed_dist = pd.DataFrame()
failed_dist['zip'] = df.loc[~df['revised_zips'].isin(distance_table['Zip']), 'revised_zips']
failed_dist = failed_dist.drop_duplicates().dropna()

# %%
# split column into multiple columns by delimiter
if len(failed_dist.index) > 0:
    failed_dist[['zip1', 'zip2']] = failed_dist['zip'].str.extract(r'([^;]*);(.*)', expand=True)
    failed_dist.dropna(how='any', inplace=True)
    failed_dist.drop(columns=['zip'], inplace=True)
    failed_dist.to_csv(OUTPUT_PATH+'temp_dist.csv', index=False)
    subprocess.Popen(["python", AUTO_DIST_PATH]).wait()
    os.remove(OUTPUT_PATH + "temp_dist.csv")

# %%
def get_distance(zips):
    try:
        # both Zip and leg should be strings
        dist = distance_table.loc[distance_table['Zip'] == zips, 'Distance'].iloc[0]
        failed_dist = None
    except:
        dist = 0
        failed_dist = zips
    return dist, failed_dist

exfailed_dist = pd.DataFrame()
df["zip_distance"], exfailed_dist['exdist'] = zip(*df['revised_zips'].apply(get_distance))

# %%
df['count_leg'] = df['count_leg'].clip(lower=1)

# calculate adjusted distance for each row
df['Adjusted Distance'] = df['zip_distance'] * df['count_leg']

# group by Primary Reference and transform the sum of adjusted distance
load_distance = df.drop_duplicates(['Primary Reference', 'revised_from_to']).groupby(['Primary Reference'])[
    'Adjusted Distance'].sum().reset_index(name='Load Total Distance')

df = pd.merge(df, load_distance, on=['Primary Reference'])

# reset index
df.reset_index(drop=True, inplace=True)

# %%
# calculate number of legs per load
legs_per_load = df.groupby(['Primary Reference']).revised_from_to.nunique().reset_index(name='Legs per Load')

# merge manifests_per_leg with fact_table
df = pd.merge(df, legs_per_load, on=['Primary Reference'])

# allocate leg cost to manifests
df['Leg Cost'] = np.where(df['Load Total Distance'] <= 0,
                          df['Carrier Total'] / df['Legs per Load'].clip(lower=1),
                          df['Carrier Total'] * df['Adjusted Distance'] / df['Load Total Distance'])

# %%
print("\nGetting the POMs...")
all_files = glob(os.path.join(ORDERS_PATH, "*.csv"))
df_poms = pd.concat((pd.read_csv(f, encoding='ISO-8859-1', engine='c', dtype={'Manifest No': object,
                                                                              'OrderNo': object},
                                usecols=['Manifest No', 'OrderNo', 'Weight', 'Cu_Ft_']
                                ) for f in all_files), ignore_index=True).drop_duplicates().round(2)
df_pomsg = df_poms.groupby(['Manifest No', 'OrderNo'], as_index=False).agg({'Cu_Ft_': 'sum', 'Weight': 'sum'})
df_pomsg['Count'] = df_poms.groupby(['Manifest No', 'OrderNo'], as_index=False).size().loc[:, 'size']
df_poms = df_pomsg.set_index('OrderNo')

# %%
print("\nGetting Closing Tickets...")
# get the CSVs in the main tickets folder 
tp_files = glob(os.path.join(TICKETS_PATH, "*.csv"))
# create a dataframe using the CSVs
df_tp = pd.concat(
    (pd.read_csv(f, encoding='ISO-8859-1', engine='c', dtype={'Order #': object},
                               usecols=[
                                   'Order #',
                                   'Order Status',
                                   'Actual Delivery Date',
                                   'First Offered Date',
                                   'Client Name',
                                   'PickupHub',
                                   'DeliveryHub',
                                   'Amount',
                                   'Approval Value'
                                ]
    ) for f in tp_files), ignore_index=True).round(2)
# get the CSVs in the other tickets folder (the linehaul_poms one)
ot_files = glob(os.path.join(OTHER_TICKETS, "*.csv"))
# create a dataframe using the CSVs
df_ot = pd.concat(
    (pd.read_csv(f, encoding='ISO-8859-1', engine='c', dtype={'Order #': object},
                                usecols=[
                                   'Order #',
                                   'Order Status',
                                   'Actual Delivery Date',
                                   'First Offered Date',
                                   'Client Name',
                                   'PickupHub',
                                   'DeliveryHub',
                                   'Amount',
                                   'Approval Value'
                                ]
    ) for f in ot_files), ignore_index=True).round(2)
# append the two dataframes, taking only those orders from main folder which are
#   not appearing in the linehaul_poms one
df_kpi = pd.concat(
    [ df_tp.loc[~df_tp["Order #"].isin(df_ot["Order #"])], df_ot ],
    ignore_index=True
)
# get rid of the temp dfs made
del df_tp
del df_ot
# groupby Order #
df_kpi = df_kpi.groupby('Order #').agg(
    {
        'Order Status': 'last',
        'Actual Delivery Date': 'last',
        'First Offered Date': 'last',
        'Client Name': 'last',
        'PickupHub': 'last',
        'DeliveryHub': 'last',
        'Amount': 'sum',
        'Approval Value': 'last'
    }
)

# %%
print("\nMerging df_poms and closing_tickets...")
df_poms = df_poms.join(df_kpi)
df_poms.index.names = ['Order #']
df_poms.reset_index(inplace=True)
#print(df_poms.shape)

# %%
print("\nMerging fact_table and df_poms...")
df_poms['Manifest No'] = df_poms['Manifest No'].fillna(0).astype(int)
df_poms.set_index('Manifest No', inplace=True)
df.set_index('manifest_num', inplace=True)
fact_table = df.join(df_poms)
fact_table.index.names = ['manifest_num']
fact_table.reset_index(inplace=True)
#print(fact_table.shape)

# %%
fact_table['Order Delivery Date'] = (pd.TimedeltaIndex(fact_table['Actual Delivery Date'
                                            ], unit='d') + datetime(1899, 12, 30)).strftime('%Y-%m-%d')
fact_table.drop(columns=['Actual Delivery Date'], inplace=True)


# %%
fact_table['Manifest Cubes'] = fact_table.groupby(['Primary Reference', 'revised_from_to', 'manifest_num'])['Cu_Ft_'].transform('sum')

# %%
fact_table['Leg Cubes'] = fact_table.groupby(['Primary Reference', 'revised_from_to'])['Cu_Ft_'].transform('sum')


# %%
# calculate number of manifests per leg
manifests_per_leg = fact_table.groupby(['Primary Reference', 'revised_from_to']).manifest_num.nunique().reset_index(name='Manifests Per Leg')

# merge manifests_per_leg with fact_table
fact_table = pd.merge(fact_table, manifests_per_leg, on=['Primary Reference', 'revised_from_to'])

# allocate leg cost to manifests
fact_table['Manifest Cost'] = np.where(fact_table['Leg Cubes'] <= 0,
                                       fact_table['Leg Cost'] / fact_table['Manifests Per Leg'].clip(lower=1),
                                       fact_table['Leg Cost'] * fact_table['Manifest Cubes'] / fact_table['Leg Cubes'])


# %%
# calculate number of orders per manifest
orders_per_manifest = fact_table.groupby(['Primary Reference', 'revised_from_to', 'manifest_num'])['Order #'].nunique().reset_index(name='Orders Per Manifest')

# merge orders_per_leg with fact_table
fact_table = pd.merge(fact_table, orders_per_manifest, on=['Primary Reference', 'revised_from_to', 'manifest_num'])

# allocate manifest cost to poms
fact_table['POM Cost'] = np.where(fact_table['Manifest Cubes'] <= 0,
                                  fact_table['Manifest Cost'] / fact_table['Orders Per Manifest'].clip(lower=1),
                                  fact_table['Manifest Cost'] * fact_table['Cu_Ft_'] / fact_table['Manifest Cubes'])

# %%
fact_table = fact_table.rename(columns={'revised_from_to': 'Leg', 'revised_zips': 'Leg Zips',
                              'count_leg': 'Leg Instance per Load', 'Cu_Ft_': 'Order Cubes',
                              'zip_distance': 'Actual Distance'})

#%%
fact_table = fact_table.drop(
    columns=[
        "Code;Zip",
        "OriginCode;Zip"
    ]
)

# %%
if os.path.exists(OUTPUT_PATH + "Product.csv"):
   old_data = pd.read_csv(OUTPUT_PATH + "Product.csv")
   references = set(fact_table["Primary Reference"])
   old_data = old_data[~old_data["Primary Reference"].isin(references)]
   final = pd.concat([old_data, fact_table])
else:
   final = fact_table

# %%
final.to_csv(OUTPUT_PATH + "Product.csv", index=False)

# %%
for file_name in xlsx_files:
    os.remove(file_name)
