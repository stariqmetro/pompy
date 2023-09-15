import re
import subprocess
from datetime import datetime
from glob import glob
import os
from itertools import product
import numpy as np

import pandas as pd

#curr_dir
import process_lane as pl
#import revise_fromto as rv
import allocate as al

#paths
MANIFEST_DETAILS_PATH = "C:/OneDrive - Metropolitan Warehouse/Vendor Control/Data Files/POM Level/ManifestCSV/"
DF_POMS_PATH = "C:/OneDrive - Metropolitan Warehouse/Vendor Control/Data Files/POM Level/Pull Sheet Data LH Team"
KPI_PATH = "C:/OneDrive - Metropolitan Warehouse/Vendor Control/Data Files/KPI Reports - 2/POMS in KPI Reports"
OTHER_TICKETS = "C:/OneDrive - Metropolitan Warehouse/Vendor Control/Data Files/KPI Reports - 2/linehaul_poms"
PRODUCT_BASE_PATH = "C:/OneDrive - Metropolitan Warehouse/Vendor Control/Data Files/POM Level/Product/{0}--"
AUTO_PULL_PATH = "C:/OneDrive - Metropolitan Warehouse/Vendor Control/Data Files/POM Level/pompy/get_missing_pull_sheets.py"
#AUTO_TICKETS_PATH = "C:/OneDrive - Metropolitan Warehouse/Vendor Control/Data Files/POM Level/pompy/get_missing_closing_tickets.py"

#Get the Pull Sheet Data and the Closing Tickets first
#subprocess.Popen(["python", AUTO_PULL_PATH, MANIFEST_DETAILS_PATH]).wait()
#subprocess.Popen(["python", AUTO_TICKETS_PATH]).wait()

try:
    print("Getting Manifest Details...\n")
    report_files = glob(os.path.join(MANIFEST_DETAILS_PATH, "*.csv"))
    fact_table = pd.concat((pd.read_csv(f, dtype={"cost1": "float64", "cost2": "float64"}) for f in report_files), ignore_index=True)
    print(fact_table.head())
except FileNotFoundError:
    input("No unprocessed file found in ManifestCSV\
          \nPress Enter to continue...")
    exit(0)

print("\nGetting legs...")
exc_stop = pd.DataFrame()
fact_table["Legs"],\
    fact_table["is_standard"], exc_stop["Excluded"] = zip(*fact_table["lane1"].apply(pl.get_legs))
fact_table["Legs2"],\
    fact_table["is_standard2"], exc_stop["Excluded2"] = zip(*fact_table["lane2"].apply(pl.get_legs))

print("\nPreparing a manifest_table with unique keys...\n")
manifest_table = fact_table.drop(columns=['cost1','vendor1','cost2','vendor2','StartDate',
                                          "is_standard", "is_standard2"])
manifest_table1 = manifest_table.loc[:, manifest_table.columns != 'key2']
manifest_table1 = manifest_table1.rename(columns={'key1': 'key'})
manifest_table1.dropna(subset=['key'], inplace=True)
manifest_table2 = manifest_table.loc[:, manifest_table.columns != 'key1']
manifest_table2= manifest_table2.rename(columns={'key2': 'key'})
manifest_table2.dropna(subset=['key'], inplace=True)
manifest_table = pd.concat([manifest_table1, manifest_table2])
manifest_table['PublishedDate'] = pd.to_datetime(manifest_table['PublishedDate'])
err_keys = manifest_table.loc[manifest_table['PublishedDate'].isnull(), 'key']
manifest_table.dropna(subset=['manifest_num', 'PublishedDate'], inplace=True)
manifest_table.reset_index(inplace=True, drop=True)
print(manifest_table.shape)

del manifest_table1
del manifest_table2

print('\nPreparing a fact_table with unique keys...\n')
fact_table1 = fact_table.loc[:, ["cost1",'vendor1','lane1','key1',"Legs", "is_standard"]]
fact_table1.rename(columns={"cost1": 'cost', "vendor1": 'vendor', "lane1": 'lane',
                            "key1": 'key', "Legs": 'leg'},
                   inplace=True)
fact_table1.dropna(subset=['key'])
fact_table2 = fact_table.loc[:, ['cost2','vendor2','lane2','key2',"Legs2", "is_standard2"]]
fact_table2.rename(columns={"cost2": 'cost', "vendor2": 'vendor', "lane2": 'lane',
                            "key2": 'key', "Legs2": 'leg', "is_standard2": "is_standard"},
                   inplace=True)
fact_table2.dropna(subset=['key'], inplace=True)
fact_table = pd.concat([fact_table1, fact_table2])
fact_table.reset_index(inplace=True, drop=True)
fact_table.drop(fact_table[fact_table['key'].isin(err_keys)].index, inplace=True)
fact_table = fact_table.loc[fact_table['key'].astype(str, errors='ignore').drop_duplicates().index]

failed_lane = fact_table.loc[fact_table['leg'].map(len) == 0].astype(str, errors='ignore')
failed_lane.drop_duplicates(subset=["lane"], inplace=True)

fact_table = fact_table.loc[fact_table.explode('leg').dropna(subset=['leg']).index]
print(fact_table.shape)

del fact_table1
del fact_table2

print("\nGetting zips...")
err_zip =  pd.DataFrame()
fact_table["Zips"], err_zip["Errors"] = zip(*fact_table["leg"].apply(pl.get_zips))
print("\nGetting distances...")
err_dist = pd.DataFrame()
fact_table["Leg Distance"], err_dist["Errors"] = zip(*fact_table["Zips"].apply(pl.get_distance))


def revise_fromto(row):
    """(A | C) -> [A | B, B | C]"""
    from_to = row['from_to'].split(" | ")
    for i in range(len(from_to)):
        if from_to[i] == "CA-OS":
            from_to[i] = "CA-EV"
        elif from_to[i] in ["NC-CH", "NC-EH", "NC-HP"]:
            from_to[i] = "NC-SD"
        elif from_to[i] in ["OH-CN", "OH-CO"]:
            from_to[i] = "OH-DY"

    origin = from_to[0]
    destination = from_to[1]
    legs1 = row["Legs"]
    legs2 = row["Legs2"]
    stops_legs1 = [stop for leg in legs1 for stop in leg.split(" | ")]
    stops_legs2 = [stop for leg in legs2 for stop in leg.split(" | ")]
    revised = []
    leg = ""
    dist_list = [] # list of distances of legs1
    dist_list2 = [] # list of distances of legs2
    special_cases = ["CA-EV", "NC-SD", "OH-DY"]
    
    #if any from_to is in special cases and has first equal to second, let it be
    if all((from_to[0] == from_to[1], len([x for x in from_to if x in special_cases])>0)):
        revised = from_to
    
    try:
        dist_list = fact_table.loc[fact_table['leg'].apply(lambda x: x==legs1), "Leg Distance"].iloc[0]
        dist_list2 = fact_table.loc[fact_table['leg'].apply(lambda x: x==legs2), "Leg Distance"].iloc[0]
    except IndexError:
        pass

    total_dist = sum(dist_list + dist_list2)

    def find_start_stop(start, stop):
        cross_join = product(start, stop) # cross join the two lists
        path = 99 # default value of path from one index to another (not miles)
        result = () # default value

        for join in cross_join:
            diff = join[1] - join[0]
            # if the current difference between two indices is less than path, update path
            if 0 < diff < path:
                path = diff
                result = join

        return result[0], result[1]

    try:
        # only revise the from_to if all of the following four conditions hold true
        if all((row["from_to"] not in legs1,
                row["from_to"] not in legs2,
                len(revised) == 0,
                total_dist != 0)):

            if any((origin not in (stops_legs1 + stops_legs2),
                destination not in (stops_legs1 + stops_legs2),
                origin == destination)):
                pass

            # if both origin and destination are found in a lane,
            # revise from_to to be from origin to destination
            elif all(x in stops_legs1 for x in [origin, destination]):
                start_i = [i for i,j in enumerate(stops_legs1) if j == origin] # find all indices of origin
                stop_i = [i for i,j in enumerate(stops_legs1) if j == destination] # find all indices of destination
                start, stop = find_start_stop(start_i, stop_i)
                revised = stops_legs1[start:stop+1]

            elif all(x in stops_legs2 for x in [origin, destination]):
                start_i = [i for i,j in enumerate(stops_legs2) if j == origin]
                stop_i = [i for i,j in enumerate(stops_legs2) if j == destination]

                start, stop = find_start_stop(start_i, stop_i)
                revised = stops_legs2[start:stop+1]

            elif destination in stops_legs2:
                start = stops_legs1.index(origin)
                stop = stops_legs2.index(destination)

                common_list = list(set(stops_legs1[start:]).intersection(stops_legs2[:stop+1]))

                start_i = [i for i,j in enumerate(stops_legs1) if j == origin]
                stop_i = [i for i,j in enumerate(stops_legs2) if j == destination]

                common1_i = [i for i,j in enumerate(stops_legs1) if j == common_list[0]]
                common2_i = [i for i,j in enumerate(stops_legs2) if j == common_list[0]]

                start, common1 = find_start_stop(start_i, common1_i) # find shortest path from start to common
                common2, stop = find_start_stop(common2_i, stop_i) # find shortest path from common to stop

                revised = stops_legs1[start:common1+1]+stops_legs2[common2:stop+1]

            elif destination in stops_legs1:
                start = stops_legs2.index(origin)
                stop = stops_legs1.index(destination)

                common_list = list(set(stops_legs2[start:]).intersection(stops_legs1[:stop+1]))

                start_i = [i for i,j in enumerate(stops_legs2) if j == origin]
                stop_i = [i for i,j in enumerate(stops_legs1) if j == destination]

                common1_i = [i for i,j in enumerate(stops_legs1) if j == common_list[0]]
                common2_i = [i for i,j in enumerate(stops_legs2) if j == common_list[0]]

                start, common2 = find_start_stop(start_i, common2_i) # find shortest path from start to common
                common1, stop = find_start_stop(common1_i, stop_i) # find shortest path from common to stop
                
                revised = stops_legs2[start:common2+1]+stops_legs1[common1:stop+1]     
        # if from_to is found in a lane, let it be as it is
        else:
            revised = from_to
    except:
        #print("except...", row["from_to"], row["manifest_num"], e)
        pass
    
    combined_list = []
    for i in range(len(revised)-1):
        if (revised[i] != revised[i+1])\
            or (revised[i] in special_cases):
            # try joining the next stop to this one to create a leg
            leg = revised[i]+" | "+revised[i+1]
        else:
            continue
        combined_list.append(leg)
    
    if "LOCAL MOVEMENT" in (stops_legs1 + stops_legs2):
        combined_list = ["LOCAL MOVEMENT"]
    
    ##combined_str = ",".join(combined_list)
    return combined_list

print("\nRevising from_to...")
manifest_table["revised from_to"] = manifest_table.apply(revise_fromto, axis=1)

print("\nDropping rows with erronous from_to...")
err_revised = manifest_table.loc[manifest_table['revised from_to'].map(len) == 0]
err_revised = err_revised.drop(columns=["Legs", "Legs2", "revised from_to"])
err_keys = manifest_table.loc[manifest_table['revised from_to'].map(len) == 0, 'key']
fact_table.drop(fact_table.loc[fact_table['key'].isin(err_keys)].index, inplace=True)
manifest_table.drop(manifest_table[manifest_table['revised from_to'].map(len) == 0].index, inplace=True)

def get_manifests(row):
    """Gets the manifests that ran in the current leg."""
    list_legs = row["leg"]
    curr_key = row["key"]
    
    # find all the manifests against the current key
    list_manifests = manifest_table.loc[manifest_table["key"] == curr_key, 'manifest_num'].tolist()
    
    legs_manifests = [] # list of legs(list of manifests) of current key
    
    for leg in list_legs:
        leg_manifests = [] # list of manifests that ran on the current leg
        
        for manifest in list_manifests:
            # find all the legs the current manifest ran on
            manifest_legs = manifest_table.loc[manifest_table['manifest_num'] == manifest,\
                                           'revised from_to'].iloc[0]
            if leg in manifest_legs:
                leg_manifests.append(manifest)
        
        # if no manifest ran on current leg, set manifest = 0
        if len(leg_manifests) == 0:
            leg_manifests = [0]
        
        legs_manifests.append(leg_manifests)
    
    return legs_manifests

print("\nFinding manifests against legs...")
fact_table["Leg Manifest"] = fact_table.apply(get_manifests, axis=1)

print("\nCleaning up the manifest_table...")
manifest_table_copy = manifest_table.copy()
manifest_table.drop_duplicates(subset=["manifest_num"], inplace=True, ignore_index=True)
manifest_table.drop(columns=["key", "EntryNo", "Legs", "Legs2", "lane1", "lane2"], inplace=True)
manifest_table['revised from_to'] = manifest_table['revised from_to'].map(lambda x: ";".join(x))

print("\nExploding columns...")
explode_columns = ['leg', 'is_standard', 'Zips', 'Leg Distance', 'Leg Manifest']
fact_table = fact_table.explode(explode_columns, ignore_index=True)
#fact_table['count_leg'] = fact_table.groupby(["key", "leg"]).transform('size') # count leg instances per key
fact_table = fact_table.explode("Leg Manifest", ignore_index=True)
print(fact_table.shape)

print("\nGetting Leg Cost...")
manifest_table['revised from_to'].fillna('', inplace=True)
fact_table['leg'].fillna('', inplace=True)
fact_table.drop_duplicates(subset=['key', 'leg', 'Leg Manifest'], inplace=True)

# Function to count occurrences of search string
def count_occurrences(group):
    search_string = group['leg'].iloc[0] # get the leg
    if not search_string: # if it is empty return 1
        return 1
    search_string_escaped = re.escape(search_string) # escape special characters
    return group['lane'].str.count(search_string_escaped).max() # return max count per lane

# Apply function to each group
result = fact_table.groupby(['key', 'leg']).apply(count_occurrences)
result.name = 'count_leg'
fact_table = fact_table.merge(result, on=['key', 'leg'], how='left')
del result

# clip count_leg to 1
fact_table['count_leg'] = fact_table['count_leg'].clip(lower=1)
# calculate adjusted distance for each row
fact_table['Adjusted Distance'] = fact_table['Leg Distance'] * fact_table['count_leg']
# group by Primary Reference and transform the sum of adjusted distance
load_distance = fact_table.drop_duplicates(['key', 'leg']).groupby(['key'])[
    'Adjusted Distance'].sum().reset_index(name='Load Total Distance')

fact_table = pd.merge(fact_table, load_distance, on=['key'])

# reset index
fact_table.reset_index(drop=True, inplace=True)

# calculate number of legs per load
legs_per_load = fact_table.groupby(['key']).leg.nunique().reset_index(name='Legs per Load')
# merge manifests_per_leg with fact_table
fact_table = pd.merge(fact_table, legs_per_load, on=['key'])
fact_table['Adjusted Distance'] = fact_table['Adjusted Distance'].astype('float64')
fact_table['Load Total Distance'] = fact_table['Load Total Distance'].astype('float64')
# allocate leg cost to manifests
fact_table['Leg Cost'] = np.where(fact_table['Load Total Distance'] > 0,
                          fact_table['cost'] * (fact_table['Adjusted Distance'] / fact_table['Load Total Distance']),
                          fact_table['cost'] / (fact_table['Legs per Load'].clip(lower=1)))

print("\nMerging fact_table and manifest_table...")
fact_table.set_index('Leg Manifest', inplace=True)
manifest_table.set_index('manifest_num', inplace=True)
fact_table = fact_table.join(manifest_table)
print(fact_table.shape)

print("\nFixing date...")
def fix_date(key, date):
    """Finds the max date against a key to fill na dates."""
    
    if pd.isnull(date):
        fixed_date = manifest_table_copy.loc[manifest_table_copy['key'] == key, 'PublishedDate'].max()
        return fixed_date

    return date
    
fact_table['PublishedDate'] = fact_table.apply(lambda row: fix_date(row['key'], row['PublishedDate']), axis=1)

print("\nGetting the POMs...")
all_files = glob(os.path.join(DF_POMS_PATH, "*.csv"))
df_poms = pd.concat((pd.read_csv(f, encoding='ISO-8859-1', engine='c', dtype={'OrderNo': str}, usecols=['Manifest No',
                    'OrderNo', 'Weight', 'Cu_Ft_']
                    ) for f in all_files), ignore_index=True).round(2) #Manifest No
df_pomsg = df_poms.groupby(['Manifest No', 'OrderNo'], as_index=False).agg({
                                                           'Cu_Ft_': 'sum',
                                                           'Weight': 'sum'})
df_pomsg['Count'] = df_poms.groupby(['Manifest No', 'OrderNo'], as_index=False).size().loc[:, 'size']
df_poms = df_pomsg.set_index('OrderNo')

print("\nGetting Closing Tickets...")
kpi_files = glob(os.path.join(KPI_PATH, "*.csv")) + glob(os.path.join(OTHER_TICKETS, "*.csv"))
df_kpi = pd.concat((pd.read_csv(f, encoding='ISO-8859-1', engine='c', dtype={'Order #': str}, usecols=[
    'Order #',
    'Order Status',
    'Actual Delivery Date',
    'First Offered Date',
    'Client Name',
    'Amount',
    'Approval Value']
    ) for f in kpi_files), ignore_index=True).round(2)
df_kpi = df_kpi.groupby('Order #').agg({
    'Order Status': 'last',
    'Actual Delivery Date': 'last',
    'First Offered Date': 'last',
    'Client Name': 'last',
    'Amount': 'sum',
    'Approval Value': 'last'}
    )

print("\nMerging df_poms and closing_tickets...")
df_poms = df_poms.join(df_kpi)
df_poms.index.names = ['Order #']
df_poms.reset_index(inplace=True)
print(df_poms.shape)

print("\nMerging fact_table and df_poms...")
df_poms.set_index('Manifest No', inplace=True)
fact_table = fact_table.join(df_poms)
fact_table.index.names = ['manifest_num']
fact_table.reset_index(inplace=True)
print(fact_table.shape)

fact_table['PublishedDate'] = fact_table['PublishedDate'].astype(str, errors='ignore', copy=False)
fact_table['Actual Delivery Date'] = (pd.TimedeltaIndex(fact_table['Actual Delivery Date'
                                                            ], unit='d') + datetime(1899, 12, 30)).strftime('%Y-%m-%d')

def write_to_csv(df, df_name):
    """Writes the dfs to csv, also explodes, drops_duplicates, and drops_na."""
    base_path = PRODUCT_BASE_PATH.format(datetime.today().strftime('%Y-%m-%d--%H-%M-%S'))
    
    explode_list = ["ErrorDistance", "ErrorZip", "ExcludedStops"]
    
    #open up the lists
    if df_name in explode_list:
        df = df.explode(list(df.columns), ignore_index=True)
    
    #drop duplicates
    df = df.drop_duplicates()
    
    #drop na
    df = df.dropna(how="all")
    
    if len(df.index) != 0:
        df.to_csv(base_path+df_name+".csv", index=False)

series_1 = pd.Series(exc_stop["Excluded"])
series_2 = pd.Series(exc_stop["Excluded2"])
exc_stop = pd.DataFrame(pd.concat([series_1, series_2], ignore_index=True), columns=["Excluded"])
del series_1, series_2

list_tables = [fact_table, err_revised, err_dist, err_zip, exc_stop, failed_lane]
list_names = ["Processed", "ErrorRevised", "ErrorDistance", "ErrorZip", "ExcludedStops", "FailedLane"]

print("\nWriting to csv...")
for table, name in zip(list_tables, list_names):
    write_to_csv(table, name)

#print("\nMarking ManifestCSV file as processed...")
#rename("ManifestCSV/ManifestDetails.csv", "ManifestCSV/Processed/ManifestDetails--{0}.csv"\
#    .format(datetime.today().strftime('%Y-%m-%d--%H-%M-%S')))

print("\nDone.")
input("\nPress Enter to continue...")
