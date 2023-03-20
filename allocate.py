# -*- coding: utf-8 -*-
"""
Created on Mon Mar 13 09:49:50 2023

@author: Shahmir.Tariq
"""

def leg_cost(row):
    list_legs = row["leg"]
    list_dist = row["Leg Distance"]
    total_dist = sum(list_dist)
    total_cost = row["cost"]
    list_cost = []
    
    for i in range(len(list_legs)):
        # find the count of current leg in the list of legs and multiply cost by this amount
        #as the duplicates will be removed, and so the cost will not add up if we don't multiply
        try:
            if total_dist == 0:
                cost = round(total_cost / len(list_legs), 2) # divide leg cost equally over legs
            else:
                cost = round((list_dist[i]/total_dist) * total_cost\
                             * list_legs.count(list_legs[i]), 2)
        except:
            print("Error getting leg cost of leg: "+str(list_legs[i]))
        finally:
            list_cost.append(cost)
        
    return list_cost

def get_manifests(row, manifest_table):
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