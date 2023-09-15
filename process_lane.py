import pandas as pd

print("\nGetting Translation Table...\n")
unique_stops = pd.read_csv("../Helpers/StopsTranslation.csv", dtype=str)
print(unique_stops.head())

print("\nGetting Distance Table...\n")
distance_table = pd.read_csv("../Helpers/ZipDistances.csv", dtype={'Zip': str})
print(distance_table.head())

def translate(stops):
    translated_stops = []
    is_standard_list = []
    excluded_stops = []
    
    # iterate over each stop
    for stop in stops:
        if stop == "nan":
            translated_stop = "nan"
            translated_stops.append(translated_stop)
            is_standard_stop = "0"
            is_standard_list.append(is_standard_stop)
        else:
            try:
                translated = unique_stops.loc[unique_stops['Stop'] == stop.strip(), 'Translated']
                is_standard = unique_stops.loc[unique_stops['Stop'] == stop.strip(), 'Bool']
                # try to get the translated stop against the current stop from the unique_stops table
                if len(translated) > 0:
                    translated_stop = translated.iloc[0]
                    translated_stops.append(translated_stop)
                    is_standard_stop = is_standard.iloc[0]
                    is_standard_list.append(is_standard_stop)
                elif stop not in excluded_stops:
                    excluded_stops.append(stop)
            except Exception as e:
                print("Error translating...", e)
    
    return translated_stops, is_standard_list, excluded_stops

def get_legs(lane):
    
    #split the lane into its stops using the "-" link
    stops = str(lane).split("-")
    leg = "nan"
    
    # translate the stops first...
    translated_stops, is_standard_list, excluded_stops = translate(stops)
    
    combined_list = []
    bool_list = []
    
    #special cases for harmonization
    special_cases = ["OH-DY", "CA-EV", "NC-SD"]
    
    # then combine the stops back into legs...
    for i in range(len(translated_stops)-1):
        if (translated_stops[i] != translated_stops[i+1])\
            or (translated_stops[i] in special_cases):
            # try joining the next stop to this one to create a leg
            leg = translated_stops[i]+" | "+translated_stops[i+1]
        else:
            continue
        
        # if either of the two stops is non-standard, append "0", else "1"
        if "0" in [is_standard_list[i], is_standard_list[i+1]]:
            bool_list.append("0")
        else:
            bool_list.append("1")
        
        # add the leg to the list of legs
        combined_list.append(leg)
    # ...then combine the stops back into legs
    
    if len(set(['LOCAL MOVEMENT', 'nan']).intersection(set(translated_stops))) > 0:
        combined_list = translated_stops
    
    if len(bool_list) == 0:
        bool_list = ['0']
    
    return combined_list, bool_list, excluded_stops

def get_zips(legs):

    ##list_legs = str(legs).split(",")
    list_legs = legs
    list_zips = []
    failed_zips = []

    for leg in list_legs:

        codes = leg.split(" | ")
        zips = []

        for code in codes:
            try:
                zip_code = str(unique_stops.loc[unique_stops['Translated'] == code.strip(),
                                                'Post'].iloc[0]).lstrip('0') # lstrip is to remove leading 0s
                zips.append(zip_code)
            except:
                if(len(code) != 0):
                    print("Failed zip: ", code, "\n Setting zip to 00000.")
                    zip_code = "00000"
                    zips.append(zip_code)
                    failed_zips.append(code)
            finally:
                pass
        
        combined_zip = ";".join(zips)
        list_zips.append(combined_zip)
    
    ##zips_str = ",".join(list_zips)
    #failed_str = ",".join(failed_zips)

    return list_zips, failed_zips

def get_distance(zips):
    
    ##list_zips = str(zips).split(",")
    list_zips = zips
    special_cases = ["91752;91752", "45424;45424", "27263;27263"]
    list_dist = []
    failed_dist = []
    
    for leg_ in list_zips:
        
        #leg = leg_.replace(";", "")
        
        try:
            # both Zip and leg should be strings
            dist = distance_table.loc[distance_table['Zip'] == leg_, 'Distance'].iloc[0]
            list_dist.append(dist)
        except:
            if(len(leg_) != 0):
                dist = 0
                list_dist.append(dist)
                if 'nan' not in leg_ and leg_ not in special_cases:
                    failed_dist.append(leg_)
        finally:
            pass
        
    ##dist_str = ",".join(list_dist)
    ##failed_str = ",".join(failed_dist)
    
    return list_dist, failed_dist
    ##return dist_str, failed_dist
