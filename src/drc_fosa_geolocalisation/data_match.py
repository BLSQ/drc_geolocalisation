from shapely.geometry import LineString, Polygon, Point
import geopandas as gpd
import pandas as pd
import matplotlib.colors as colors
import numpy as np
from fuzzywuzzy import process


def love_machine(source_1, source_2, source_2_name, province=None, zone=None):
    """ Finds matches between health facility names using a 2-step fuzzy matching process.
    First we check for an exact match. If not found, fuzzymatch source 1 facility name against all
    available source 2 facility names. Then take that result (lover 1) and fuzzymatch against
    source 1 names. If the resulting fosa name (lover 2) is the same as the original input name,
    make a match. Otherwise keep processing.
    
    If province and/or zone passed as argument, only match on those subsets of data.
    
    inputs
        source_1: [data frame] data to match to. usually the SNIS data
        source_2: [data frame] data to match against
        source_2_name: [string] name of source 2, used for naming ouptut columns
        province: [string] name of province to subset data, if available
        zone: [string] name of zone to subset data, if available
        
    outputs
        [data frame] containing matched health facilities and columns metadata for each source
    
    """
    
    out = pd.DataFrame()
    match = 0
    
    snis_names = source_1.fosa_name
    snis_names_indexed=names_to_dict(snis_names)
    unmatched_snis_names=snis_names.copy()
    
    # logic to set up variations of love machine matching
    province_match = 'no'
    zone_match = 'no'
    
    if province and zone:
        source_2_names = source_2.loc[(source_2.province == province) & (source_2.zone == zone), 'fosa_name']
        province_match = 'yes'
        zone_match = 'yes'
        
    elif province and not zone:
        source_2_names = source_2.loc[(source_2.province == province), 'fosa_name']
        province_match = 'yes'
        
    elif zone and not province:
        source_2_names = source_2.loc[(source_2.zone == zone), 'fosa_name']
        zone_match = 'yes'
        
    else:
        source_2_names = source_2.fosa_name
    
    
    source_2_names_indexed = names_to_dict(source_2_names)
    
    matched_snis = pd.DataFrame()
    matched_source_2 = pd.DataFrame()
    
    for row in snis_names:
        if source_2_names_indexed.get(row) and len(source_2_names_indexed.get(row))==1 \
        and snis_names_indexed.get(row) and len(snis_names_indexed.get(row))==1:
            
            lover_1=(row,100,)
            lover_2=(row,100,)
        
        else:
            # process_extract returns tuple: names and score
            # When we choose lover_2[0], we then choose to only take the name and the score
            lovers_1 = process.extract(row, source_2_names, limit=1)  
            lover_1 = lovers_1[0] if len(lovers_1)>0 else None


        if lover_1 is not None:
            lovers_2 = process.extract(lover_1[0], unmatched_snis_names, limit=1)
            lover_2 = lovers_2[0]                
            if lover_2[0] == row:
                
                #unmatched_snis_names = unmatched_snis_names.drop(row,inplace=True)
                matched_snis = source_1[source_1.fosa_name == row].reset_index(drop=True)
                matched_snis["match_id"] = match
                matched_source_2 = source_2[(source_2.fosa_name == lover_1[0])].reset_index(drop=True)
                matched_source_2["match_id"] = match
                matched = matched_source_2.merge(matched_snis, 
                                              left_on = ["match_id"], right_on = ["match_id"], 
                                              suffixes=["_%s" % source_2_name ,"_snis"])
                matched["name_distance_%s" % source_2_name] = lover_1[1]
                matched['province_match_%s' % source_2_name] = province_match
                matched['zone_match_%s' % source_2_name] = zone_match
                matched = matched.drop_duplicates('ID_snis')
                
                match = match + 1
                
                out = out.append(matched, sort = False)
                
    return out.reset_index()  


def unique_orgunit_id_name(cleaned_df_dict, source, orgunit): ### formerly df_cleaned_orgunit_id_name
    """ Takes a cleaned IASO df and returns a df of unique hierarchy values and their ID
    inputs
        cleaned_df_dict: dictionary of cleaned source data
        souce: name of source
        orgunit: level of hierarchy to return
        
    output
        df with two columns - unique values for that orgunit and the corresponding ID number
        returns none if orgunit not in hierarchy 
    """
    df_source = cleaned_df_dict[source]
    if orgunit in df_source.columns:
        df_orgunit_source = df_source[[orgunit + '_id', orgunit]]
        df_orgunit_source = df_orgunit_source.drop_duplicates(subset=[orgunit + '_id', orgunit])
        df_orgunit_source.columns = [orgunit + '_id_%s' % source, orgunit]
    else:
        df_orgunit_source=None

    return(df_orgunit_source)


def unique_zone_id_name(cleaned_df_dict, source):
    """ Takes a cleaned IASO df and returns a df of unique zones, zone IDs, 
        and associated provinces if available
        
    inputs
        cleaned_df_dict: dictionary of cleaned source data
        souce: name of source
        
    output
        df with columns for unique zones, zone ID number, and province if available
        returns none if zone is not in source hierarchy
    
    """
    
    df_source=cleaned_df_dict[source]
    
    if 'zone' in df_source.columns:
        #df_source=pd.DataFrame(df_source)
        if 'province' in df_source.columns:
            df_zone_source = df_source[['zone_id','zone','province']]
            df_zone_source=df_zone_source.drop_duplicates(subset=['zone_id','zone','province'])
            df_zone_source.columns=['zone_id_%s' % source,'zone','province']
        else:
            df_zone_source = df_source[['zone_id','zone']]
            df_zone_source=df_zone_source.drop_duplicates(subset=['zone_id','zone'])
            df_zone_source.columns=['zone_id_%s' % source,'zone']            
    else:
        df_zone_source=None

    return(df_zone_source)


def dist_between_duplicates(cluster):
    for_dist = ""
    cluster = cluster.to_crs(epsg=3310)
    if len(cluster) == 2:
        for_dist = LineString(cluster["geometry"].tolist())
    if len(cluster) > 2:
        for_dist = pd.DataFrame()
        for_dist['geometry'] = cluster['geometry'].apply(lambda x: x.coords[0])
        for_dist = Polygon(for_dist['geometry'].tolist())
    if (type(for_dist) == str) is False:
        centroid = for_dist.centroid    
        distance = cluster.distance(centroid) / 1000
        out = gpd.GeoDataFrame({"geometry":[centroid], "mean_distance":[distance.mean()]},crs={'init':'epsg:3310'})
        out = out.to_crs(epsg=4326)
        return out


class MidpointNormalize(colors.Normalize):
    def __init__(self, vmin=None, vmax=None, midpoint=None, clip=False):
        self.midpoint = midpoint
        colors.Normalize.__init__(self, vmin, vmax, clip)

    def __call__(self, value, clip=None):
        x, y = [self.vmin, self.midpoint, self.vmax], [0, 0.5, 1]
        return np.ma.masked_array(np.interp(value, x, y))
        
def matching_metrics(matched_data, string_threshold):
    print("Raw Number Matches: " + str(len(matched)))
    print("Raw % matched in Kemri: " + str(len(matched) / kemri_n))
    print("-----------------------")
    over_threshold = matched[matched.name_distance > string_threshold]
    print("Matches over string_threshold : " + str(len(over_threshold)))
    print("% matched over string_threshold in Kemri: " + str(len(over_threshold)/ kemri_n))
    return over_threshold