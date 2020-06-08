import pandas as pd
import numpy as np
import geopandas as gpd

replace_dico = {"common":{
                    "_":" ",
                    "-":" ",
                    "'":" ",
                    "\.":"",
                    "ã¯":"i",
                    "ï":"i",
                    " 1":" i",
                    " 2":" ii",
                    " 3":" iii",
                    " 4":" iv",
                    "é":"e",
                    "è":"e",
                    "ô":"o",
                    "  ":" "
                    },
                "province":{
                    "mai ndombe":"maindombe",
                    "lulua":"kasai central",
                    "congo central":"kongo central",
                    "kasai occidental":"kasai",
                    "iturie":"ituri",
                    " province":"",
                    "orientale":"",
                    "buele":"bas uele",
                    "nordkivu":"nord kivu",
                    "haut katanga":"katanga",
                    " dps":""
                    },
                "zone":{
                    'lolanga lolanga mampoko':"lolanga mampoko",
                    'nyirangongo':"nyiragongo",
                    'vuhuvi':'vuhovi',
                    'nsona pangu':'nsona mpangu',
                    'bena tshiadi':"bena tshadi",
                    'hors zone: tshiamilemba':"",
                    'mambre':"",
                    'divine':"",
                    'la police':'police',
                    'benaleka':'bena leka',
                    'mobayimbongo':'mobayi mbongo',
                    'ndjoko punda':'ndjoko mpunda',
                    'mwetshi':'muetshi',
                    'wamba luadi':'wamba lwadi',
                    'djalo djeka':'djalo ndjeka',
                    'kisandji':'kisanji',
                    'ruashi':'rwashi',
                    'haut plateau':'hauts plateaux',
                    'kiambi':'kiyambi',
                    'bogosenubea':'bogosenubia',
                    'muanda':'moanda',
                    'massa':'masa',
                    'pendjwa':'penjwa',
                    'yalifafu':'yalifafo',
                    'busanga':'bosanga',
                    'citenge':'tshitenge',
                    'tshilenge':'tshitenge',
                    'banzow moke':'banjow moke',
                    "banjow moke":'banjow moke',
                    'mweneditu':'mwene ditu',
                    'kimbao':'kimbau',
                    'kabeya kamwanga':'kabeya kamuanga',
                    'penjwa':'pendjwa',
                    'nyankunde':'nyakunde',
                    'gety':'gethy',
                    'mongbwalu':'mongbalu',
                    'mampoko':'lolanga mampoko',
                    'bena tshiadi': 'bena tshadi',
                    'nsona-pangu':'nsona mpangu',
                    " zone de sante":""
                    },
                "aire":{},
                "fosa":{
                    "centre de sante de reference":"centre de sante",
                    "csr":"centre de sante",
                    "hgr":"hopital general de reference",
                    "general referencia":"hopital general de reference",
                    "centre hopital":"centre hospitalier",
                    "cs ":"centre de sante",
                    " cs":"centre de sante",
                    " cm":"centre medical",
                    "poly clinique":"polyclinique",
                    "centre sante":"centre de sante",
                    "centre de de sante":"centre de sante",
                    "polyclinic":"polyclinique",
                    "polyclique":"polyclinique",
                    "policlique":"polyclinique",
                    "palyclinique":"polyclinique",
                    "policlynique":"polyclinique",
                    "clinic":"clinique",
                    "centre hospistalier":"centre hospitalier",
                    "centre de medical":"centre medical",
                    "centre de centre de reference": "centre de sante",
                    "a general de reference":"a hopital general de reference",
                    " h?pital ":" hopital ",
                    "centre medico centre chirurgical":"centre medico chirurgical",
                    "poste sante":" poste de sante",
                    " centre hospital de reference ":" hopital general de reference ",
                    "posta de sante":"poste de sante",
                    " centre hospital general de reference ":" hopital general de reference ",
                    "centre centre chirurgical":"centre medico chirurgical",
                    "centrede sante":"centre de sante",
                    "centre de hospitalier":"centre hospitalier",
                    " hopita hopital de reference ": " hopital general de reference ",
                    "poste de de sante":"poste de sante",
                    "medical center":"centre medical",
                    "health center":"centre de sante",
                    "cente de sante": "centre de sante",
                    " oste de sante ": " poste de sante ",
                    " cs ": " centre de sante ",
                    "postede sante": "poste de sante",
                    " . general de reference": " hopital general de reference",
                    " centre de hospitalier":"centre hospitalier",
                    "poste de sangte":"poste de sante",
                    "anoalite hospital":"anoalite hopital",
                    "jerusalem clinic": "jerusalem clinique",
                    "babonde ch":"babonde centre hospitalier",
                    "butembo 3 ch":"butembo 3 centre hospitalier",
                    "mushenyi centre de":"mushenyi centre de sante",
                    "de bangamba centre de sant":"de bangamba centre de sante",
                    "ã‰":"e",
                    "\?":"e",
                    " ste ":" sainte ",
                    " st ":" saint ",
                    " Centr? ":" centre ",
                    " centr? ":" centre ",
                    " ra©fa©rence ":" reference ",
                    "maternita©":"maternite",
                    " sant? ":" sante ",
                    " santa‰ ":" sante ",
                    " santa© ":" sante ",
                    " ra‰fa‰renc ":" reference ",
                    " ra©ference ":" reference ",
                    " solidarita© ":" solidarite "
                    }
                }

province_prefix = ['kl ', 'ks ', 'hk ', 'sk ', 'kr ', 'su ', 'kg ', 'it ', 'kn ',
                   'eq ', 'lm ', 'll ', 'kc ', 'hl ', 'ke ', 'tn ', 'nu ', 'mg ',
                   'tp ', 'nk ', 'bu ', 'md ', 'mn ', 'hu ', 'sn ', 'tu ']

fosa_types = ["centre de sante","poste de sante","hopital general de reference","centre medical","aire de sante"
              "dispensaire","centre hospitalier","hopital secondaire","polyclinique","hopital","clinique","centre medico chirurgical"]

drop = ["supprimer","structure a supprime","structure à supprime","to delete","aire de santte","aire de sante"," ",
        "zone de sante kikwit nord","bureau central dela zone de sante rurale de nioki",
        'bureau central de la zone de sante rurale de mushie']

def read_and_clean_carte_sanitaire(url):
    colnames = ["country", "province", "zone", "fosa_drop" , "fosa", "resp","address","phone","level","dhis2_id","gps"]
    carte_sanitaire = pd.read_excel(url, sheet_name = "Entités", 
                                    names=colnames )
    # Split GPS field  as separate Longitude and Latitude
    gps_as_list = carte_sanitaire.gps.str.replace("\[|\]","").str.split(",")
    carte_sanitaire["long"] = gps_as_list.apply(lambda x: x[0] if (type(x) is list ) is True else np.nan ).astype(float)
    carte_sanitaire["lat"] = gps_as_list.apply(lambda x: x[1] if (type(x) is list ) is True else np.nan ).astype(float)
    carte_sanitaire = carte_sanitaire.drop(["fosa_drop","resp","address","phone","level","gps","country"], axis=1)
    # Create a geopandas DataFrame
    carte_sanitaire = gpd.GeoDataFrame(carte_sanitaire[["province","zone","fosa","dhis2_id"]],
                             geometry=gpd.points_from_xy(carte_sanitaire.long, carte_sanitaire.lat),crs={'init':'epsg:4326'})
    # Format_names
    levels = ["province", "zone", "fosa"]
    for level in levels:
        carte_sanitaire[level] = carte_sanitaire[level].str[3:]
        carte_sanitaire[level] = name_formatter(carte_sanitaire[level], level)
    carte_sanitaire = split_names(carte_sanitaire, fosa_types, drop)
    return carte_sanitaire


def read_and_clean_kemri_data(url):
    colnames = ["country","province","fosa","fosa_type","ownership","lat","long","source"]
    kemri_data = pd.read_excel(url, names=colnames)
    # Subset only to DRC data
    kemri_drc  = kemri_data[kemri_data.country == "Democratic Republic of the Congo"]
    kemri_drc = kemri_drc.drop(["country"], axis=1)
    # Create a unique index
    kemri_drc["kemri_id"] = "km" + kemri_drc.index.astype(str)
    # Create a geopandas DataFrame
    kemri_drc = gpd.GeoDataFrame(kemri_drc[["province","fosa","kemri_id","fosa_type","ownership","source"]],
                                           geometry=gpd.points_from_xy(kemri_drc.long, kemri_drc.lat),crs={'init':'epsg:4326'}
                                            )
    # Format_names
    levels = ["province", "fosa"]
    for level in levels:
        kemri_drc[level] = name_formatter(kemri_drc[level], level)
    kemri_drc.fosa[kemri_drc.fosa.str[0:3].isin(province_prefix)] = kemri_drc.fosa[kemri_drc.fosa.str[0:3].isin(province_prefix)].str[3:]
    kemri_drc = split_names(kemri_drc, fosa_types, drop)
    return (kemri_data, kemri_drc)

def read_and_clean_zones_data(url, hierarchy):
    zones = gpd.read_file(url)
    zones.columns = ["zone_id","geometry"]
    hierarchy = pd.read_csv(hierarchy)
    hierarchy = hierarchy[hierarchy.level == 3]
    hierarchy = hierarchy[["id","name","level_2_name"]]
    hierarchy.columns = ["zone_id","zone","province"]
    # Format_names
    levels = ["province", "zone"]
    for level in levels:
        hierarchy[level] = hierarchy[level].str[3:]
        hierarchy[level] = name_formatter(hierarchy[level], level)
    zones = zones.merge(hierarchy)
    return zones


def name_formatter(data, replace_dico, names, name_level):
    """Reformats names to remove special characters, replace acronyms, and correct common misspellings
    inputs
        data: data frame containing source data from IASO
        replace_dico: nested dictionary of strings to replace with sub dictionaries
                    for common phrases and characters, province, zone, aire, and fosa
        names: column name of string data to reformat
        name_level: level of hierarchy (province, zone, aire, fosa) of names column
    outputs
        data frame with reformatted columns
    
    """
    data.loc[:, names] = data.loc[:, names].str.lower().replace(replace_dico["common"], regex=True)
    data.loc[:, names] = data.loc[:, names].replace(replace_dico[name_level], regex=True ).str.strip()
    data.loc[data[names].str[0:3].isin(province_prefix), names] = data.loc[data[names].str[0:3].isin(province_prefix), names].str[3:]
    data.loc[data[names] == "", names] = None
    return data


def split_names(data, names, fosa_types, drop):
    """Separates facility type prefix from facility name
    inputs
        data: data frame containing source data from IASO
        names: column name to split
        fosa_types: list of facility types
        drop: list of prefixes indicating row should be dropped from data
        
    outputs
        data frame with name column separated into fosa type and fosa name columns
    """
    type_pattern = '|'.join(fosa_types)
    data.loc[:, "fosa_type"] = data.loc[:, names].str.extract('('+type_pattern+')', expand=True)
    data.loc[:, "fosa_name"] = data.loc[:, names].str.replace(type_pattern, "")

    data = data[~(data[names].isin(drop))]
    data = data[~(data.fosa_name.isin(drop))]
    return data


def get_iaso_data(source, org_unit_type=["Centre de Santé", "Hôpital","zone de santé","aire de santé"]):    
    """Pulls data from IASO server for given source
    inputs
        source: dictionary of source metadata,  ex: {'col': 'peru', 'hierarchy': ['aire', 'zone'],  
                                                    'iaso_tag': 'UCLA', 'name': 'UCLA Data'}
    output
        pandas.DataFrame of IASO data
    """
    
    print("Reading " + source["name"])
    version_id = SourceVersion.objects.get(data_source__name=source["iaso_tag"], number=1)
    
    if ("hierarchy" in source.keys()):
        hierarchy = source["hierarchy"]
    else: 
        hierarchy = None
    
    if ("iaso_groups" in source.keys()):
        groups = source["iaso_groups"]
    else: 
        groups = None
        
    if hierarchy:
        parents = "__".join(["parent"]*len(hierarchy))
        source_ou = OrgUnit.objects.filter(version_id=version_id, 
                                           org_unit_type__name__in = org_unit_type).select_related(parents)
    else:
        source_ou = OrgUnit.objects.filter(version_id=version_id, org_unit_type__name__in = org_unit_type)
    
    if groups:
        source_ou = OrgUnit.objects.filter(version_id=version_id, 
                                           org_unit_type__name__in = org_unit_type,
                                          groups__name__in = groups).select_related('parent__parent__parent')
        
    features_query = "[(ou.id, ou.name, ou.location if ou.location else None,\
    ou.org_unit_type if ou.org_unit_type else None"
    hierarchy_query = ""
    columns = ["ID", "name", "coordinates", "type"] 
    
    print("hierarchy", hierarchy)
    
    if hierarchy:
        for level in hierarchy:
            level_id = level+"_id"
            columns = columns + [level_id, level]        
        level = 0 
        parents = []
        query = ""
        while level  < len(hierarchy):
            level = level + 1
            parent = "ou" + ".parent"*level
            parents = parents + [parent]
            query = query + parent + ".id" + " if (" + " and ".join(parents) + ") else None,"+ parent + \
                    ".name" + " if (" + " and ".join(parents) + ") else None,"
            parent_id = "ou" + ".parent"*level+"_id"

        hierarchy_query = "," + query[:-1]
        
    query_final = features_query + hierarchy_query + ") for ou in source_ou]"

    values_list = eval(query_final)       
    print("   Found " + str(len(values_list)) + " values")
    
    out_df = pd.DataFrame(values_list, columns = columns)
    return out_df


def pull_and_clean_data(sources, replace_dico):
    """ Helper function to pull all data from IASO and run through text cleaning functions
    inputs
        sources: dictionary of metadata dictionaries
    outputs
        dictionary of cleaned IASO data
    """

    df_cleaned_sources_for_cs={}

    for source in sources.keys():
        sources[source]["data"] = get_iaso_data(sources[source]) # pull source data from IASO
        if ("hierarchy" in sources[source].keys()): # format each name column in hierarchy
            for level in sources[source]["hierarchy"] :
                name_formatter(sources[source]["data"], replace_dico, level, level)

        name_formatter(sources[source]["data"], replace_dico, "name", "fosa")
        df_cs = split_names(sources[source]["data"], "name", drc_geoloc.fosa_types, drc_geoloc.drop)
        df_cs = gpd.GeoDataFrame(df_cs)
        df_cs = df_cs.dropna(subset=['fosa_name'])
        df_cleaned_sources_for_cs[source] = df_cs
        
    return df_cleaned_sources_for_cs