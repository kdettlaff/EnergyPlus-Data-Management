# -*- coding: utf-8 -*-
"""
20240807

@author: ninad gaikwad 
"""

# Changed setup so that It uses the text file of filepaths, similar to how data generation was reworked.
#  Hasn't been tested but should work. 

# Need to import Zone Name List 

# =============================================================================
# Import Required Modules
# =============================================================================

# External Modules
import os
import pandas as pd
import pickle
from re import S
import psycopg2
import copy

# =============================================================================
# Aggregate Building from Pickle 
# # =============================================================================
        
def aggregate_building(completed_simulation_folderpath, aggregation_zone_name, aggregation_zone_list, aggregation_type):
 
    # If Aggregation Folder does not exist, create it
    
    aggregation_folderpath = os.path.join(completed_simulation_folderpath, 'Sim_Aggregated_Data')
    if not os.path.exists(aggregation_folderpath): os.makedirs(aggregation_folderpath)
     
    # Load the building pickle file
    pickle_filepath = os.path.join(completed_simulation_folderpath, 'Sim_Processed_Data', 'IDF_OutputVariables_DictDF.pickle')
    with open(pickle_filepath, "rb") as file: data = pickle.load(file)
     
    # Get Associated Areas and Volumes of each Zone
     
    # =============================================================================
    # Initialize Aggregation_DF and Aggregation_DF_Equipment
    # =============================================================================

    # Creating Equipment List
    Equipment_List = ['People', 'Lights', 'ElectricEquipment', 'GasEquipment', 'OtherEquipment', 'HotWaterEquipment', 'SteamEquipment']

    # Initializing Aggregation_DF
    Aggregation_DF = pd.DataFrame()

    # FOR LOOP: For each Variable 
    for key, value in data:
        
        # IF LOOP: For the Variable Name Schedule_Value_
        if (key == 'Schedule_Value_'): # Create Schedule Columns which are needed
        
            # FOR LOOP: For each element in Equipment_List
            for element in Equipment_List:
                
                # Creating Current_EIO_Dict_Key
                Current_EIO_Dict_Key = element + ' ' + '_Internal_Gains_Nominal.csv'
                
                # IF LOOP: To check if Current_EIO_Dict_Key is present in Eio_OutputFile_Dict
                if (Current_EIO_Dict_Key in list(data.keys())):           
                
                    # Creating key1 for column Name
                    key1 = key + element
                
                    # Initializing Aggregation_Dict with None
                    Aggregation_DF[key1] = None    
        
        else: # For all other Columns
        
            # Initializing Aggregation_Dict with None
            Aggregation_DF[key] = None

    # Initializing Aggregation_DF_Equipment
    Aggregation_DF_Equipment = pd.DataFrame()        
            
    # FOR LOOP: For each element in Equipment_List
    for element in Equipment_List:
        
        # Creating Current_EIO_Dict_Key
        Current_EIO_Dict_Key = element + ' ' + '_Internal_Gains_Nominal.csv'
        
        # IF LOOP: To check if Current_EIO_Dict_Key is present in Eio_OutputFile_Dict
        if (Current_EIO_Dict_Key in list(data.keys())): # Key present in Eio_OutputFile_Dict            
        
            # Creating key1 for column Name
            key1 =  element + '_Level'
        
            # Initializing Aggregation_Dict with None
            Aggregation_DF_Equipment[key1] = None

    # =============================================================================
    # Initialize Aggregation_Dict 
    # =============================================================================
    
        DateTime_List = data['DateTime_List']
        
        Aggregation_Zone_NameStem = 'Aggregation_Zone'

        # Initializing Aggregation_Dict
        Aggregation_Dict = {'DateTime_List': DateTime_List}

        # Initializing Counter
        Counter = 0

        # FOR LOOP: For each element in Aggregation_Zone_List
        for element in aggregation_zone_list:
        
            # Incrementing Counter
            Counter = Counter + 1
        
            # Creating Aggregated Zone name 1 : For the Aggregated Time Series
            Aggregated_Zone_Name_1 = Aggregation_Zone_NameStem + "_" + str(Counter)
        
            # Creating Aggregated Zone name 2 : For the Aggregated Equipment 
            Aggregated_Zone_Name_2 = Aggregation_Zone_NameStem + "_Equipment_" + str(Counter)    
        
            # Appending empty Aggregation_DF to Aggregation_Dict
            Aggregation_Dict[Aggregated_Zone_Name_1] = copy.deepcopy(Aggregation_DF)
        
            Aggregation_Dict[Aggregated_Zone_Name_2] = copy.deepcopy(Aggregation_DF_Equipment)