# -*- coding: utf-8 -*-
"""
Created on Mon 20240617

@author: Kasey Dettlaff

"""

#This version has csv generation commented out, and was created to work on automation.
#Issue with generatiion of eio pickle file was resolved. 
#This version of the code adds code to delete EIO_Dict_Full and the csv files after generation is complete. 

# =============================================================================
# Import Required Modules
# =============================================================================

# External Modules
import os
import numpy as np
import pandas as pd
import scipy.io
import opyplus as op
import re
import shutil
import datetime
import pickle

# Custom Modules
    
# =============================================================================
# Get Location from Climate Zone
# =============================================================================
def climateZone_to_location(climateZone):
    
    location = 'Unknown'
    
    if climateZone == "CZ1AWH":
        location = "Miami"
    elif climateZone == "CZ1AWHT":
        location = "Honolulu"
    elif climateZone == "CZ1AWHTS":
        location = "Honolulu"
    elif climateZone == "CZ2AWH":
        location = "Tampa"
    elif climateZone == "CZ2B":
        location = "Tucson"
    elif climateZone == "CZ3A":
        location = "Atlanta"
    elif climateZone == "CZ3AWH":
        location = "Montgomery"
    elif climateZone == "CZ3B":
        location = "ElPaso"
    elif climateZone == "CZ3C":
        location = "SanDiego"
    elif climateZone == "CZ4A":
        location = "NewYork"
    elif climateZone == "CZ4B":
        location = "Albuquerque"
    elif climateZone == "CZ4C":
        location = "Seattle"
    elif climateZone == "CZ5A":
        location = "Buffalo"
    elif climateZone == "CZ5B":
        location = "Denver"
    elif climateZone == "CZ5C":
        location = "PortAngeles"
    elif climateZone == "CZ6A":
        location = "Rochester"
    elif climateZone == "CZ6B":
        location = "GreatFalls"
    elif climateZone == "CZ7":
        location = "InternationalFalls"
    elif climateZone == "CZ8":
        location = "Fairbanks"
        
    return location


# =============================================================================
# Get Simulation Name
# =============================================================================
def get_simulation_name(IDF_Filepath):
    
    IDF_FileName = os.path.basename(IDF_Filepath).replace(".idf", "")
    
    # EnergyPlus Prototype Commercial
    # Naming Convention: Standard_Year_Location_BuildingType
    if IDF_FileName.startswith("ASHRAE"): 
        FileName_Split = IDF_FileName.split('_')
        Simulation_Name = "ASHRAE" + '_' + FileName_Split[2][3:] + '_' + FileName_Split[3] + '_' + FileName_Split[1]
    elif IDF_FileName.startswith("IECC"): 
        FileName_Split = IDF_FileName.split('_')
        Simulation_Name = "IECC" + '_' + FileName_Split[2][3:] + '_' + FileName_Split[3] + '_' + FileName_Split[1]
        
    # EnergyPlus Prototype Residential 
    # Naming Convention: Prototype_ClimateZone_Location_HeatingType_FoundationType_Standard_Year
    elif IDF_FileName.startswith("US"): 
        FileName_Split = IDF_FileName.split('+') 
        Simulation_Name = FileName_Split[1] + '_' + FileName_Split[2] + '_' + climateZone_to_location(FileName_Split[2]) + '_' + FileName_Split[3] + '_' + FileName_Split[4] + '_' + FileName_Split[5]

    # EnergyPlus Prototypes Manufactured
    # Naming Convention: Configuration_Location_ClimateZone_EnergyCode_HeatingType
    elif IDF_FileName.startswith("MS") or IDF_FileName.startswith("SS"):
        Simulation_Name = IDF_FileName
    
    else: Simulation_Name = IDF_FileName
    
    return Simulation_Name

# =============================================================================
# Generate Simulation Information
# =============================================================================

def Generate_Simulation_Information(Data_FolderPath, Simulation_Results_FolderPath):
      
    Automated_Generation_FolderPath = os.path.dirname(__file__)
    Simulation_Information_Filepath = os.path.join(Automated_Generation_FolderPath, '..', 'Generated_Textfiles', 'Simulation_Information.csv')
    
    Simulation_Information = open(Simulation_Information_Filepath, 'w')

    # =============================================================================
    # Get List of Weather Files
    # =============================================================================

    commercial_weatherfolderpath = os.path.join(Data_FolderPath, "TMY3_WeatherFiles_Commercial")
    manufactured_weatherfolderpath = os.path.join(Data_FolderPath, "TMY3_WeatherFiles_Manufactured")
    residential_weatherfolderpath = os.path.join(Data_FolderPath, "TMY3_WeatherFiles_Residential")

    commercial_weatherfile_list = []
    for filename in os.listdir(commercial_weatherfolderpath):
        filename = filename.replace('San.Diego', 'SanDiego')
        filename = filename.replace('International.Falls', 'InternationalFalls')
        filename = filename.replace('Great.Falls', 'GreatFalls')
        filename = filename.replace('New.York', 'NewYork')
        filename = filename.replace('El.Paso', 'ElPaso')
        filename = filename.replace('Port.Angeles', 'PortAngeles')
        filepath = os.path.join(commercial_weatherfolderpath, filename)
        commercial_weatherfile_list.append(filepath)
    
    manufactured_weatherfile_list = []
    for filename in os.listdir(manufactured_weatherfolderpath):
        filename = filename.replace('San.Francisco', 'SanFrancisco')
        filename = filename.replace('El.Paso', 'ElPaso')
        filepath = os.path.join(manufactured_weatherfolderpath, filename)
        manufactured_weatherfile_list.append(filepath)
    
    residential_weatherfile_list = []
    for filename in os.listdir(residential_weatherfolderpath):
        filename = filename.replace('San.Diego', 'SanDiego')
        filename = filename.replace('International.Falls', 'InternationalFalls')
        filename = filename.replace('Great.Falls', 'GreatFalls')
        filename = filename.replace('New.York', 'NewYork')
        filename = filename.replace('El.Paso', 'ElPaso')
        filename = filename.replace('Port.Angeles', 'PortAngeles')
        filepath = os.path.join(residential_weatherfolderpath, filename)
        residential_weatherfile_list.append(filepath)

    # =============================================================================
    # Write IDF and Weather Filepath List
    # =============================================================================

    commercial_idf_folderpath = os.path.join(Data_FolderPath, "Commercial_Prototypes")
    manufactured_idf_folderpath = os.path.join(Data_FolderPath, "Manufactured_Prototypes")
    residential_idf_folderpath = os.path.join(Data_FolderPath, "Residential_Prototypes")

    Simulation_Information = open(Simulation_Information_Filepath, 'w')
    
    Simulation_Information.write('BuildingID,IDF Filepath,Weather Filepath,Completed Simulation FolderPath,Simulation Status\n')

    # Commercial IDF Files
    for standard_subfolder in os.listdir(commercial_idf_folderpath):
        standard_subfolder_path = os.path.join(commercial_idf_folderpath, standard_subfolder)
        for year_subfolder in os.listdir(standard_subfolder_path):
            year_subfolder_path = os.path.join(standard_subfolder_path, year_subfolder)
            for file in os.listdir(year_subfolder_path):
                if file.endswith(".idf"):
                    idf_filepath = os.path.join(year_subfolder_path, file)
                    simulation_name = get_simulation_name(os.path.basename(file))
                    simulation_results_folderpath = os.path.join(Simulation_Results_FolderPath, simulation_name)
                    # Write corresponding simulation name
                    simulation_location = simulation_name.split('_')[2]
                    # Write corresponding weather filename
                    found_weatherfile = 0
                    for weatherfilepath in commercial_weatherfile_list:
                        if re.search(simulation_location, weatherfilepath): 
                            Simulation_Information.write('NA,' + idf_filepath + ',' + weatherfilepath + ',' + simulation_results_folderpath + ',' + 'Not Started\n')
                
    # Manufactured IDF Files
    for standard_subfolder in os.listdir(manufactured_idf_folderpath):
        standard_subfolder_path = os.path.join(manufactured_idf_folderpath, standard_subfolder)
        for location_subfolder in os.listdir(standard_subfolder_path):
            location_subfolder_path = os.path.join(standard_subfolder_path, location_subfolder)
            for file in os.listdir(location_subfolder_path):
                if file.endswith(".idf"):
                    idf_filepath = os.path.join(location_subfolder_path, file)
                    simulation_name = get_simulation_name(os.path.basename(file))
                    simulation_results_folderpath = os.path.join(Simulation_Results_FolderPath, simulation_name)
                    # Write corresponding simulation name
                    simulation_location = simulation_name.split('_')[1]
                    # Write corresponding weather filename
                    found_weatherfile = 0
                    for weatherfilepath in manufactured_weatherfile_list:
                        if re.search(simulation_location, weatherfilepath): 
                            Simulation_Information.write('NA,' + idf_filepath + ',' + weatherfilepath + ',' + simulation_results_folderpath + ',' + 'Not Started\n')
                
    # Residential IDF Files
    for standard_subfolder in os.listdir(residential_idf_folderpath):
        standard_subfolder_path = os.path.join(residential_idf_folderpath, standard_subfolder)
        for climateZone_subfolder in os.listdir(standard_subfolder_path):
            climateZone_subfolder_path = os.path.join(standard_subfolder_path, climateZone_subfolder)
            for file in os.listdir(climateZone_subfolder_path):
                if file.endswith(".idf"):
                    idf_filepath = os.path.join(climateZone_subfolder_path, file)
                    simulation_name = get_simulation_name(os.path.basename(file))
                    simulation_results_folderpath = os.path.join(Simulation_Results_FolderPath, simulation_name)
                    # Write corresponding simulation name
                    simulation_location = simulation_name.split('_')[2]
                    # Write corresponding weather filename
                    found_weatherfile = 0
                    for weatherfilename in residential_weatherfile_list:
                        if re.search(simulation_location, weatherfilepath):
                            Simulation_Information.write('NA,' + idf_filepath + ',' + weatherfilepath + ',' + simulation_results_folderpath + ',' + 'Not Started\n')

    # =============================================================================
    # Close CSV
    # =============================================================================

    Simulation_Information.close()
    
    return Simulation_Information_Filepath

# =============================================================================
# Remove Broken IDF's 
# =============================================================================

def remove_broken_idfs(simulation_information_csv_filepath):
    
    with open(simulation_information_csv_filepath, 'r') as file:
        lines = file.readlines()
    
    filtered_lines = []
    for line in lines:
        idf_filename = os.path.basename(line.split(',')[1])
        # Check conditions and append matching lines to filtered_lines
        if (idf_filename.startswith("MS") or idf_filename.startswith("SS") or
            "heatedbsmt" in idf_filename or "slab" in idf_filename or
            "officeLarge" in idf_filename):
            print("Removing: " + idf_filename + '\n')
        else:
            filtered_lines.append(line)
            
    with open(simulation_information_csv_filepath, 'w') as file:
        file.writelines(filtered_lines)

# =============================================================================
# Main
# =============================================================================

Data_FolderPath = r"D:\Building_Modeling_Code\Data"

Automated_DataGeneration_filepath = os.path.dirname(__file__)
Generated_Data_folderpath = os.path.abspath(os.path.join(Automated_DataGeneration_filepath, '..', 'Generated_Data'))

Simulation_Information_Filepath = Generate_Simulation_Information(Data_FolderPath, Generated_Data_folderpath)
remove_broken_idfs(Simulation_Information_Filepath)