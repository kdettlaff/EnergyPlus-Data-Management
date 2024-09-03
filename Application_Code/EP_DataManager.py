# -*- coding: utf-8 -*-
"""
Created on Thurs 20240718

@author: Kasey Dettlaff, Ninad Gaikwad

"""

# =============================================================================
# Import Required Modules - Reviewed
# =============================================================================

# External Modules
import sys
import os
import glob
from time import process_time
import numpy as np
import pandas as pd
import scipy.io
import opyplus as op
import re
import shutil
import datetime
import pickle

#Internal Module
from EP_DataGenerator import *
from EP_DataAggregator import * 
from EP_DataRetrieval import *
from Database_Creator import * 
from BuildingIds_DataUploader import *
from BuildingTimeSeriesData_Uploader import *
from EioTableData_DataUploader import * 

# =============================================================================
# Check Simulation Status
# =============================================================================
def check_simulation_status(sim_results_folderpath):
    
    sim_information_filepath = os.path.join(os.path.dirname(__file__), '..', 'Generated_Textfiles', 'Simulation_Information.csv')
    with open(sim_information_filepath, 'r') as file:
        lines = file.readlines()
    
    for line in lines:
        if sim_results_folderpath == line.split(',')[3]:
            status = line.split(',')[4] 
    
    return status

# =============================================================================
# Update Simulation Information CSV
# =============================================================================
def update_simulation_information(sim_results_folderpath, field, newvalue):
    """
    Updates a specific field in the Simulation_Information.csv file.

    Args:
        filepaths (dict): A dictionary containing file paths including 'sim_information_filepath' and 'sim_results_folderpath'.
        field (str): The field (column) to update. Possible values are 'BuildingID', 'IDF Filepath', 'Weather Filepath', 'Completed Simulation Folderpath', 'Simulation Status'.
        newvalue (str): The new value to set in the specified field.

    Returns:
        None
    """
    
    sim_information_filepath = os.path.join(os.path.dirname(__file__), '..', 'Generated_Textfiles', 'Simulation_Information.csv')

    # Mapping of field names to their column indices
    field_to_index = {
        'BuildingID': 0,
        'IDF Filepath': 1,
        'Weather Filepath': 2,
        'Completed Simulation Folderpath': 3,
        'Simulation Status': 4
    }
    
    field_index = field_to_index[field]

    # Read the file contents
    with open(sim_information_filepath, 'r') as file:
        lines = file.readlines()

    # Update the specific field for the matching row
    for i in range(len(lines)):
        line_fields = lines[i].strip().split(',')
        if sim_results_folderpath == line_fields[3]:
            line_fields[field_index] = newvalue
            lines[i] = ','.join(line_fields) + '\n'

    # Write the updated content back to the file
    with open(sim_information_filepath, 'w') as file:
        file.writelines(lines)

# =============================================================================
# Generate and Upload One Variable - Reviewed
# =============================================================================

def generate_and_upload_variable(conn_information, simulation_settings, buildingid, idf_filepath, weather_filepath, sim_results_folderpath, variablename):
    
    timeseriesdata_information_filepath = os.path.join(os.path.dirname(__file__), '..', 'Generated_Textfiles', 'TimeSeriesData_Information.csv')
    simulation_information_filepath = os.path.join(os.path.dirname(__file__), '..', 'Generated_Textfiles', 'Simulation_Information.csv')
    
    if not already_uploaded(timeseriesdata_information_filepath, simulation_settings, buildingid, variablename): 
        
        # Simulate Variable
        print("Simulating Variable: " + variablename + '\n')
        timeseriesdata_csv_filepath, eiofilepath = simulate_variable(simulation_settings, idf_filepath, weather_filepath, sim_results_folderpath, variablename)
        
        # Upload Variable
        print("Uploading Variable to TimeSeriesData Table: " + variablename + '\n')
        upload_variable_timeseriesdata(conn_information, buildingid, variablename, simulation_settings, timeseriesdata_csv_filepath)
        
        if simulation_settings["keepfile"] == 'none': shutil.rmtree(filepaths["sim_results_folderpath"])
    
    return timeseriesdata_csv_filepath, eiofilepath
        
# =============================================================================
# Generate and Upload One Building 
# =============================================================================
            
def generate_and_upload_building(conn_information, simulation_settings, sim_results_folderpath, idf_filepath, weather_filepath, variable_list):  
    
    # Load Simulation Settings into IDF file
    edited_idf_filepath = make_edited_idf(simulation_settings, sim_results_folderpath, idf_filepath)

    # Check if BuildingIds Table already created. If not, create Table
    table_exists, table_empty = check_table_exists(conn_information, "public", "buildingids")
    if not table_exists: create_buildingids_table(conn_information)
    
    # Check if TimeSeriesData Table already created. If not, create Table
    table_exists, table_empty = check_table_exists(conn_information, "public", "timeseriesdata")
    if not table_exists: create_timeseriesdata_table(conn_information)
    
    # Check if EioTableData Table already created. If not, create Table
    table_exists, table_empty = check_table_exists(conn_information, "public", "eiotabledata")
    if not table_exists: create_eiotabledata_table(conn_information)
    
    if not check_simulation_status(sim_results_folderpath) == 'Uploaded':
        
        # Update Simulation_Information.csv
        update_simulation_information(sim_results_folderpath, 'Simulation Status', 'Incomplete')
        
        print("Simulating Building: " + os.path.basename(filepaths["sim_results_folderpath"]) + '\n')
        buildingid = upload_to_buildingids(conn_information, sim_results_folderpath) 
        print("Adding to BuildingIDs: " + str(buildingid) + '\n')
        
        for variablename in variable_list:
            
            timeseriesdata_csv_filepath, eiofilepath = generate_and_upload_variable(conn_information, simulation_settings, buildingid, edited_idf_filepath, weather_filepath, sim_results_folderpath, variablename) 
        
        # Update Simulation_Information.csv
        update_simulation_information(sim_results_folderpath, 'Simulation Status', 'Complete')
            
    # Process Data
    if simulation_settings["keepfiles"] in ["Processed", "All"]:
        Process_TimeSeriesData(simulation_settings, sim_results_folderpath)
        Process_Eio_OutputFile(simulation_settings, sim_results_folderpath) 
    if simulation_settings["keepfiles"] == "Processed":
        for filename in os.path.listdir(os.path.join(sim_results_folderpath), 'TimeSeriesData'):
            filepath = os.path.join(sim_results_folderpath, 'TimeSeriesData', filename)
            os.remove(filepath)
        for filename in os.path.listdir(os.path.join(sim_results_folderpath), 'OutputFiles'):
            filepath = os.path.join(sim_results_folderpath, 'OutputFiles', filename)
            os.remove(filepath)        

# =============================================================================
# Generate and Upload Multiple Buildings
# =============================================================================

def automated_data_generation(conn_information, simulation_settings, filepaths, variable_list, sim_information_csv_filepath):
    
    # Create Time Series Data Information CSV if it does not already exist
    timeseriesdata_csv_filepath = os.path.join(os.path.dirname(sim_information_csv_filepath), 'TimeSeriesData_Information.csv')
    if not os.path.exists(timeseriesdata_csv_filepath):
        with open(timeseriesdata_csv_filepath, 'w') as file:
            file.write('BuildingID,Variable Name,Variable Type,SubVariable Name,Sim Start Datetime,Last Uploaded Datetime\n')
    
    with open(sim_information_csv_filepath, 'r') as file:
        lines = file.readlines()
        lines = lines[1:]
        
    for line in lines:
        idf_filepath = line.split(',')[1]
        weather_filepath = line.split(',')[2]
        sim_results_folderpath = line.split(',')[3]
        
        generate_and_upload_building(conn_information, simulation_settings, sim_results_folderpath, idf_filepath, weather_filepath, variable_list) 

# =============================================================================
# Input Dictionaries Used
# =============================================================================

sim_start_datetime = datetime.datetime(2012, 5, 1, 0, 0, 0, 0)
sim_end_datetime = datetime.datetime(2012, 5, 2, 0, 0, 0, 0) # FYI It stills simulated the entire day of 2012/5/2

# Simulation settings dictionary
simulation_settings = {
    "sim_start_datetime": sim_start_datetime, # Example start datetime
    "sim_end_datetime": sim_end_datetime,    # Example end datetime
    "sim_timestep": 5,                           # Example timestep in minutes
    "sim_output_variable_reporting_frequency": 'timestep', # Example reporting frequency
    "keepfile": "all"
}

# Filepaths dictionary
filepaths = {
    "idf_filepath": 'example/path/file.idf',
    "weather_filepath": 'example/path/file.epw',
    "sim_results_folderpath": 'example/folderpath',
    "sim_information_filepath": 'example/file/path.csv'}

# Keepfiles Settings
# 'all' - keeps unprocessed files and created pickles. 
# 'unprocessed' - keeps unprocessed files, doesn't make pickles.
# 'processed' - makes pickles, then deletes unprocessed files. 
# 'none' - doesn't save any backups, the only data is in the database.   

# =============================================================================
# Main
# =============================================================================

Automated_Generation_FolderPath = os.path.dirname(__file__)
sim_information_filepath = os.path.abspath(os.path.join(Automated_Generation_FolderPath, 'Generated_Textfiles', 'Simulation_Information.csv'))

conn_information = "dbname=EP_DataManagement_Application user=kasey password=OfficeLarge"

variable_list = ['Facility Total HVAC Electric Demand Power']

# Empty Time Series Data
table_exists, table_empty = check_table_exists(conn_information, "public", "timeseriesdata")
if not table_empty: empty_table(conn_information, "public", "timeseriesdata")

# Empty BuildingIds Table
table_exists, table_empty = check_table_exists(conn_information, "public", "buildingids")
if not table_empty: empty_table(conn_information, "public", "buildingids")

automated_data_generation(conn_information, simulation_settings, filepaths, variable_list, sim_information_filepath)     

# =============================================================================
# Debug
# =============================================================================

# For testing, should make a second database

            
# =============================================================================
# To DO
# =============================================================================

# customize simulation_information csv creator code, remove broken simulations
# customize server initializer code
# Add to the checks system to save time in the case that a simulation is interrupted     
