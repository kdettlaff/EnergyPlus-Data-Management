# -*- coding: utf-8 -*-
"""
Created on Thurs 20240718

@author: Kasey Dettlaff, Ninad Gaikwad

"""

# =============================================================================
# Import Required Modules
# =============================================================================

# External Modules
import sys
import os
import glob
import numpy as np
import pandas as pd
import scipy.io
import opyplus as op
import re
import shutil
import datetime
import pickle

#Internal Module
from Automated_DataGeneration.EP_DataGenerator import *
from Database_DataUploader.Database_Creator import *
from Database_DataUploader.BuildingTimeSeriesData_Uploader import *
from Database_DataUploader.BuildingIds_DataUploader import *

# =============================================================================
# Check Simulation Status
# =============================================================================
def check_simulation_status(filepaths):
    
    with open(filepaths["sim_information_filepath"], 'r') as file:
        lines = file.readlines()
    
    for line in lines:
        if filepaths["sim_results_folderpath"] == line.split(',')[3]:
            status = line.split(',')[4] 
    
    return status

# =============================================================================
# Generate and Upload One Variable
# =============================================================================

def generate_and_upload_variable(conn_information, simulation_settings, filepaths, buildingid, variable):
    
    timeseriesdata_information = filepaths["timeseriesdata_information"] 
    simulation_information = filepaths["sim_information_filepath"]
    
    if not already_uploaded(timeseriesdata_information, simulation_settings, buildingid, variable): # DEBUG: changed already_updated function, need to fix
        
        # Simulate Variable
        timeseriesdata_csv_filepath, eiofilepath = simulate_variable(simulation_settings, filepaths, variable)
        
        # Upload Variable
        upload_variable_timeseriesdata(conn_information, simulation_settings, filepaths, timeseriesdata_csv_filepath, buildingid, variable)
        
        if simulation_settings["keepfile"] == 'none': shutil.rmtree(filepaths["sim_results_folderpath"])
    
    return timeseriesdata_csv_filepath, eiofilepath
        
# =============================================================================
# Generate and Upload One Building 
# =============================================================================
            
def generate_and_upload_building(conn_information, simulation_settings, filepaths, variable_list):  
    
    # Load Simulation Settings into IDF file
    edited_idf_filepath = make_edited_idf(simulation_settings, filepaths)
    filepaths["idf_filepath"] = edited_idf_filepath


    # Check if BuildingIds Table already created. If not, create Table
    table_exists, table_empty = check_table_exists(conn_information, "public", "buildingids")
    if not table_exists: create_buildingids_table(conn_information)
    
    # Check if TimeSeriesData Table already created. If not, create Table
    table_exists, table_empty = check_table_exists(conn_information, "public", "timeseriesdata")
    if not table_exists: create_timeseriesdata_table(conn_information)
    
    # Check if TimeSeriesData Table already created. If not, create Table
    table_exists, table_empty = check_table_exists(conn_information, "public", "eiotabledata")
    if not table_exists: create_eiotabledata_table(conn_information)
    
    # Check if Eio
    
    if not check_simulation_status(filepaths) == 'Uploaded':
        
        buildingid = upload_to_buildingids(conn_information, filepaths) # BUG: This is returning buildingid = 1 when it should be greater than 1
        
        for variable in variable_list:
            
            # Facility Total HVAC Demand power didn't get uploaded
            # Time series data information got written to empty
            timeseriesdata_csv_filepath, eiofilepath = generate_and_upload_variable(conn_information, simulation_settings, filepaths, buildingid, variable)    
            
    # Organize Output Files
            
    if simulation_settings["keepfile"] in ["all", "processed"]:
        
        processed_timeseriesdata_pickle_filepath = Process_TimeSeriesData(simulation_settings, variable_list, filepaths) # A datetime is formatted incorrectly somewhere
        Eio_OutputFile_Dict, Eio_OutputFile_Dict_Filepath = Process_Eio_OutputFile(simulation_settings, filepaths, eiofilepath)
        
    if simulation_settings["keepfile"] == "processed":
        
        # Delete all except the pickle files
        for filename in os.listdir(filepaths["sim_results_folderpath"]):
            if not filename.endswith('.pickle'):
                filepath = os.path.join(filepaths["sim_results_folderpath"], filename)
                os.remove(filepath)
                  
    # Make Folder structures
    timeseriesdata_subfolder = os.path.join(filepaths["sim_results_folderpath"], 'Time Series Data')
    os.makedirs(timeseriesdata_subfolder, exist_ok=True)
    outputfiles_subfolder = os.path.join(filepaths["sim_results_folderpath"], 'Output Files')
    os.makedirs(outputfiles_subfolder, exist_ok=True)
    processed_data_subfolder = os.path.join(filepaths["sim_results_folderpath"], 'Processed Data')
    os.makedirs(processed_data_subfolder, exist_ok=True)
        
    for filename in os.listdir(filepaths["sim_results_folderpath"]):
        filepath = os.path.join(filepaths["sim_results_folderpath"], filename)
        if os.path.isfile(filepath):
            if filename.endswith('.csv'): shutil.move(filepath, os.path.join(timeseriesdata_subfolder, filename))
            elif filename.endswith('.pickle'): shutil.move(filepath, os.path.join(processed_data_subfolder, filename))
            else: shutil.move(filepath, os.path.join(outputfiles_subfolder, filename))  

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
        
        filepaths = {
            "idf_filepath": idf_filepath,
            "weather_filepath": weather_filepath,
            "sim_results_folderpath": sim_results_folderpath,
            "sim_information_filepath": sim_information_csv_filepath,
            "timeseriesdata_information": timeseriesdata_csv_filepath}
        
        generate_and_upload_building(conn_information, simulation_settings, filepaths, variable_list) # BUG: For second building, still uploading buildingid = 1

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

automated_data_generation(conn_information, simulation_settings, filepaths, variable_list, sim_information_filepath)     

# =============================================================================
# Debug
# =============================================================================

# For testing, should make a second database

            
# =============================================================================
# To DO
# =============================================================================

# customize server initializer code
# Add to the checks system to save time in the case that a simulation is interrupted     
