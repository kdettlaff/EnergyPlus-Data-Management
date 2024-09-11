# -*- coding: utf-8 -*-
"""
Created on Thurs 20240718

@author: Kasey Dettlaff, Ninad Gaikwad

"""

# Reviewed

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

from datetime import datetime as dt, timedelta

# =============================================================================
# Format Datetime Correctly 
# =============================================================================

def format_datetime(simulation_year, datetime_str):
    """
    Formats a date and time string by combining it with a simulation year to produce a standard datetime format.
    This function handles cases where the time is "24:00:00" by converting it to "00:00:00" of the next day.

    Args:
        simulation_year (int or str): The year to be used in the formatted datetime string.
        datetime_str (str): The date and time string in the format "MM/DD  HH:MM:SS".

    Returns:
        str: A formatted datetime string in the ISO 8601 format "YYYY-MM-DD HH:MM:SS".

    Example:
        >>> format_datetime(2024, "08/13  14:30:00")
        '2024-08-13 14:30:00'
      
    """
    
    datetime_str = datetime_str.strip()
    
    # Parse the date and time components
    month = datetime_str.split('/')[0].zfill(2)
    day = datetime_str.split('/')[1].split('  ')[0].zfill(2)
    time = datetime_str.split('  ')[1].strip()

    hour = time.split(':')[0]
    minute = time.split(':')[1]
    second = time.split(':')[2]
    
    if hour == '24':
        # Convert to 00:00:00 of the next day
        original_date_str = f"{simulation_year}-{month}-{day} 00:00:00"
        date_obj = dt.strptime(original_date_str, '%Y-%m-%d %H:%M:%S')
        date_obj += timedelta(days=1)
        formatted_datetime = date_obj.strftime('%Y-%m-%d %H:%M:%S')
    else:
        # Format the datetime normally
        formatted_datetime = f"{simulation_year}-{month}-{day} {time}"
    
    return formatted_datetime

# =============================================================================
# Make Edited IDF File
# =============================================================================

def make_edited_idf(simulation_settings, sim_results_folderpath, idf_filepath):
    
    # Copying IDF to Temporary Folder
    temp_folderpath = os.path.abspath(os.path.join(sim_results_folderpath, '..', 'Temporary_Folder')) #DEBUG Sim results folderpath is incorrect
    if not os.path.exists(temp_folderpath): os.mkdir(temp_folderpath)
    temp_idf_filepath = os.path.join(temp_folderpath, os.path.basename(idf_filepath))
    shutil.copy(idf_filepath, temp_folderpath)
    
    # Loading Temp IDF File
    temp_idf = op.Epm.load(temp_idf_filepath)
    
    # Editing RunPeriod
    temp_idf_runperiod = temp_idf.RunPeriod.one()
    temp_idf_runperiod['begin_day_of_month'] = simulation_settings["sim_start_datetime"].day 
    temp_idf_runperiod['begin_month'] = simulation_settings["sim_start_datetime"].month
    temp_idf_runperiod['end_day_of_month'] = simulation_settings["sim_end_datetime"].day
    temp_idf_runperiod['end_month'] = simulation_settings["sim_end_datetime"].month
    
    # Editing TimeStep
    temp_idf_timestep = temp_idf.TimeStep.one()
    temp_idf_timestep['number_of_timesteps_per_hour'] = int(60/simulation_settings["sim_timestep"])
    
    # Getting Current Schedule
    current_schedule_compact = temp_idf.Schedule_Compact
    current_schedule_compact_records_dict = current_schedule_compact._records
    
    # Save Edited IDF
    temp_idf.save(temp_idf_filepath)
    
    # Appending Special IDF File into Edited IDF File
    special_idf_filepath = os.path.join(os.path.dirname(__file__), 'Special.idf')
    with open(special_idf_filepath, "r") as idf_from: data = idf_from.read()
    with open(temp_idf_filepath, "a") as idf_to: 
        idf_to.write("\n")
        idf_to.write(data)
    
    return temp_idf_filepath

# =============================================================================
# Simulate Variable 
# =============================================================================

def simulate_variable(simulation_settings, idf_filepath, weather_filepath, sim_results_folderpath, variablename): # DEBUG: Changed header, will need to update everywhere the function in used. 
    
    """
    Simulate a variable using the given IDF and weather files, and save the output variable to a CSV file.
    
    Parameters:
    sim_results_folderpath (str): The folder path where the simulation results are stored. Has subfolders:
        - 'TimeSeriesData' for time series data in CSV format.
        - 'ProcessedData' for processed data in pickle format.
        - 'OutputFiles' for additional output files. 
        - 'Temporary_Folder' for temporary files

    This function performs the following steps:
    1. Loads the IDF file and retrieves the Output:Variable object.
    2. Updates the Output:Variable object with the specified parameters.
    3. Saves the modified IDF file to a specified output folder.
    4. Runs the EnergyPlus simulation using the modified IDF and weather files.
    5. Deletes the modified IDF file after the simulation is complete.

    Returns:
    
    """
    
    # Create Folder structure for Simulation Results, if it does not exist
    if not os.path.exists(sim_results_folderpath): os.makedirs(sim_results_folderpath)
    if not os.path.exists(os.path.join(sim_results_folderpath, 'TimeSeriesData')): os.makedirs(os.path.join(sim_results_folderpath, 'TimeSeriesData'))
    if not os.path.exists(os.path.join(sim_results_folderpath, 'OutputFiles')): os.makedirs(os.path.join(sim_results_folderpath, 'OutputFiles'))
    if not os.path.exists(os.path.join(sim_results_folderpath, 'ProcessedData')): os.makedirs(os.path.join(sim_results_folderpath, 'ProcessedData'))
    if not os.path.exists(os.path.join(sim_results_folderpath, 'Temporary_Folder')): os.makedirs(os.path.join(sim_results_folderpath, 'Temporary_Folder'))
    
    # Getting Output Variable Queryset from IDF File
    Edited_IDFFile = op.Epm.load(idf_filepath)
    OutputVariable_QuerySet = Edited_IDFFile.Output_Variable.one() # DEBUG we are getting Queryset contains no value, probably because we ignored the Special IDF Stuff
    
    # Updating OutputVariable_QuerySet in Special IDF File
    OutputVariable_QuerySet['key_value'] = '*'
    OutputVariable_QuerySet['reporting_frequency'] = simulation_settings["sim_output_variable_reporting_frequency"]
    OutputVariable_QuerySet['variable_name'] = variablename
    
    # Saving Edited IDF File in Temporary Folder
    Edited_IDFFile_Path = os.path.abspath(os.path.join(idf_filepath, '..', 'Edited_IDFFile.idf'))
    Edited_IDFFile.save(Edited_IDFFile_Path)
    
    # Run Building Simulation to obtain current output variable
    op.simulate(Edited_IDFFile_Path, weather_filepath, base_dir_path=sim_results_folderpath)

    # Organize Output Files
    timeseriesdata_csv_filepath = os.path.join(sim_results_folderpath, 'TimeSeriesData', variablename).replace(' ', '_') + ".csv"
    timeseriesdata_source_filepath = os.path.join(sim_results_folderpath, "eplusout.csv")
    shutil.move(timeseriesdata_source_filepath, timeseriesdata_csv_filepath)
    
    for filename in os.listdir(sim_results_folderpath):
        source_filepath = os.path.join(sim_results_folderpath, filename)
        destination_filepath = os.path.join(sim_results_folderpath, 'OutputFiles', filename)
        shutil.move(source_filepath, destination_filepath)
    
    # Delete the Edited IDF File
    os.remove(Edited_IDFFile_Path)
    
    eiofilepath = os.path.join(sim_results_folderpath, 'eplusout.eio')
    
    return timeseriesdata_csv_filepath, eiofilepath

# =============================================================================
# Convert and Save Output Variables .csv to.mat in Results Folder
# =============================================================================    

def Process_TimeSeriesData(simulation_settings, sim_results_folderpath):
    """
    Processes output variable time series data from multiple CSV files and saves the processed data into a single dictionary. Pickles the Dictionary.

    Parameters:
    CSV_Folderpath: Path to folder containing time series data in CSV format for multiple variables. 
    ProcessedData_FolderPath (str): Path to the folder where the processed data (Pickle File) will be saved.
    IDF_FileYear (str, optional): Year to be used for the datetime conversion. Default is '2013'.

    The function performs the following steps:
    1. Reads the 'Date/Time' column from the first CSV file and processes it into a list of datetime objects.
       If the time is '24:00:00', it is converted to '00:00:00' of the next day.
    2. For each CSV file, drops the 'Date/Time' column and stores the remaining data in a dictionary.
    3. Collects the column names of all variables in the CSV files.
    4. Writes the collected column names to a text file.
    5. Saves the processed data into a dictionary with keys for datetime information and variables.
    6. Serializes the dictionary to a pickle file.
    7. Returns the filepath to the pickle file. 

    The resulting dictionary has the following structure:
    {
        'DateTime_List': [list of datetime objects],
        'Column_Names': [list of column names],
        'Variable1': dataframe of variable1 data,
        'Variable2': dataframe of variable2 data,
        ...
    }

    The pickle file is saved in the specified folder with the name 'IDF_OutputVariables_DictDF.pickle'.
    The column names are saved in a text file with the name 'IDF_OutputVariable_ColumnName_List.txt'.

    Returns:
    IDF_OutputVariables_DictDF_filepath
    """
    
    # Get Filepath of all Time Series Data CSV's
    timeseriesdata_filepaths = []
    for filename in os.listdir(os.path.join(sim_results_folderpath, 'TimeSeriesData')):
        if filename.endswith('.csv'):
            timeseriesdata_filepaths.append(os.path.join(sim_results_folderpath, 'TimeSeriesData', filename))
    
    # Initializing IDF_OutputVariable_Dict
    IDF_OutputVariable_Dict = {}
    IDF_OutputVariable_ColumnName_List = []

    Is_First_FilePath = 1;
       
    for filepath in timeseriesdata_filepaths:
        
            # ===== Creating and saving DateTime to IDF_OutputVariable_Dict =================== #
        
            if Is_First_FilePath == 1:
            
                Current_DF = pd.read_csv(filepath)
                DateTime_List = []
                DateTime_Column = Current_DF['Date/Time']
            
                sim_year = simulation_settings["sim_start_datetime"].year

                for DateTime in DateTime_Column: 
        
                    formated_datetime = format_datetime(sim_year, DateTime)
                        
                    DateTime_List.append(formated_datetime)
        
                IDF_OutputVariable_Dict['DateTime_List'] = DateTime_List
        
            # ===== Processing Variable ====================================================== #
        
            # Reading .csv file into dataframe
            if Is_First_FilePath == 0: Current_DF = pd.read_csv(filepath)
        
            # Dropping DateTime Column
            Current_DF = Current_DF.drop(Current_DF.columns[[0]],axis=1)
                        
            # Appending Column Names to IDF_OutputVariable_ColumnName_List
            IDF_OutputVariable_ColumnName_List.extend(Current_DF.columns)

            # Storing Current_DF in IDF_OutputVariable_Dict
            VariableName = ((os.path.basename(filepath)).replace('_', ' ')).replace('.csv', '')
            IDF_OutputVariable_Dict[VariableName] = Current_DF
        
            Is_First_FilePath = 0;
    
    # Writing and Saving Column Names to a Text File
    with open(os.path.join(sim_results_folderpath, "IDF_OutputVariable_ColumnName_List.txt"), "w") as textfile:
        for ColumnName in IDF_OutputVariable_ColumnName_List:
            textfile.write(ColumnName + "\n")
    
    # Storing Processed Data in Pickle File
    IDF_OutputVariables_DictDF_filepath = os.path.join(sim_results_folderpath, 'ProcessedData', "IDF_OutputVariables_DictDF.pickle")        
    with open(IDF_OutputVariables_DictDF_filepath, "wb") as f: pickle.dump(IDF_OutputVariable_Dict, f)
            
    return IDF_OutputVariables_DictDF_filepath

# =============================================================================
# Process .eio Output File and save in Results Folder
# ============================================================================= 

def Process_Eio_OutputFile(simulation_settings, sim_results_folderpath)Ligho:
    """
    Processes the contents of an .eio file into a dictionary. Pickles the dictionary. 

    Parameters:
    sim_results_folderpath (str): The folder path where the simulation results are stored. Has subfolders:
        - 'TimeSeriesData' for time series data in CSV format.
        - 'ProcessedData' for processed data in pickle format.
        - 'OutputFiles' for additional output files. 
        - 'Temporary_Folder' for temporary files

    The function performs the following steps:
    1. Reads the lines from the .eio file.
    2. Identifies and processes table headers and their corresponding data rows.
    3. Extracts and cleans column names for each category.
    4. Extracts data rows and fills missing columns with 'NA'.
    5. Creates a pandas DataFrame for each category and stores it in a dictionary.
    6. Serialized the dictionary as a pickle.
    7. Returns the dictionary
    8. Returns a path to the pickle file. 

    The resulting dictionary has the following structure:
    {
        'Category1': DataFrame of category1 data,
        'Category2': DataFrame of category2 data,
        ...
    }

    Returns: 
    Eio_OutputFile_Dict, Eio_OutputFile_Dict_Filepath
    """
    
    # Initializing Eio_OutputFile_Dict
    Eio_OutputFile_Dict = {}
    
    eio_filepath = os.path.join(sim_results_folderpath, 'OutputFiles', 'eplusout.eio')
    
    with open(eio_filepath) as f: Eio_OutputFile_Lines = f.readlines() 
    Eio_OutputFile_Lines = Eio_OutputFile_Lines[1:] # Removing Intro Lines
    
    Category_Key = ""
    Category_Key_List = ["Zone Information", "Zone Internal Gains Nominal", "People Internal Gains Nominal", "Lights Internal Gains Nominal", "ElectricEquipment Internal Gains Nominal", "GasEquipment Internal Gains Nominal", "HotWaterEquipment Internal Gains Nominal", "SteamEquipment Internal Gains Nominal", "OtherEquipment Internal Gains Nominal" ]
    Is_Table_Header = 0
    
    # FOR LOOP: For each category in .eio File
    for line in Eio_OutputFile_Lines:

        #Check if Line contains table Header
        for Item in Category_Key_List:
            Category_Key = Item
            if ((line.find(Item) >= 0) and (line.find('!') >= 0)):
                Is_Table_Header = 1
                break 
            else:
                Is_Table_Header = 0
                
        if (Is_Table_Header > 0):

            # Get the Column Names for the .eio File category
            DF_ColumnName_List = line.split(',')[1:]

            # Removing the '\n From the Last Name
            DF_ColumnName_List[-1] = DF_ColumnName_List[-1].rstrip()

            # Removing Empty Element
            if DF_ColumnName_List[-1] == ' ': DF_ColumnName_List = DF_ColumnName_List[:-1]

            # Initializing DF_Index_List
            DF_Index_List = []

            # Initializing DF_Data_List
            DF_Data_List = []

            # FOR LOOP: For all elements of current .eio File category
            for Line_2 in Eio_OutputFile_Lines:

                # IF ELSE LOOP: To check data row belongs to current Category
                if ((Line_2.find('!') == -1) and (Line_2.find(Category_Key) >= 0)):

                    #print(Line_2 + '\n')

                    DF_ColumnName_List_Length = len(DF_ColumnName_List)

                    # Split Line_2
                    Line_2_Split = Line_2.split(',')

                    # Removing the '\n From the Last Data
                    Line_2_Split[-1] = Line_2_Split[-1].split('\n')[0]

                    # Removing Empty Element
                    if Line_2_Split[-1] == ' ':
                        Line_2_Split = Line_2_Split[:-1]

                    # Getting DF_Index_List element
                    DF_Index_List.append(Line_2_Split[0])

                    Length_Line2 = len(Line_2_Split[1:])

                    Line_2_Split_1 = Line_2_Split[1:]

                    # Filling up Empty Column
                    if Length_Line2 < DF_ColumnName_List_Length:
                        Len_Difference = DF_ColumnName_List_Length - Length_Line2

                        for i in range(Len_Difference):
                            Line_2_Split_1.append('NA')

                        # Getting DF_Data_List element
                        DF_Data_List.append(Line_2_Split_1)

                    else:
                        # Getting DF_Data_List element
                        DF_Data_List.append(Line_2_Split[1:])

                else:

                    continue

            # Creating DF_Table
            DF_Table = pd.DataFrame(DF_Data_List, index=DF_Index_List, columns=DF_ColumnName_List)

            # Adding DF_Table to the Eio_OutputFile_Dict
            Eio_OutputFile_Dict[Category_Key] = DF_Table

        else:

            continue
        
    Eio_OutputFile_Dict_Filepath = os.path.join(sim_results_folderpath, 'ProcessedData',"Eio_OutputFile.pickle")
                                                  
    pickle.dump(Eio_OutputFile_Dict, open(Eio_OutputFile_Dict_Filepath, "wb"))
      
    return Eio_OutputFile_Dict, Eio_OutputFile_Dict_Filepath           
 
# =============================================================================
# Check Simulation Already Completed
# =============================================================================
def check_simulation_completed(csv_filepath, simulation_results_folderpath):
    """
    Checks if a simulation has been completed based on the information in the Simulation_Information CSV

    Args:
        csv_filepath (str): The file path to the CSV file that contains the simulation information
        simulation_results_folderpath (str): The folder path where results are stored for the simulation in question.

    Returns:
        int: Returns `1` if a matching record with a 'Complete' status is found, otherwise `0`.
    """
    
    simulation_completed = 0
    
    with open(csv_filepath, 'r') as file:
        lines = file.readlines()
        lines = lines[1:] # Exclude Column Headers
    
    for line in lines:
        if simulation_results_folderpath == line.split(',')[3]:
            if line.strip(',')[4] == 'Complete':
                simulation_completed = 1
    
    return simulation_completed
           
# =============================================================================
# Update Simulation Infomration CSV - Simulation Has Been Started
# =============================================================================
def update_simulation_information(simulation_results_folderpath, status):
    """
    Updates the status of a simulation in the Simulation_Information CSV

    Args:
        csv_filepath (str): The file path to the Simulation_Information CSV
        simulation_results_folderpath (str): The folder path where results are stored for the simulation in question.
        status (str): The new status to be updated in the CSV file.
    
    Returns:
        None: The function updates the file in place and does not return any value.
    """
    
    simulation_information_filepath = os.path.join(os.path.dirname(__file__), '..', 'Simulation_Information.csv')

    with open(simulation_information_filepath, 'r') as file:
        lines = file.readlines()
        lines = lines[1:] # Exclude Column Headers
        
    for line in lines:
        if simulation_results_folderpath == line.split(',')[3]:
            line_split = line.split(',')
            line_split[4] = status 
            

      
        

    

