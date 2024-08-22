# -*- coding: utf-8 -*-
"""
Created on Thurs 20240718

@author: Kasey Dettlaff, Ninad Gaikwad

"""

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

def make_edited_idf(simulation_settings, filepaths):
    
    # Copying IDF to Temporary Folder
    temp_folderpath = os.path.abspath(os.path.join(filepaths["sim_results_folderpath"], '..', 'Temporary_Folder')) #DEBUG Sim results folderpath is incorrect
    if not os.path.exists(temp_folderpath): os.mkdir(temp_folderpath)
    temp_idf_filepath = os.path.join(temp_folderpath, os.path.basename(filepaths["idf_filepath"]))
    shutil.copy(filepaths["idf_filepath"], temp_folderpath)
    
    # Loading Temp IDF File
    temp_idf = op.Epm.load(temp_idf_filepath)
    
    # Editing RunPeriod
    temp_idf_runperiod = temp_idf.RunPeriod.one()
    temp_idf_runperiod['begin_day_of_month'] = simulation_settings["sim_start_datetime"].day # DEBUG: datetime is a string not a datetime obj, need to fix
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

def simulate_variable(simulation_settings, filepaths, variable): # DEBUG: Changed header, will need to update everywhere the function in used. 
    
    """
    Simulate a variable using the given IDF and weather files, and save the output variable to a CSV file.

    This function performs the following steps:
    1. Loads the IDF file and retrieves the Output:Variable object.
    2. Updates the Output:Variable object with the specified parameters.
    3. Saves the modified IDF file to a specified output folder.
    4. Runs the EnergyPlus simulation using the modified IDF and weather files.
    5. Deletes the modified IDF file after the simulation is complete.

    Returns:
    
    """
    
    # Getting Output Variable Queryset from IDF File
    Edited_IDFFile = op.Epm.load(filepaths["idf_filepath"])
    OutputVariable_QuerySet = Edited_IDFFile.Output_Variable.one() # DEBUG we are getting Queryset contains no value, probably because we ignored the Special IDF Stuff
    
    # Updating OutputVariable_QuerySet in Special IDF File
    OutputVariable_QuerySet['key_value'] = '*'
    OutputVariable_QuerySet['reporting_frequency'] = simulation_settings["sim_output_variable_reporting_frequency"]
    OutputVariable_QuerySet['variable_name'] = variable
    
    # Saving Edited IDF File in Temporary Folder
    Edited_IDFFile_Path = os.path.abspath(os.path.join(filepaths["idf_filepath"], '..', 'Edited_IDFFile.idf'))
    Edited_IDFFile.save(Edited_IDFFile_Path)
    
    # Run Building Simulation to obtain current output variable
    op.simulate(Edited_IDFFile_Path, filepaths["weather_filepath"], base_dir_path=filepaths["sim_results_folderpath"])

    # Rename Time Series Data CSV
    timeseriesdata_csv_filepath = os.path.join(filepaths["sim_results_folderpath"], variable).replace(' ', '_') + ".csv"
    timeseriesdata_csv_filepath = os.path.abspath(timeseriesdata_csv_filepath)
    timeseriesdata_source_filepath = os.path.abspath(os.path.join(filepaths["sim_results_folderpath"], "eplusout.csv"))
    shutil.move(timeseriesdata_source_filepath, timeseriesdata_csv_filepath)
    
    # Delete the Edited IDF File
    os.remove(Edited_IDFFile_Path)
    
     # THIS MAY NOT BE CORRECT - don't remember how EP names the output csv files. 
    eiofilepath = os.path.join(filepaths["sim_results_folderpath"], 'eplusout.eio')
    
    return timeseriesdata_csv_filepath, eiofilepath

# =============================================================================
# Convert and Save Output Variables .csv to.mat in Results Folder
# =============================================================================    

def Process_TimeSeriesData(simulation_settings, variable_list, filepaths):
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
    
    sim_results_folderpath = filepaths["sim_results_folderpath"]
    
    # Get Filepath of all Time Series Data CSV's
    timeseriesdata_filepaths = []
    for variablename in variable_list:
        timeseriesdata_filename = variablename.replace(' ', '_') + '.csv'
        timeseriesdata_filepath = os.path.join(sim_results_folderpath, timeseriesdata_filename)
        timeseriesdata_filepaths.append(timeseriesdata_filepath)
    
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
    IDF_OutputVariables_DictDF_filepath = os.path.join(sim_results_folderpath, "IDF_OutputVariables_DictDF.pickle")        
    with open(IDF_OutputVariables_DictDF_filepath, "wb") as f: pickle.dump(IDF_OutputVariable_Dict, f)
            
    return IDF_OutputVariables_DictDF_filepath

# =============================================================================
# Process .eio Output File and save in Results Folder
# ============================================================================= 

def Process_Eio_OutputFile(simulation_settings, filepaths, eio_filepath):
    """
    Processes the contents of an .eio file into a dictionary. Pickles the dictionary. 

    Parameters:
    Eio_FilePath (str): Path to the .eio file to be processed.
    ProcessedData_FolderPath (str): Path to the folder where the processed data will be saved.
    IDF_FileYear (str, optional): Year to be used for the datetime conversion. Default is '2013'.

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
        
    Eio_OutputFile_Dict_Filepath = os.path.join(filepaths["sim_results_folderpath"],"Eio_OutputFile.pickle")
                                                  
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
def update_simulation_information(csv_filepath, simulation_results_folderpath, status):
    """
    Updates the status of a simulation in the Simulation_Information CSV

    Args:
        csv_filepath (str): The file path to the Simulation_Information CSV
        simulation_results_folderpath (str): The folder path where results are stored for the simulation in question.
        status (str): The new status to be updated in the CSV file.
    
    Returns:
        None: The function updates the file in place and does not return any value.
    """

    with open(csv_filepath, 'r') as file:
        lines = file.readlines()
        lines = lines[1:] # Exclude Column Headers
        
    for line in lines:
        if simulation_results_folderpath == line.split(',')[3]:
            line_split = line.split(',')
            line_split[4] = status 
            
# =============================================================================
# Simulate Building - OLD
# =============================================================================

# This Function is Unused for now 
#  A revised version of this function will go into the wrapper application           

# def simulate_building(IDF_FilePath, Weather_FilePath, Simulation_Name, IDF_FileYear, Simulation_VariableNames, Sim_Start_Day, Sim_Start_Month, Sim_End_Day, Sim_End_Month, Sim_OutputVariable_ReportingFrequency, Sim_TimeStep, Completed_Simulation_FolderPath):
#     """
#     Simulates the energy performance of a building using an IDF and Weather File, Processes the results into a structured format.

#     This function performs the following steps:
#     1. Creates the necessary folder structure for the simulation results.
#     2. Copies the provided IDF and weather files to a temporary directory.
#     3. Loads and edits the IDF file to adjust the simulation parameters.
#     4. Runs simulations for each variable specified. 
#     5. Processes time series data and EIO output files, organizing the results in the appropriate folders.
#     6. Cleans up by deleting the temporary files and folders used during the simulation.

#     Parameters:
#     -----------
#     IDF_FilePath : str
#         The file path to the original IDF file.
#     Weather_FilePath : str
#         The file path to the weather file used for the simulation.
#     Simulation_Name : str
#         The name to assign to this simulation run.
#     IDF_FileYear : str
#         The year associated with the IDF file, used for processing the results.
#     Simulation_VariableNames : list of str
#         A list of variables to be simulated, for which results will be generated.
#     Sim_Start_Day : int
#         The starting day of the simulation period (1-31).
#     Sim_Start_Month : int
#         The starting month of the simulation period (1-12).
#     Sim_End_Day : int
#         The ending day of the simulation period (1-31).
#     Sim_End_Month : int
#         The ending month of the simulation period (1-12).
#     Sim_OutputVariable_ReportingFrequency : str
#         The reporting frequency for the simulation output variables (e.g., "Hourly", "Daily").
#     Sim_TimeStep : int
#         The simulation timestep (e.g., 1, 10, 15, 60 minutes).
#     Completed_Simulation_FolderPath : str
#         The folder path where the completed simulation files will be stored.

#     Returns: None
    
#     """
    
#     IDF_FileName = os.path.basename(IDF_FilePath)
#     Weather_FileName = os.path.basename(Weather_FilePath)
    
#     # =============================================================================
#     # Creating Completed Simulation Folder Structure
#     # =============================================================================
#     processedData_folderPath = os.path.join(Completed_Simulation_FolderPath, 'Sim_ProcessedData')
#     outputFiles_folderPath = os.path.join(Completed_Simulation_FolderPath, 'Sim_OutputFiles')

#     # =============================================================================
#     # Copying IDF and Weather Files to Temporary Folder
#     # =============================================================================

#     # Getting Temporary Folder Path
#     Temporary_FolderPath = os.path.join(Completed_Simulation_FolderPath, 'Temporary_FolderPath')
    
#     # Getting Temporary IDF/Weather File Paths
#     Temporary_IDF_FilePath = os.path.join(Temporary_FolderPath, IDF_FileName)
#     Temporary_Weather_FilePath = os.path.join(Temporary_FolderPath, Weather_FileName)
    
#     shutil.copy(IDF_FilePath, Temporary_IDF_FilePath)
#     shutil.copy(Weather_FilePath, Temporary_Weather_FilePath)
    
#     # =============================================================================
#     # Editing Current IDF File
#     # =============================================================================
    
#     # Loading Current IDF File
#     Current_IDFFile = op.Epm.load(Temporary_IDF_FilePath)
    
#     # Loading Current IDF File
#     Current_IDFFile = op.Epm.load(Temporary_IDF_FilePath)

#     # Editing RunPeriod
#     Current_IDF_RunPeriod = Current_IDFFile.RunPeriod.one()
#     Current_IDF_RunPeriod['begin_day_of_month'] = Sim_Start_Day
#     Current_IDF_RunPeriod['begin_month'] = Sim_Start_Month
#     Current_IDF_RunPeriod['end_day_of_month'] = Sim_End_Day
#     Current_IDF_RunPeriod['end_month' ]= Sim_End_Month

#     # Editing TimeStep
#     Current_IDF_TimeStep = Current_IDFFile.TimeStep.one()
    
#     # Getting Current Schedule
#     Current_ScheduleCompact = Current_IDFFile.Schedule_Compact
#     Current_ScheduleCompact_Records_Dict = Current_ScheduleCompact._records
    
#     # Creating Edited IDF File
#     Edited_IDFFile = Current_IDFFile
    
#     # Saving Edited IDF and Weather File in Results Folder
#     Edited_IDFFile_FilePath = os.path.join(Temporary_FolderPath, Simulation_Name,  'Edited_IDFFile.idf')
#     Edited_IDFFile.save(Edited_IDFFile_FilePath)
    
#     for OutputVariable in Simulation_VariableNames:
    
#         simulate_variable(Edited_IDFFile_FilePath, Weather_FilePath, Completed_Simulation_FolderPath, OutputVariable)
        
#         # Move and Rename CSV
#         csv_filename = OutputVariable.replace(' ', '_') + '.csv'
#         csv_source_filepath = os.path.join(Completed_Simulation_FolderPath, OutputVariable)
#         csv_destination_filepath = os.path.join(processedData_folderPath, csv_filename)
#         shutil.move(csv_source_filepath, csv_destination_filepath)
        
#     # Processing CSV Files
#     Process_TimeSeriesData(processedData_folderPath, processedData_folderPath, IDF_FileYear = '2013')
    
#     # Processing EIO Files
#     Eio_FilePath = os.path.join(outputFiles_folderPath,'eplusout.eio')
#     Process_Eio_OutputFile(Eio_FilePath, processedData_folderPath, IDF_FileYear = '2013')
    
#     for filepath in os.listdir(Completed_Simulation_FolderPath):
        
#        if os.path.isfile(filepath): 
            
#            destination_path = os.path.join(outputFiles_folderPath, os.path.basename(filepath))
#            shutil.move(filepath, destination_path)
    
#     # Delete Temporary Folder 
#     shutil.rmtree(Temporary_FolderPath)

      
        

    

