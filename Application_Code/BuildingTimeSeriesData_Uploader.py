import os
import pickle
import psycopg2
from psycopg2 import sql
import pandas as pd
import dateutil
from dateutil.parser import isoparse
import csv

import datetime as dt 

# Reviewed 

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
        date_obj = dt.datetime.strptime(original_date_str, '%Y-%m-%d %H:%M:%S')
        date_obj += dt.timedelta(days=1)
        formatted_datetime = date_obj.strftime('%Y-%m-%d %H:%M:%S')
    else:
        # Format the datetime normally
        formatted_datetime = f"{simulation_year}-{month}-{day} {time}"
    
    return formatted_datetime

# =============================================================================
# Get Last Datetime
# =============================================================================

def get_last_datetime(buildingid, variablename=None, subvariable_name=None):
    """
    Retrieves the last datetime entry from a time series data file for a given building ID, 
    and optionally filters by variable name and subvariable name.

    Args:
        buildingid (str): The ID of the building to search for in the time series data.
        timeseriesdata_information (str): The filepath of the CSV file containing time series data.
        variablename (str, optional): The name of the variable to filter by. 
        subvariable_name (str, optional): The name of the subvariable to filter by. 

    Returns:
        str or None: The last datetime entry for the specified filters, or None if no matching entry is found.
    """
    
    timeseriesdata_information_filepath = os.path.join(os.path.dirname(__file__), '..', 'Generated_Textfiles', 'TimeSeriesData_Information.csv')
    with open(timeseriesdata_information_filepath, 'r') as file:
        lines = file.readlines()

    # Filter lines by building ID
    search_lines = [line for line in lines if line.split(',')[0] == buildingid]
    
    # Further filter by variable name if specified
    if variablename:
        search_lines = [line for line in search_lines if line.split(',')[1] == variablename]

    # Further filter by subvariable name if specified
    if subvariable_name:
        search_lines = [line for line in search_lines if line.split(',')[3] == subvariable_name]

    # Get the last datetime from the filtered list
    if search_lines:
        last_datetime = search_lines[-1].split(',')[5]
    else:
        last_datetime = None  # Handle case where no matching lines are found
    
    return last_datetime 

# =============================================================================
# Check if a Particular Row has already been Uploaded
# =============================================================================

def datetime_already_uploaded(datetime, buildingid, variablename=None, subvariable_name=None):
    
    datetime_already_uploaded = False
    
    last_datetime = get_last_datetime(buildingid, variablename, subvariable_name)
    
    if last_datetime is not None:
        if last_datetime >= datetime: datetime_already_uploaded = True
   
    return datetime_already_uploaded     

# =============================================================================
# Uploade Datetime
# =============================================================================
def upload_datetime(conn_information, buildingid, datetime, timeresolution, variablename, schedulename, zonename, surfacename, systemnodename, value):
    
    insert_query = sql.SQL("""
        INSERT INTO timeseriesdata (buildingid, datetime, timeresolution, variablename, schedulename, zonename, surfacename, systemnodename, value)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """)
    
    conn = psycopg2.connect(conn_information)
    cur = conn.cursor()
    
    cur.execute(insert_query, (buildingid, datetime, timeresolution, variablename, schedulename, zonename, surfacename, systemnodename, value))
    conn.commit()
    
    cur.close()
    conn.close()

# =============================================================================
# Check if a Particular Building, Variable, or SubVariable has already been Uploaded
# =============================================================================

def already_uploaded(timeseriesdata_information, simulation_settings, buildingid, variable=None, subvariable=None):
    """
    Checks if a particular building, variable, or subvariable has already been uploaded by comparing sim end time 
    with existing entries in the time series data file.

    Args:
        timeseriesdata_information (str): The filepath to the CSV file containing time series data.
        simulation_information (dict): A dictionary containing simulation information, including the simulation end datetime.
        buildingid (str): The ID of the building to check for in the time series data.
        variable (str, optional): The variable name to filter by. Defaults to None, meaning no filtering by variable.
        subvariable (str, optional): The subvariable name to filter by. Defaults to None, meaning no filtering by subvariable.

    Returns:
        bool: True if data has already been uploaded. False otherwise.
    """

    already_uploaded = False  
    sim_end_datetime = str(simulation_settings["sim_end_datetime"])
    
    with open(timeseriesdata_information, 'r') as file:
        lines = file.readlines()
    
    for line in lines:
        fields = line.split(',')
        if fields[0] == buildingid:
            if variable is None or fields[1] == variable:
                if subvariable is None or fields[3] == subvariable:
                    if fields[5] == sim_end_datetime:
                        already_uploaded = True  
                        break  
        
    return already_uploaded

# =============================================================================
# Update TimeSeriesData_Information CSV
# =============================================================================
def update_last_datetime(new_datetime, buildingid, variable, subvariable=None):
    
    timeseriesdata_information_filepath = os.path.join(os.path.dirname(__file__), '..', 'Generated_Textfiles', 'TimeSeriesData_Information.csv')
    with open(timeseriesdata_information_filepath, 'r') as file:
        lines = file.readlines()
    
    # Find the line to update
    update_line_index = None
    for i, line in enumerate(lines):
        line_parts = line.split(',')
        if str(line_parts[0]) == buildingid and line_parts[1] == variable and (subvariable is None or line_parts[3] == subvariable):
            update_line_index = i
            break  # Exit the loop once the line is found
    
    # Update the line with the new datetime
    if update_line_index is not None:
        new_line_parts = lines[update_line_index].split(',')
        new_line_parts[5] = str(new_datetime)  # Update the datetime part
        lines[update_line_index] = ','.join(new_line_parts) + '\n'  # Reconstruct the line
    
    # Write the updated lines back to the file
    with open(timeseriesdata_information_filepath, 'w') as file:
        file.writelines(lines)
    
# =============================================================================
# Upload Time Series Data for One Variable 
# =============================================================================
def upload_variable_timeseriesdata(conn_information, buildingid, variablename, simulation_settings=None, timeseriesdata_csv_filepath=None, data=None,):
    
    # Can Provide the Dataframe directly or the CSV File Path
    
    timeseriesdata_information_filepath = os.path.join(os.path.dirname(__file__), '..', 'Generated_Textfiles', 'TimeSeriesData_Information.csv')   
    
    # Prepare Query
    table_df_columns = ['buildingid', 'datetime', 'timeresolution', 'variablename', 'zonename', 'surfacename', 'systemnodename', 'value']
    insert_query = sql.SQL("""
        INSERT INTO timeseriesdata (buildingid, datetime, timeresolution, variablename, schedulename, zonename, surfacename, systemnodename, value)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """)
  
    simulation_year = str(simulation_settings["sim_end_datetime"].year)
    if data is None: data = pd.read_csv(timeseriesdata_csv_filepath)
    
    timeresolution = simulation_settings["sim_timestep"]
    
    caught_up = False
    
    if variablename == 'Schedule_Value':
            variablename_value = 'Schedule Value'
            zonename_value = 'NA'
            surfacename_value = 'NA'
            systemnodename_value = 'NA'
            schedulenames = data.columns.tolist()  
            schedulenames.remove('Date/Time')
            
            for schedulename in schedulenames:
                schedulename_value = schedulename.split(':')[0].strip()
                if not already_uploaded(timeseriesdata_csv_filepath, simulation_settings, buildingid, variablename_value, schedulename_value): # Check Schedule already Uploaded
                    for _, row in data.iterrows():
                        datetime_value = format_datetime(simulation_year, row['Date/Time'].strip())
                        if caught_up or not datetime_already_uploaded(datetime_value, buildingid, timeseriesdata_csv_filepath, variablename, schedulename_value): # Check Datetime already uploaded
                            caught_up = True
                            table_tilevalue = row[schedulename]
                            upload_datetime(conn_information, buildingid, datetime_value, timeresolution, variablename, schedulename_value, zonename_value, surfacename_value, systemnodename_value, table_tilevalue)
                            update_last_datetime(timeseriesdata_information_filepath, datetime_value, buildingid, variablename_value, schedulename_value)
            
    elif variablename.startswith('Facility') or variablename.startswith('Site'):  
        variablename_value = variablename.replace('_', ' ').strip()
        schedulename_value = 'NA'
        zonename_value = 'NA'
        surfacename_value = 'NA'
        systemnodename_value = 'NA'
        
        for _, row in data.iterrows():
            datetime_value = format_datetime(simulation_year, row['Date/Time'].strip())
            if caught_up or not datetime_already_uploaded(datetime_value, buildingid, timeseriesdata_csv_filepath, variablename): # Check datetime already uploaded
                caught_up = True
                columnname = data.columns[1]
                table_tilevalue = row[columnname]
                upload_datetime(conn_information, buildingid, datetime_value, timeresolution, variablename_value, schedulename_value, zonename_value, surfacename_value, systemnodename_value, table_tilevalue)
                update_last_datetime(timeseriesdata_information_filepath, datetime_value, buildingid, variablename_value)
                
    elif variablename.startswith('Zone'): 
        variablename_value = variablename.replace('_', ' ').strip()
        schedulename_value = 'NA'
        surfacename_value = 'NA'
        systemnodename_value = 'NA'
           
        for columnname in data.columns:    
                if columnname != 'Date/Time': 
                    zonename_value = columnname.split(':')[0].strip()
                    if not already_uploaded(timeseriesdata_csv_filepath, simulation_settings, buildingid, variablename, zonename_value): # Check Zone already uploaded
                        for _, row in data.iterrows():
                            datetime_value = format_datetime(simulation_year, row['Date/Time'].strip())
                            if caught_up or not datetime_already_uploaded(datetime_value, buildingid, timeseriesdata_csv_filepath, variablename_value, zonename_value): # Check datetime already uploaded
                                caught_up = True
                                table_tilevalue = row[columnname]
                                upload_datetime(conn_information, buildingid, datetime_value, timeresolution, variablename_value, schedulename_value, zonename_value, surfacename_value, systemnodename_value, table_tilevalue)
                                update_last_datetime(timeseriesdata_information_filepath, datetime_value, buildingid, variablename_value, zonename_value)
                            

    elif variablename.startswith('Surface'):
        variablename_value = variablename.replace('_', ' ').strip()
        schedulename_value = 'NA'
        zonename_value = 'NA'
        systemnodename_value = 'NA'
         
        for columnname in data.columns:
                if columnname != 'Date/Time':
                    surfacename_value = columnname.split(':')[0].strip()
                    if not already_uploaded(timeseriesdata_csv_filepath, simulation_settings, buildingid, variablename, surfacename_value): # Check Surface already uploaded
                        for _, row in data.iterrows():
                            datetime_value = format_datetime(simulation_year, row['Date/Time'].strip())
                            if caught_up or not datetime_already_uploaded(datetime_value, buildingid, variablename_value, surfacename_value): # Check datetime already uploaded
                                caught_up = True
                                table_tilevalue = row[columnname]
                                upload_datetime(conn_information, buildingid, datetime_value, timeresolution, variablename_value, schedulename_value, zonename_value, surfacename_value, systemnodename_value, table_tilevalue)
                                update_last_datetime(timeseriesdata_information_filepath, datetime_value, buildingid, variablename_value, surfacename_value)
            
    elif variablename.startswith('System_node'):
        variablename_value = variablename.replace('_', ' ').strip()
        schedulename_value = 'NA'
        zonename_value = 'NA'
        surfacename_value = 'NA'
            
        for columnname in data.columns:
                if columnname != 'Date/Time':
                    systemnodename_value = columnname.split(':')[0].strip()
                    if not already_uploaded(timeseriesdata_csv_filepath, simulation_settings, buildingid, variablename, systemnodename_value): # Check System Node already uploaded
                        for _, row in data.iterrows():
                            datetime_value = format_datetime(simulation_year, row['Date/Time'].strip())
                            if caught_up or not datetime_already_uploaded(datetime_value, buildingid, variablename_value, systemnodename_value): # Check datetime already uploaded
                                caught_up = True
                                table_tilevalue = row[columnname]
                                upload_datetime(conn_information, buildingid, datetime_value, timeresolution, variablename_value, schedulename_value, zonename_value, surfacename_value, systemnodename_value, table_tilevalue)
                                update_last_datetime(timeseriesdata_information_filepath, datetime_value, buildingid, variablename_value, systemnodename_value)
                                
# =============================================================================
# Upload Time Series Data from Pickle File
# =============================================================================

def upload_timeseriesdata_frompickle(conn_information, buildingid, simulation_settings, timeseriesdata_pickle_filepath):
    
    with open(timeseriesdata_pickle_filepath, 'rb') as file:
        timeseriesdata = pickle.load(file)
        
    for variable in timeseriesdata.keys():
        variablename = variable.replace('_', ' ').replace('.csv', '')
        upload_variable_timeseriesdata(conn_information, buildingid, variablename, simulation_settings=simulation_settings, data=timeseriesdata[variable])    
        
# =============================================================================
# TEST
# =============================================================================

def test():
    
    pickle_filepath = r"D:\Building_Modeling_Code\Results\Processed_BuildingSim_Data\ASHRAE_2013_Albuquerque_ApartmentHighRise\Sim_ProcessedData\IDF_OutputVariables_DictDF.pickle"
    
    conn_information = "dbname=Building_Models user=kasey password=OfficeLarge"
    
    sim_start_datetime = dt.datetime(2013, 5, 1, 0, 0, 0, 0)
    sim_end_datetime = dt.datetime(2013, 5, 2, 0, 0, 0, 0)
    
    # Simulation settings dictionary
    simulation_settings = {
        "sim_start_datetime": sim_start_datetime, # Example start datetime
        "sim_end_datetime": sim_end_datetime,    # Example end datetime
        "sim_timestep": 5,                           # Example timestep in minutes
        "sim_output_variable_reporting_frequency": 'timestep', # Example reporting frequency
        "keepfile": "all"
    }
    
    upload_timeseriesdata_frompickle(conn_information, 1, simulation_settings, pickle_filepath)
    
test()