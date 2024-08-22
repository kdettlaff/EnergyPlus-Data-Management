import os
import pickle
import psycopg2
from psycopg2 import sql
import pandas as pd
import dateutil
from dateutil.parser import isoparse
import csv

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
# Get Last Datetime
# =============================================================================

def get_last_datetime(buildingid, timeseriesdata_information, variablename=None, subvariable_name=None):
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

    with open(timeseriesdata_information, 'r') as file:
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

def datetime_already_uploaded(datetime, buildingid, timeseriesdata_information, variablename=None, subvariable_name=None):
    
    datetime_already_uploaded = False
    
    last_datetime = get_last_datetime(buildingid, timeseriesdata_information, variablename, subvariable_name)
    
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
# Update Time Series Data Information CSV
# =============================================================================
def update_last_datetime(timeseriesdata_information, new_datetime, buildingid, variable, subvariable=None):
    
    # Read the existing file
    with open(timeseriesdata_information, 'r') as file:
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
    with open(timeseriesdata_information, 'w') as file:
        file.writelines(lines)
    
# =============================================================================
# Upload Time Series Data for One Variable - NEW
# =============================================================================

def upload_variable_timeseriesdata(conn_information, simulation_settings, filepaths, timeseriesdata_csv_filepath, buildingid, variable):

    timeseriesdata_information = filepaths["timeseriesdata_information"]    
    
    # Prepare Query
    table_df_columns = ['buildingid', 'datetime', 'timeresolution', 'variablename', 'zonename', 'surfacename', 'systemnodename', 'value']
    insert_query = sql.SQL("""
        INSERT INTO timeseriesdata (buildingid, datetime, timeresolution, variablename, schedulename, zonename, surfacename, systemnodename, value)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """)
  
    simulation_year = str(simulation_settings["sim_end_datetime"].year)
    variablename = os.path.basename(timeseriesdata_csv_filepath).replace('.csv', '')
    data = pd.read_csv(timeseriesdata_csv_filepath)
    
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
                            update_last_datetime(timeseriesdata_information, datetime_value, buildingid, variablename_value, schedulename_value)
            
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
                update_last_datetime(timeseriesdata_information, datetime_value, buildingid, variablename_value)
                
    elif variablename.startswith('Zone'): 
        variablename_value = variablename.replace('_', ' ').strip()
        schedulename_value = 'NA'
        surfacename_value = 'NA'
        systemnodename_value = 'NA'
           
        for columnname in data.columns:    
                if columnname != 'Date/Time': 
                    zonename_value = columnname.split(':')[0].strip()
                    if not already_uploaded(timeseriesdata_csv_filepath, simulation_settings, buildingid, variable, zonename_value): # Check Zone already uploaded
                        for _, row in data.iterrows():
                            datetime_value = format_datetime(simulation_year, row['Date/Time'].strip())
                            if caught_up or not datetime_already_uploaded(datetime_value, buildingid, timeseriesdata_csv_filepath, variablename_value, zonename_value): # Check datetime already uploaded
                                caught_up = True
                                table_tilevalue = row[columnname]
                                upload_datetime(conn_information, buildingid, datetime_value, timeresolution, variablename_value, schedulename_value, zonename_value, surfacename_value, systemnodename_value, table_tilevalue)
                                update_last_datetime(timeseriesdata_information, datetime_value, buildingid, variablename_value, zonename_value)
                            

    elif variablename.startswith('Surface'):
        variablename_value = variablename.replace('_', ' ').strip()
        schedulename_value = 'NA'
        zonename_value = 'NA'
        systemnodename_value = 'NA'
         
        for columnname in data.columns:
                if columnname != 'Date/Time':
                    surfacename_value = columnname.split(':')[0].strip()
                    if not already_uploaded(timeseriesdata_csv_filepath, simulation_settings, buildingid, variable, surfacename_value): # Check Surface already uploaded
                        for _, row in data.iterrows():
                            datetime_value = format_datetime(simulation_year, row['Date/Time'].strip())
                            if caught_up or not datetime_already_uploaded(datetime_value, buildingid, variablename_value, surfacename_value): # Check datetime already uploaded
                                caught_up = True
                                table_tilevalue = row[columnname]
                                upload_datetime(conn_information, buildingid, datetime_value, timeresolution, variablename_value, schedulename_value, zonename_value, surfacename_value, systemnodename_value, table_tilevalue)
                                update_last_datetime(timeseriesdata_information, datetime_value, buildingid, variablename_value, surfacename_value)
            
    elif variablename.startswith('System_node'):
        variablename_value = variablename.replace('_', ' ').strip()
        schedulename_value = 'NA'
        zonename_value = 'NA'
        surfacename_value = 'NA'
            
        for columnname in data.columns:
                if columnname != 'Date/Time':
                    systemnodename_value = columnname.split(':')[0].strip()
                    if not already_uploaded(timeseriesdata_csv_filepath, simulation_settings, buildingid, variable, systemnodename_value): # Check System Node already uploaded
                        for _, row in data.iterrows():
                            datetime_value = format_datetime(simulation_year, row['Date/Time'].strip())
                            if caught_up or not datetime_already_uploaded(datetime_value, buildingid, variablename_value, systemnodename_value): # Check datetime already uploaded
                                caught_up = True
                                table_tilevalue = row[columnname]
                                upload_datetime(conn_information, buildingid, datetime_value, timeresolution, variablename_value, schedulename_value, zonename_value, surfacename_value, systemnodename_value, table_tilevalue)
                                update_last_datetime(timeseriesdata_information, datetime_value, buildingid, variablename_value, systemnodename_value)
                                
# =============================================================================
# Upload Time Series Data for One Variable - OLD
# =============================================================================

# def upload_variable_timeseriesdata(conn_information, simulation_settings, filepaths, timeseriesdata_csv_filepath, buildingid, variable):
    
#     """
#     Uploads time series data from a CSV file to a PostgreSQL database

#     Args:
#         conn_information (str): The connection string or information required to connect to the PostgreSQL database.
#         csv_filepath (str): The file path to the CSV file containing the time series data.
#         building_id (str): The ID of the building associated with the data.
#         timeresolution (str): The time resolution of the data (e.g., hourly, daily).
#         sim_end_datetime (datetime): The final datetime for the simulation

#     Process:
#         1. Establishes a connection to the PostgreSQL database using the provided connection information.
#         2. Reads the CSV file, ignoring the header row.
#         3. Determines the type of variable being processed based on the CSV file name.
#         4. For each row of data, it checks if the data has already been uploaded.
#            - If not, it inserts the data into the `timeseriesdata` table.
#         5. Commits the transactions and closes the database connection.
        
#     Returns:
#         None

#     Example Usage:
#         >>> conn_info = "dbname=Building_Models user=postgres password=secret host=localhost"
#         >>> upload_variable_timeseriesdata(conn_info, 'path/to/data.csv', 'Building_1', 'hourly', 2024)

#     Notes:
#         - The CSV file is expected to have a 'Date/Time' column and one or more data columns.
#         - The function enures that the database connection is always closed, even if an exception occurs during processing. 
        
# """
    
#     # Create Time Series Data Information CSV if it does not already exist
#     timeseriesdata_information = filepaths["timeseriesdata_information"]
#     if not os.path.exists(timeseriesdata_information):
#         with open(timeseriesdata_information, 'w') as file:
#             file.write('BuildingID,Variable Name,Variable Type,SubVariable Name,Sim Start Datetime,Last Uploaded Datetime\n')
            
#     table_df_columns = ['buildingid', 'datetime', 'timeresolution', 'variablename', 'zonename', 'surfacename', 'systemnodename', 'value']
    
#     insert_query = sql.SQL("""
#         INSERT INTO timeseriesdata (buildingid, datetime, timeresolution, variablename, schedulename, zonename, surfacename, systemnodename, value)
#         VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
#     """)
    
#     simulation_year = str(simulation_settings["sim_end_datetime"].year)
    
#     variablename = os.path.basename(timeseriesdata_csv_filepath).replace('.csv', '')
#     data = pd.read_csv(timeseriesdata_csv_filepath)
    
#     caught_up = False  # Initialize the caught_up variable
    
#     conn = psycopg2.connect(conn_information)
#     cur = conn.cursor()

#     try:

#         if variablename == 'Schedule_Value':
#             variablename_value = 'Schedule Value'
#             zonename_value = 'NA'
#             surfacename_value = 'NA'
#             systemnodename_value = 'NA'
#             schedulenames = data.columns.tolist()  
#             schedulenames.remove('Date/Time')
            
#             for schedulename in schedulenames:
#                 schedulename_value = schedulename.split(':')[0].strip()
#                 if not variable_already_uploaded(conn_information, simulation_settings, buildingid, variable): # Potential Issue Here
#                     for _, row in data.iterrows():
#                         datetime_value = format_datetime(simulation_year, row['Date/Time'].strip())
#                         if caught_up or not datetime_already_uploaded(datetime_value, buildingid, variablename_value, schedulename_value):#DEBUG this could be causing the exception
#                             caught_up = True
#                             table_tilevalue = row[schedulename]
#                             cur.execute(insert_query, (buildingid, datetime_value, simulation_settings["timestep"], variablename_value, schedulename_value, zonename_value, surfacename_value, systemnodename_value, table_tilevalue))
#                             conn.commit()

#         elif variablename.startswith('facility') or variablename.startswith('site'):  
#             variablename_value = variablename.replace('_', ' ').strip()
#             schedulename_value = 'NA'
#             zonename_value = 'NA'
#             surfacename_value = 'NA'
#             systemnodename_value = 'NA'
            
#             if not variable_already_uploaded(buildingid, variablename_value):
#                 for _, row in data.iterrows():
#                     datetime_value = format_datetime(simulation_year, row['Date/Time'].strip())
#                     if caught_up or not datetime_already_uploaded(datetime_value, buildingid, variablename_value):
#                         caught_up = True
#                         columnname = data.columns[1]
#                         table_tilevalue = row[columnname]
#                         cur.execute(insert_query, (buildingid, datetime_value, simulation_settings["timestep"], variablename_value, schedulename_value, zonename_value, surfacename_value, systemnodename_value, table_tilevalue))
#                         conn.commit()
                
#         elif variablename.startswith('zone'): 
#             variablename_value = variablename.replace('_', ' ').strip()
#             schedulename_value = 'NA'
#             surfacename_value = 'NA'
#             systemnodename_value = 'NA'
            
#             for _, row in data.iterrows():
#                 datetime_value = format_datetime(simulation_year, row['Date/Time'].strip())
#                 for columnname in data.columns:
#                     if columnname != 'Date/Time':
#                         zonename_value = columnname.split(':')[0].strip()
#                         if caught_up or not datetime_already_uploaded(datetime_value, buildingid, variablename_value, zonename_value):
#                             caught_up = True
#                             table_tilevalue = row[columnname]
#                             cur.execute(insert_query, (buildingid, datetime_value, simulation_settings["timestep"], variablename_value, schedulename_value, zonename_value, surfacename_value, systemnodename_value, table_tilevalue))
#                             conn.commit()

#         elif variablename.startswith('surface'):
#             variablename_value = variablename.replace('_', ' ').strip()
#             schedulename_value = 'NA'
#             zonename_value = 'NA'
#             systemnodename_value = 'NA'
            
#             for _, row in data.iterrows():
#                 datetime_value = format_datetime(simulation_year, row['Date/Time'].strip())
#                 for columnname in data.columns:
#                     if columnname != 'Date/Time':
#                         surfacename_value = columnname.split(':')[0].strip()
#                         if caught_up or not datetime_already_uploaded(datetime_value, buildingid, variablename_value, surfacename_value):
#                             caught_up = True
#                             table_tilevalue = row[columnname]
#                             cur.execute(insert_query, (buildingid, datetime_value, simulation_settings["timestep"], variablename_value, schedulename_value, zonename_value, surfacename_value, systemnodename_value, table_tilevalue))
#                             conn.commit()
            
#         elif variablename.startswith('system_node'):
#             variablename_value = variablename.replace('_', ' ').strip()
#             schedulename_value = 'NA'
#             zonename_value = 'NA'
#             surfacename_value = 'NA'
            
#             for _, row in data.iterrows():
#                 datetime_value = format_datetime(simulation_year, row['Date/Time'].strip())
#                 for columnname in data.columns:
#                     if columnname != 'Date/Time':
#                         systemnodename_value = columnname.split(':')[0].strip()
#                         if caught_up or not datetime_already_uploaded(datetime_value, buildingid, variablename_value, systemnodename_value):
#                             caught_up = True
#                             table_tilevalue = row[columnname]
#                             cur.execute(insert_query, (buildingid, datetime_value, simulation_settings["timestep"], variablename_value, schedulename_value, zonename_value, surfacename_value, systemnodename_value, table_tilevalue))
#                             conn.commit()
                            
#         cur.close()
#         conn.close() 

#     except Exception as e:
        
#         cur.close()
#         conn.close()    