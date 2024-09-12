
# Required Modules

import os
import pandas as pd
import csv
import pickle
import psycopg2
from psycopg2 import sql

import datetime as dt 
import dateutil
from dateutil.parser import isoparse
import time

# =============================================================================
# Format Time
# ============================================================================

def convert_seconds_to_hhmmss(seconds):
    hours, remainder = divmod(int(seconds), 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02}:{minutes:02}:{seconds:02}"

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
# Get Unique Rows: Prevent Duplicates 
# =============================================================================

# Query Database to retrieve rows that match conditions for potential duplicates.
# Compare fetched data with new DataFrame.
# Return only new rows.

def get_unique_rows(conn_information, tablename, df, conditions):
    """
    Fetch rows from the database that match the specified conditions,
    compare with the new DataFrame, and return only the new (unique) rows.
    
    Args:
    - conn_information (str): Connection information for the database.
    - tablename (str): Name of the table to query.
    - df (pd.DataFrame): The new DataFrame to compare.
    - conditions (dict): Dictionary of column names and their corresponding values for filtering.
    
    Returns:
    - new_rows_df (pd.DataFrame): DataFrame containing only the unique rows not found in the database.
    """

    # Build the WHERE clause dynamically from the conditions dictionary
    where_clause = " AND ".join([f"{col} = %s" for col in conditions.keys()])
    query = f"SELECT * FROM {tablename} WHERE {where_clause}"
    
    # Connect to the database
    conn = psycopg2.connect(conn_information)
    cur = conn.cursor()

    # Execute the query with the values from the conditions dictionary
    cur.execute(query, tuple(conditions.values()))
    
    # Fetch all the matching rows from the database
    rows = cur.fetchall()
    
    # Close the cursor and connection
    cur.close()
    conn.close()
    
    # Convert the fetched rows to a DataFrame
    if rows:
        # Assuming the database table has the same columns as the DataFrame
        existing_df = pd.DataFrame(rows, columns=df.columns)
    else:
        existing_df = pd.DataFrame(columns=df.columns)  # Empty DataFrame if no matches found

    # Merge the new DataFrame with the fetched data to find unique rows
    # 'left_only' will identify rows that exist in 'df' but not in 'existing_df'
    merged_df = df.merge(existing_df, how='left', on=list(conditions.keys()), indicator=True)
    new_rows_df = merged_df[merged_df['_merge'] == 'left_only'].drop('_merge', axis=1)

    return new_rows_df

# =============================================================================
# Upload Data to Database
# =============================================================================

def upload_df_to_db(conn_information, tablename, df):
    """
    Upload a Pandas DataFrame to a PostgreSQL database table in bulk.
    
    Args:
    - conn_information (str): Connection information for the PostgreSQL database.
    - tablename (str): Name of the table in the database.
    - df (pd.DataFrame): DataFrame to be uploaded. Column names in the DataFrame must match those in the table.
    
    Returns:
    - None
    """
  
    # Prepare the SQL INSERT statement dynamically based on DataFrame column names
    columns = df.columns.tolist()  # Get list of DataFrame column names
    columns_str = ', '.join(columns)  # Convert column names to a string for SQL query
    placeholders = ', '.join(['%s'] * len(columns))  # Create placeholders for values
    
    # SQL INSERT query (dynamically constructed)
    insert_query = f"INSERT INTO {tablename} ({columns_str}) VALUES {placeholders}"
    
    # Insert rows into the database in bulk
    # Convert DataFrame to a list of tuples
    rows = [tuple(x) for x in df.to_numpy()]
    
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(conn_information)
    cur = conn.cursor()

    # Bulk insert using `execute()` with multiple values at once
    args_str = ','.join(cur.mogrify(f"({placeholders})", row).decode('utf-8') for row in rows)
    bulk_insert_query = f"INSERT INTO {tablename} ({columns_str}) VALUES {args_str}"
    
    # Execute the bulk insert
    cur.execute(bulk_insert_query)

    # Commit the transaction to save the changes
    conn.commit()

    # Step 5: Close the cursor and connection
    cur.close()
    conn.close()
    
def timeseriesdata_format_df(df, buildingid, variablename, simulation_year):
    """
    Formats time series data into a DataFrame suitable for further processing.
    
    Args:
    - df (pd.DataFrame): Original DataFrame to format.
    - buildingid (str): The ID of the building.
    - variablename (str): The variable name for processing.
    - simulation_year (int): The year of the simulation.
    
    Returns:
    - new_df (pd.DataFrame): A formatted DataFrame with proper structure.
    """
    
    # Initialize a list to store rows instead of appending to a DataFrame in each loop
    rows_list = []
    
    # Process based on the variable type
    if variablename == 'Schedule_Value':
        variablename_value = 'Schedule Value'
        zonename_value = 'NA'
        surfacename_value = 'NA'
        systemnodename_value = 'NA'
        schedulenames = df.columns.tolist()
        schedulenames.remove('Date/Time')

        for schedulename in schedulenames:
            schedulename_value = schedulename.split(':')[0].strip()
            for _, row in df.iterrows():
                datetime_value = format_datetime(simulation_year, row['Date/Time'].strip())
                table_cell_value = row[schedulename]
                new_row = {'buildingid': buildingid, 
                           'datetime_value': datetime_value, 
                           'variablename': variablename_value, 
                           'schedulename': schedulename_value,
                           'zonename': zonename_value,
                           'surfacename': surfacename_value,
                           'systemnodename': systemnodename_value,
                           'value': table_cell_value}
                rows_list.append(new_row)

    elif variablename.startswith('Facility') or variablename.startswith('Site'):  
        variablename_value = variablename.replace('_', ' ').strip()
        schedulename_value = 'NA'
        zonename_value = 'NA'
        surfacename_value = 'NA'
        systemnodename_value = 'NA'

        for _, row in df.iterrows():
            datetime_value = format_datetime(simulation_year, row['Date/Time'].strip())
            columnname = df.columns[1]  # Assuming data is in the second column
            table_cell_value = row[columnname]
            new_row = {'buildingid': buildingid, 
                       'datetime_value': datetime_value, 
                       'variablename': variablename_value, 
                       'schedulename': schedulename_value,
                       'zonename': zonename_value,
                       'surfacename': surfacename_value,
                       'systemnodename': systemnodename_value,
                       'value': table_cell_value}
            rows_list.append(new_row)
            
    elif variablename.startswith('Zone'):
        variablename_value = variablename.replace('_', ' ').strip()
        schedulename_value = 'NA'
        surfacename_value = 'NA'
        systemnodename_value = 'NA'

        for columnname in df.columns:
            if columnname != 'Date/Time':
                zonename_value = columnname.split(':')[0].strip()
                for _, row in df.iterrows():
                    datetime_value = format_datetime(simulation_year, row['Date/Time'].strip())
                    table_cell_value = row[columnname]
                    new_row = {'buildingid': buildingid, 
                               'datetime_value': datetime_value, 
                               'variablename': variablename_value, 
                               'schedulename': schedulename_value,
                               'zonename': zonename_value,
                               'surfacename': surfacename_value,
                               'systemnodename': systemnodename_value,
                               'value': table_cell_value}
                    rows_list.append(new_row)

    elif variablename.startswith('Surface'):
        variablename_value = variablename.replace('_', ' ').strip()
        schedulename_value = 'NA'
        zonename_value = 'NA'
        systemnodename_value = 'NA'

        for columnname in df.columns:
            if columnname != 'Date/Time':
                surfacename_value = columnname.split(':')[0].strip()
                for _, row in df.iterrows():
                    datetime_value = format_datetime(simulation_year, row['Date/Time'].strip())
                    table_cell_value = row[columnname]
                    new_row = {'buildingid': buildingid, 
                               'datetime_value': datetime_value, 
                               'variablename': variablename_value, 
                               'schedulename': schedulename_value,
                               'zonename': zonename_value,
                               'surfacename': surfacename_value,
                               'systemnodename': systemnodename_value,
                               'value': table_cell_value}
                    rows_list.append(new_row)

    elif variablename.startswith('System_node'):
        variablename_value = variablename.replace('_', ' ').strip()
        schedulename_value = 'NA'
        zonename_value = 'NA'
        surfacename_value = 'NA'

        for columnname in df.columns:
            if columnname != 'Date/Time':
                systemnodename_value = columnname.split(':')[0].strip()
                for _, row in df.iterrows():
                    datetime_value = format_datetime(simulation_year, row['Date/Time'].strip())
                    table_cell_value = row[columnname]
                    new_row = {'buildingid': buildingid, 
                               'datetime_value': datetime_value, 
                               'variablename': variablename_value, 
                               'schedulename': schedulename_value,
                               'zonename': zonename_value,
                               'surfacename': surfacename_value,
                               'systemnodename': systemnodename_value,
                               'value': table_cell_value}
                    rows_list.append(new_row)

    # Convert the list of rows into a DataFrame
    new_df = pd.DataFrame(rows_list)
    
    return new_df
                                