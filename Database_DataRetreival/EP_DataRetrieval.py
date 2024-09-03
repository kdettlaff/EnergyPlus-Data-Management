
# =============================================================================
# Import Required Modules
# =============================================================================

import os
import pandas as pd
import pickle
from re import S
import psycopg2

# =============================================================================
# Retreive data for specified time range, building, and variable
# =============================================================================

def retrieve_timeseriesdata(conn_information, buildingid=None, startdatetime=None, enddatetime=None, timeresolution=None, variable=None, subvariabletype=None, subvariable=None):
    """
    Retrieve time series data for a specified building, time range, and variable.

    Parameters:
    -----------
    conn_information : str
        Database connection string containing the necessary information to connect to the database.
    buildingid : int, optional
        Identifier for the building from which data is to be retrieved.
    startdatetime : datetime, optional
        Start of the time range for which data is to be retrieved.
    enddatetime : datetime, optional
        End of the time range for which data is to be retrieved.
    timeresolution : int, optional
        The time resolution of the data in minutes.
    variable : str, optional
        The name of the variable for which data is to be retrieved.
    subvariabletype : str, optional
        The type of subvariable to filter the data by (e.g., 'schedulename', 'zonename', 'surfacename', 'systemnodename').
    subvariable : str, optional
        The name of the subvariable to filter the data by.

    Returns:
    --------
    pandas.DataFrame
        A DataFrame with the relevant columns based on the query, including:
        - 'timeseriesdataid': The unique identifier for each data point.
        - 'datetime': The timestamp of each data point.
        - 'value': The corresponding value for the specified variable at each timestamp.
        - Additional columns depending on the parameters provided.

    Example:
    --------
    df = retrieve_data(conn_info, buildingid=1, startdatetime='2024-01-01 00:00:00', enddatetime='2024-01-02 00:00:00', 
                       timeresolution=5, variable='Facility Total HVAC Electric Demand Power')
                       
    Warning:
    --------
    Lack of filters could cause the query to return a large amount of data, which could cause performance issues. 
    
    """

    # Connect to the database
    conn = psycopg2.connect(conn_information)
    cur = conn.cursor()

    # Initialize subvariable parameters with default values
    schedulename = 'NA'
    zonename = 'NA'
    surfacename = 'NA'
    systemnodename = 'NA'
    
    # Set the appropriate subvariable based on subvariabletype
    if subvariabletype == 'schedulename':
        schedulename = subvariable
    elif subvariabletype == 'zonename':
        zonename = subvariable
    elif subvariabletype == 'surfacename':
        surfacename = subvariable
    elif subvariabletype == 'systemnodename':
        systemnodename = subvariable

    # Build the SELECT part of the query
    select_columns = ["timeseriesdataid"]

    if not buildingid:
        select_columns.append("buildingid")
    
    if variable is None:
        select_columns.append("variable")
    
    if schedulename != 'NA':
        select_columns.append("schedulename")
    
    if zonename != 'NA':
        select_columns.append("zonename")
    
    if surfacename != 'NA':
        select_columns.append("surfacename")
    
    if systemnodename != 'NA':
        select_columns.append("systemnodename")
        
    select_columns.append("datetime")
    select_columns.append("value")
    
    # Convert list of columns to a string for the SELECT clause
    select_clause = ", ".join(select_columns)

    # Build the base query
    query = f"SELECT {select_clause} FROM timeseriesdata WHERE 1=1"
    params = []

    # Add filters based on provided parameters
    if buildingid:
        query += " AND buildingid = %s"
        params.append(buildingid)
    
    if startdatetime:
        query += " AND datetime >= %s"
        params.append(startdatetime)
    
    if enddatetime:
        query += " AND datetime <= %s"
        params.append(enddatetime)
    
    if timeresolution:
        query += " AND timeresolution = %s"
        params.append(timeresolution)
    
    if variable:
        query += " AND variable = %s"
        params.append(variable)
    
    if schedulename != 'NA':
        query += " AND schedulename = %s"
        params.append(schedulename)
    
    if zonename != 'NA':
        query += " AND zonename = %s"
        params.append(zonename)
    
    if surfacename != 'NA':
        query += " AND surfacename = %s"
        params.append(surfacename)
    
    if systemnodename != 'NA':
        query += " AND systemnodename = %s"
        params.append(systemnodename)

    # Execute the query
    cur.execute(query, params)
    data = cur.fetchall()

    # Close the connection
    cur.close()
    conn.close()

    # Convert the data to a DataFrame with dynamic columns
    df = pd.DataFrame(data, columns=select_columns)

    # Return the DataFrame
    return df

# =============================================================================
# Retreive eiotabledata for specified buildingid, tablename, zonename, variablename
# =============================================================================

def retrieve_eiotabledata(conn_information, buildingid=None, tablename=None, zonename=None, variablename=None):
    """
    Retrieve data from the eiotabledata table for a specified building, table, zone, and variable.

    Parameters:
    -----------
    conn_information : str
        Database connection string containing the necessary information to connect to the database.
    buildingid : int, optional
        Identifier for the building from which data is to be retrieved.
    tablename : str, optional
        The name of the table from which data is to be retrieved.
    zonename : str, optional
        The name of the zone from which data is to be retrieved.
    variablename : str, optional
        The name of the variable for which data is to be retrieved.

    Returns:
    --------
    pandas.DataFrame
        A DataFrame with the relevant columns based on the query, including:
        - 'buildingid': The identifier for the building.
        - 'tablename': The name of the table.
        - 'zonename': The name of the zone.
        - 'variablename': The name of the variable.
        - 'stringvalue': The corresponding string value for the specified variable (if applicable).
        - 'floatvalue': The corresponding float value for the specified variable (if applicable).

    Example:
    --------
    df = retrieve_eiotabledata(conn_info, buildingid=1, tablename='ZoneInfo', zonename='Zone1', variablename='Area')

    Warning:
    --------
    Lack of filters could cause the query to return a large amount of data, which could cause performance issues. 
    """

    # Connect to the database
    conn = psycopg2.connect(conn_information)
    cur = conn.cursor()
    
    # Build the SELECT part of the query
    select_columns = []

    if buildingid is None:
        select_columns.append("buildingid")
    
    if tablename is None:
        select_columns.append("tablename")
    
    if zonename is None:
        select_columns.append("zonename")
    
    if variablename is None:
        select_columns.append("variablename")
    
    select_columns.append("stringvalue")
    select_columns.append("floatvalue")

    # Convert list of columns to a string for the SELECT clause
    select_clause = ", ".join(select_columns)

    # Build the base query
    query = f"SELECT {select_clause} FROM eiotabledata WHERE 1=1"
    params = []

    # Add filters based on provided parameters
    if buildingid:
        query += " AND buildingid = %s"
        params.append(buildingid)
    
    if tablename:
        query += " AND tablename = %s"
        params.append(tablename)
    
    if zonename:
        query += " AND zonename = %s"
        params.append(zonename)
    
    if variablename:
        query += " AND variablename = %s"
        params.append(variablename)

    # Execute the query
    cur.execute(query, params)
    data = cur.fetchall()

    # Close the connection
    cur.close()
    conn.close()

    # Convert the data to a DataFrame with dynamic columns
    df = pd.DataFrame(data, columns=select_columns)

    # Return the DataFrame
    return df