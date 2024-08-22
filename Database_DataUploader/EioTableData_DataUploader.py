import os
import pickle
import psycopg2
from psycopg2 import sql
import pandas as pd
import dateutil
from dateutil.parser import isoparse

# =============================================================================
# Get Eio Table Data for One Building 
# =============================================================================

def upload_eiotable_data(conn_information, buildingid, eio_outputfile_dict):  
    """
    Uploads EIO table data to a PostgreSQL database.
    The EIO table consists of additional zone information

    This function:
    1. Processes EIO table data provided in a dictionary and uploads it to the 'eiotabledata' 
    table in the PostgreSQL database. 
    2. Checks whether each row of data already exists in the database to avoid duplicates. 

    Args:
        conn_information (str): The connection string or details required to connect to the PostgreSQL database.
        buildingid (str): The ID of the building associated with the EIO data.
        eio_outputfile_dict (dict): A dictionary where the keys are table names and the values are DataFrames 
                                    containing the corresponding table data from the EIO file.

    Returns:
        None

    Example:
        >>> conn_info = "dbname=Building_Models user=postgres password=secret host=localhost"
        >>> eio_data = {'Table1': df1, 'Table2': df2}
        >>> upload_eiotable_data(conn_info, '1', eio_data)
    """
    
    insert_query = sql.SQL("""
        INSERT INTO eiotabledata (buildingid, tablename, zonename, variablename, stringvalue, floatvalue)
        VALUES (%s, %s, %s, %s, %s, %s)
    """)
    
    check_query = sql.SQL("""
        SELECT EXISTS(
            SELECT 1 FROM eiotabledata 
            WHERE buildingid = %s AND tablename = %s AND zonename = %s AND variablename = %s AND stringvalue = %s AND floatvalue = %s
        )
    """)
    
    conn = psycopg2.connect(conn_information)    
    cur = conn.cursor()

    try:
        conn = psycopg2.connect("dbname=Building_Models user=kasey password=OfficeLarge")
        cur = conn.cursor()

        for tablename, value in eio_outputfile_dict.items():
            columns = value.columns.tolist()
            columns.remove('Zone Name')
            
            for column in columns:
                variablename = column.strip()
                
                for _, row in value.iterrows():
                    zonename = row['Zone Name'].strip()
                    table_value = row[column]
                    
                    try:
                        floatvalue = float(table_value)
                        stringvalue = 'NA'
                    except ValueError:
                        stringvalue = str(table_value)
                        floatvalue = None
                    
                    # Check if the row already exists
                    cur.execute(check_query, (buildingid, tablename, zonename, variablename, stringvalue, floatvalue))
                    exists = cur.fetchone()[0]
                    
                    if not exists:
                        # Insert the row if it does not exist
                        cur.execute(insert_query, (buildingid, tablename, zonename, variablename, stringvalue, floatvalue))
                        conn.commit()
        
        cur.close()
        conn.close()
                        
    except Exception as e:
        
        cur.close()
        conn.close()
            


