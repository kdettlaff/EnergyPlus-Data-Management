import os
import pickle
from re import S
import psycopg2
from flask import Flask

# =============================================================================
# Initialize Server
# =============================================================================  

def initialize_server(app_name, host='0.0.0.0', port=None):
    app = Flask(app_name)
    
    # Set default port if not provided
    if port is None:
        port = int(os.environ.get("PORT", 5000))
    
    # Additional setup or routes can be added here
    @app.route('/')
    def home():
        return "Server Running"
    
    # Run the server
    app.run(host=host, port=port)

# =============================================================================
# Check Table Exists
# =============================================================================  

def check_table_exists(conn_information, schema_name, tablename):
    conn = psycopg2.connect(conn_information)
    cur = conn.cursor()
    
    # Query to check if the table exists
    check_table_query = """
    SELECT EXISTS (
        SELECT 1
        FROM information_schema.tables 
        WHERE table_schema = %s 
        AND table_name = %s
    );
    """
    
    # Query to count the number of rows in the table
    check_empty_query = """
    SELECT COUNT(*) 
    FROM {schema}.{table};
    """.format(schema=schema_name, table=tablename)
    
    # Execute the query to check if the table exists
    cur.execute(check_table_query, (schema_name, tablename))
    table_exists = cur.fetchone()[0]
    
    # Initialize is_empty to None in case the table does not exist
    is_empty = None
    
    # Only check if the table is empty if it exists
    if table_exists:
        cur.execute(check_empty_query)
        row_count = cur.fetchone()[0]
        is_empty = (row_count == 0)
    
    # Clean up
    cur.close()
    conn.close()
    
    return table_exists, is_empty

# =============================================================================
# Empty Table
# =============================================================================  
def empty_table(conn_information, schema_name, tablename):
    
    conn = psycopg2.connect(conn_information)
    cur = conn.cursor()

    truncate_query = f'TRUNCATE TABLE "{schema_name}"."{tablename}" RESTART IDENTITY;'
    
    cur.execute(truncate_query)
    conn.commit()
    
    cur.close()
    conn.close()

# =============================================================================
# Create BuildingIds Table
# =============================================================================  

def create_buildingids_table(conn_information):
    
    conn = psycopg2.connect(conn_information)
    cursor = conn.cursor()

    create_table_query = """
        CREATE TABLE buildingids (
            buildingid SERIAL PRIMARY KEY,
            buildingcategory TEXT,
            buildingtype TEXT,
            buildingstandard TEXT,
            buildingstandardyear TEXT,
            buildinglocation TEXT,
            buildingheatingtype TEXT,
            buildingfoundationtype TEXT,
            buildingclimatezone TEXT,
            buildingprototype TEXT,
            buildingconfiguration TEXT
        );
        """
    cursor.execute(create_table_query)
    conn.commit()  

    # Close the connection
    cursor.close()
    conn.close()
    
# =============================================================================
# Create TimeSeriesData Table
# =============================================================================

def create_timeseriesdata_table(conn_information):
    
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(conn_information)
    cursor = conn.cursor()

    # zonename only applies for zone-based variables
    # surfacename only applies for surface-based variables
    # systemnodename only applies for node related varables. 
        
    create_table_query = f"""
            CREATE TABLE timeseriesdata (
                timeseriesdataid SERIAL PRIMARY KEY,
                buildingid INTEGER REFERENCES buildingids(buildingid),
                datetime TEXT,
                timeresolution TEXT,
                variablename TEXT,
                schedulename TEXT, 
                zonename TEXT, 
                surfacename TEXT,
                systemnodename TEXT,
                value REAL
            );
            """

    cursor.execute(create_table_query)
    conn.commit()  # Commit all operations at once

    # Close the connection
    cursor.close()
    conn.close()

# =============================================================================
# Create EioTableData Table
# =============================================================================
    
def create_eiotabledata_table(conn_information):
    
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(conn_information)
    cursor = conn.cursor()

    # zonename only applies for zone-based variables
    # surfacename only applies for surface-based variables
    # systemnodename only applies for node related varables. 
        
    create_table_query = f"""
            CREATE TABLE eiotabledata (
                eiotabledataid SERIAL PRIMARY KEY,
                buildingid INTEGER REFERENCES buildingids(buildingid),
                tablename TEXT,
                zonename TEXT,
                variablename TEXT,
                stringvalue TEXT,
                floatvalue REAL
            );
            """

    cursor.execute(create_table_query)
    conn.commit()  # Commit all operations at once

    # Close the connection
    cursor.close()
    conn.close()