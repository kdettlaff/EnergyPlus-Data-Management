import os
import pickle
from re import S
import psycopg2

# =============================================================================
# Find Heating Type for Commercial Buildings 
# =============================================================================

def find_heating_type(filepath): # Use only for Commercial Buildings
    """
    Determines the heating type(s) used in a commercial building based the contents of the IDF file.

    The function looks for the following heating types:
        - Water
        - Electric
        - Steam
        - Gas
    
    If any of these heating types are found, their corresponding names are returned as a string, with multiple types
    joined by " & " if more than one type is found.

    Args:
        filepath (str): The file path to the building's IDF file. 

    Returns:
        str: A string listing the heating types found in the file, joined by " & ". If no heating types are found, 
             an empty string is returned.
    """
    
    search_strings = [
        'ALL OBJECTS IN CLASS: COIL:HEATING:WATER',
        'ALL OBJECTS IN CLASS: COIL:HEATING:ELECTRIC',
        'ALL OBJECTS IN CLASS: COIL:HEATING:STEAM',
        'ALL OBJECTS IN CLASS: COIL:HEATING:GAS'
    ]
    
    found_string = ""
    found_strings = []
    
    # Open and read the file
    with open(filepath, 'r') as file:
        file_contents = file.read()
        
        # Check each string in search_strings to see if it is in the file
        for search_string in search_strings:
            if search_string in file_contents and search_string not in found_strings:
                
                if search_string == 'ALL OBJECTS IN CLASS: COIL:HEATING:WATER':
                    found_strings.append('Water')
                elif search_string == 'ALL OBJECTS IN CLASS: COIL:HEATING:ELECTRIC':
                    found_strings.append('Electric')
                elif search_string == 'ALL OBJECTS IN CLASS: COIL:HEATING:STEAM':
                    found_strings.append('Steam')
                else:
                    found_strings.append('Gas')
                                
    combined_string = ' & '.join(found_strings)
  
    return combined_string

# =============================================================================
# Find Climate Zone for Commercial Buildings
# =============================================================================

def commercial_climate_zone(commercial_building_location):
    """
    Determines the ASHRAE climate zone for a commercial building based on its location.

    Args:
        commercial_building_location (str): The name of the city or location where the commercial building is located.

    Returns:
        str: The Corresponding Climate Zone, or 'Unknown' 
        
    """
    
    climate_zone = 'Unknown'
    
    if (commercial_building_location == 'HoChiMinh'):
        climate_zone = '0A'
    elif (commercial_building_location == 'Dubai'): 
        climate_zone = '0B'
    elif (commercial_building_location == 'Miami'):  
        climate_zone = '1A' 
    elif (commercial_building_location == 'Honolulu'):
        climate_zone = '1A'
    elif (commercial_building_location == 'NewDehli'):  
        climate_zone = '1B'
    elif (commercial_building_location == 'Tampa'):  
        climate_zone = '2A'
    elif (commercial_building_location == 'Tucson'):  
        climate_zone = '2B'
    elif (commercial_building_location == 'Atlanta'):  
        climate_zone = '3A'   
    elif (commercial_building_location == 'ElPaso'):  
        climate_zone = '3B'
    elif (commercial_building_location == 'SanDiego'):  
        climate_zone = '3C'
    elif (commercial_building_location == 'NewYork'):  
        climate_zone = '4A'
    elif (commercial_building_location == 'Albuquerque'):  
        climate_zone = '4B'
    elif (commercial_building_location == 'Seattle'):  
        climate_zone = '4C'      
    elif (commercial_building_location == 'Buffalo'):  
        climate_zone = '5A'
    elif (commercial_building_location == 'Denver'):  
        climate_zone = '5B'
    elif (commercial_building_location == 'PortAngeles'):  
        climate_zone = '5C'
    elif (commercial_building_location == 'Rochester'):  
        climate_zone = '6A'
    elif (commercial_building_location == 'GreatFalls'):  
        climate_zone = '6B'
    elif (commercial_building_location == 'InternationalFalls'):  
        climate_zone = '7'
    elif (commercial_building_location == 'Fairbanks'):  
        climate_zone = '8'

    return climate_zone

# =============================================================================
# Parse Name Commercial Buildings
# =============================================================================

def parse_name_commercial(idf_filepath, results_folderpath): # Naming Convention: Standard_Year_Location_BuildingType
    """
    Parses the model name for a commercial building simulation and extracts key information based on the naming convention.

    Args:
        idf_filepath (str): The file path to the IDF file associated with the simulation.
        results_folderpath (str): The file path to the folder where the simulation results are stored.

    Returns:
        dict: A dictionary containing the parsed information with the following keys:
            - 'Standard' (str): The standard used in the simulation.
            - 'StandardYear' (str): The year of the standard.
            - 'Climate_Zone' (str): The ASHRAE climate zone based on the location.
            - 'Location' (str): The location of the building.
            - 'BuildingType' (str): The type of the building.
            - 'HeatingType' (str): The heating type determined by analyzing the IDF file.
    
    """
    model_name = os.path.basename(results_folderpath)
    
    split_string = model_name.split('_')
    model_information = dict(Standard = split_string[0], StandardYear = split_string[1], Climate_Zone = commercial_climate_zone(split_string[2]), Location = split_string[2], BuildingType = split_string[3], HeatingType = find_heating_type(idf_filepath))
    
    return model_information

# =============================================================================
# Parse Name Residential Buildings
# =============================================================================

def parse_name_residential(results_folderpath): # Naming Convention: Prototype_ClimateZone_Location_HeatingType_FoundationType_Standard_Year 
    """
    Parses the model name for a residential building simulation and extracts key information based on the naming convention.

    Args:
        results_folderpath (str): The file path to the folder where the simulation results are stored.

    Returns:
        dict: A dictionary containing the parsed information with the following keys:
            - 'Prototype' (str): The prototype of the building.
            - 'ClimateZone' (str): The climate zone of the building.
            - 'Location' (str): The location of the building.
            - 'HeatingType' (str): The type of heating used in the building.
            - 'FoundationType' (str): The type of foundation of the building.
            - 'Standard' (str): The standard used in the simulation.
            - 'Year' (str): The year associated with the standard.

    """
    model_name = os.path.basename(results_folderpath)
    
    split_string = model_name.split('_')
    model_information = dict(Prototype = split_string[0], ClimateZone = split_string[1], Location = split_string[2], HeatingType = split_string[3], FoundationType = split_string[4], Standard = split_string[5], Year = split_string[6]);
    
    return model_information

# =============================================================================
# Parse Name Manufactured Buildings
# =============================================================================

def parse_name_manufactures(results_folderpath): # Naming Convention: Configuration_Location_ClimateZone_EnergyCode_HeatingType
    """
    Parses the model name for a manufactured building simulation and extracts key information based on the naming convention.

    Args:
        results_folderpath (str): The file path to the folder where the simulation results are stored.

    Returns:
        dict: A dictionary containing the parsed information with the following keys:
            - 'Configuration' (str): The configuration of the building.
            - 'Location' (str): The location of the building.
            - 'ClimateZone' (str): The climate zone of the building.
            - 'EnergyCode' (str): The energy code used in the simulation.
            - 'HeatingType' (str): The type of heating used in the building.

    """
    model_name = os.path.basename(results_folderpath)
    
    split_string = model_name.split('_')
    model_information = dict(Configuration = split_string[0], Location = split_string[1], ClimateZone = split_string[2], EnergyCode = split_string[3], HeatingType = split_string[4]);
    
    return model_information

# =============================================================================
# Check Building already uploaded to BuildingIDs table for particular simulation
# =============================================================================
def check_sim_uploaded_to_buildingids(csv_filepath, results_folderpath):
    """
    Checks whether a simulation corresponding to the given results folder path has been uploaded to the `buildingids` table.

    Args:
    csv_filepath (str): Filepath to Simulation_Information.csv
    results_folderpath (str): Folderpath to results for specified simulation

    Returns 1 if the simulation corresponding to `results_folderpath` has already been uploaded, 
    otherwise returns 0.
    
    """

    already_uploaded = 0
    
    with open(csv_filepath, 'r') as file:
        lines = file.readlines()
    
    for line in lines:
        if results_folderpath == line.split(',')[3]:
            if not line.split(',')[0] == 'NA':
                already_uploaded = 1
    
    return already_uploaded

# =============================================================================
# Upload Model Information To BuildingIDs Table for Single Building
# =============================================================================

def upload_model_information(model_information, building_category, conn_information): 
    """
    Inserts building information into the `buildingids` table and returns the auto-incremented building ID

    Args:
        model_information (dict): infomration aquired by parsing simulation name. 
        building_category (str)
        conn_information (str): The connection string or information required to connect to the PostgreSQL database.

    Returns:
    buildingid (int)
    
    """

    # SQL query template for inserting data
    insert_query = """
    INSERT INTO buildingids (
        buildingcategory, buildingtype, buildingprototype, buildingconfiguration, buildingstandard, 
        buildingstandardyear, buildinglocation, buildingclimatezone, 
        buildingheatingtype, buildingfoundationtype
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    # Prepare data for insertion, replacing missing fields with 'NA'
    data = (
        building_category,
        model_information.get('BuildingType', 'NA'),
        model_information.get('Prototype', 'NA'),
        model_information.get('Configuration', 'NA'),
        model_information.get('Standard', 'NA'),
        model_information.get('StandardYear', 'NA'),
        model_information.get('Location', 'NA'),
        model_information.get('Climate_Zone', 'NA'),
        model_information.get('HeatingType', 'NA'),
        model_information.get('FoundationType', 'NA')
    )
    
    conn = psycopg2.connect(conn_information)
    cursor = conn.cursor()
    cursor.execute(insert_query, data)
    conn.commit()
    cursor.close()
    conn.close()
        
    # Retrieve the last inserted ID (buildingid)
    buildingid = cursor.lastrowid + 1
    return buildingid
        
# =============================================================================
# Upload a single building to BuildingIds Table
# =============================================================================       

def upload_to_buildingids(conn_information, filepaths): 
    """
    Uploads building information to the database and updates the corresponding building ID in the CSV file.

    This function: 
    1. connects to a PostgreSQL database
    2. processes a CSV file to find the idf filepath corresponding to the given results folder path
    3. parses simulation name and idf file as needed to obtain building information
    4. uploads the building information to the `buildingids` table. 
    5. updates the `buildingid` in the CSV file for the corresponding row.

    Args:
        conn_information (str): The connection string or information required to connect to the PostgreSQL database.
        csv_filepath (str): the filepath to Simulation_Information.csv
        results_folderpath (str): the folderpath to the results of a particular simulation

    Returns:
    None
    """
    
    csv_filepath = filepaths["sim_information_filepath"]
    results_folderpath = filepaths["sim_results_folderpath"]
    
    with open(csv_filepath, 'r') as file:
        lines = file.readlines()
    
    for i, line in enumerate(lines):
        if results_folderpath == line.split(',')[3]:
            idf_filepath = line.split(',')[1]
            row_number = i
            break  
        
    if os.path.basename(results_folderpath).startswith('ASHRAE') or results_folderpath.startswith('IECC'):
        model_information = parse_name_commercial(idf_filepath, results_folderpath)
        buildingid = upload_model_information(model_information, 'Commercial', conn_information)
    elif os.path.basename(results_folderpath).startswith('MF') or results_folderpath.startswith('SF'):
        model_information = parse_name_residential(os.path.basename(results_folderpath))
        buildingid = upload_model_information(model_information, 'Residential', conn_information)
    else:
        model_information = parse_name_manufactures(os.path.basename(results_folderpath))
        buildingid = upload_model_information(model_information, 'Manufactured', conn_information)
    
    updated_row_split = lines[row_number].split(',')
    updated_row = str(buildingid) + ',' + updated_row_split[1] + ',' + updated_row_split[2] + ',' + updated_row_split[3] + ',' + updated_row_split[4]
    lines[row_number] = updated_row
    
    with open(csv_filepath, 'w') as file:
        file.writelines(lines)
    
    return buildingid

# =============================================================================
# Main
# =============================================================================   

