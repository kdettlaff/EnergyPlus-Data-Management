# -*- coding: utf-8 -*-
"""
20240807

@author: ninad gaikwad 
"""

# Changed setup so that It uses the text file of filepaths, similar to how data generation was reworked.
#  Hasn't been tested but should work. 

# Need to import Zone Name List 

# =============================================================================
# Import Required Modules
# =============================================================================

# External Modules
import os
import pandas as pd
import pickle
from re import S
import psycopg2

from Database_DataRetreival.EP_DataRetrieval import *


# =============================================================================
# Aggregate one Variable
# =============================================================================

def aggregate_variable(conn_information, buildingid, variablename, startdatetime, enddatetime, timeresolution, variable, aggregation_type, aggregation_zone_list=None):
    """
    Aggregate variable data for a specified time range, building, and variable using different aggregation types.

    Parameters:
    -----------
    variablename : (str) The name of the variable to be aggregated.
    conn_information : (str) Database connection string
    buildingid : (int) Identifier for the building from which data is to be retrieved.
    startdatetime : (datetime) Start of the time range for which data is to be retrieved.
    enddatetime : (datetime) End of the time range for which data is to be retrieved.
    timeresolution : (int) The time resolution of the data in minutes.
    variable : (str) The name of the variable for which data is to be retrieved.
    aggregation_type : (str) The type of aggregation to be performed ('Average', 'Zone Area Weighted Average', 'Zone Volume Weighted Average').
    aggregation_zone_list : list of lists, optional
        List of lists, where each sublist contains the zone names to be aggregated together.
        Example: [['zone1','zone2','zone3'], ['zone4','zone5','zone6']]
        If None, then all zones are aggregated to a single zone.
    """
    
    # Retrieve data for the specified time range, building, and variable
    data_df = retrieve_timeseriesdata(conn_information, buildingid, startdatetime, enddatetime, timeresolution, variable)  # Assuming variable is zone-based

    # Handle case when aggregation_zone_list is None (Single-Zone Aggregation)
    if aggregation_zone_list is None:
        aggregation_zone_list = [data_df['zonename'].tolist()]
   
    # Make aggregation zone name
    aggregation_zonename = "Aggregation_" + str(len(aggregation_zone_list)) + "Zone"
    
    # Initialize list to hold aggregated data
    aggregated_zone_data = []

    # Perform aggregation based on the specified type
    if aggregation_type == 'Average':
        for zone_list in aggregation_zone_list:
            zone_data = []
            for zone in zone_list:
                values = data_df.loc[data_df['zonename'] == zone, ['datetime', 'value']].values
                if values.size > 0:
                    # Append tuple of (datetime, value)
                    for value in values:
                        zone_data.append((value[0], value[1]))
            if zone_data:
                # Calculate average of the float values
                average = sum(value[1] for value in zone_data) / len(zone_data)
                aggregated_zone_data.append(average)

    elif aggregation_type == 'Zone Area Weighted Average':
        area_df = retrieve_eiotabledata(conn_information, buildingid, tablename='tablename', variablename='area')
        for zone_list in aggregation_zone_list:
            weighted_sum = 0
            total_area = 0
            for zone in zone_list:
                values = data_df.loc[data_df['zonename'] == zone, ['datetime', 'value']].values
                area = area_df.loc[area_df['zonename'] == zone, 'floatvalue'].values
                if values.size > 0 and area.size > 0:
                    # Calculate weighted sum using the float part of the tuple
                    for value in values:
                        weighted_sum += area[0] * value[1]
                    total_area += area[0]
            if total_area > 0:
                aggregated_zone_data.append(weighted_sum / total_area)

    elif aggregation_type == 'Zone Volume Weighted Average':
        volume_df = retrieve_eiotabledata(conn_information, buildingid, tablename='tablename', variablename='volume')
        for zone_list in aggregation_zone_list:
            weighted_sum = 0
            total_volume = 0
            for zone in zone_list:
                values = data_df.loc[data_df['zonename'] == zone, ['datetime', 'value']].values
                volume = volume_df.loc[volume_df['zonename'] == zone, 'floatvalue'].values
                if values.size > 0 and volume.size > 0:
                    # Calculate weighted sum using the float part of the tuple
                    for value in values:
                        weighted_sum += volume[0] * value[1]
                    total_volume += volume[0]
            if total_volume > 0:
                aggregated_zone_data.append(weighted_sum / total_volume)
    
    # Upload aggregated data to the database
    
    
    
    
        
 
    
