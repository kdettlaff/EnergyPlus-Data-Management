# =============================================================================
# Import Required Modules
# =============================================================================

# External Modules
import os
import pandas as pd
import numpy as np
import pickle
import datetime
import copy

# For debugging
import matplotlib.pyplot as plt

# Custom Modules
from EP_DataRetrieval import *

# =============================================================================
# User Inputs
# =============================================================================

# Building ID
# Datetime Range - Or automatically retrieve data for entire year. 
# Timeresolution
# Aggregation Zone Name

# =============================================================================
# Retieve Data
# =============================================================================

def retreivedata(conn_information, buildingid, startdatetime, enddatetime, aggregation_zonename):
    
    zonebased_df = retrieve_timeseriesdata(conn_information, buildingid, startdatetime, enddatetime, subvariabletype = 'Zone', subvariable = aggregation_zonename)
    
    remaining_df = retrieve_timeseriesdata(conn_information, buildingid, startdatetime, enddatetime, subvariabletype = 'Zone', subvariable = 'NA')
    
    # Question - For Aggregation, is aggregation applied to surface-based and system-node-based variables as well? 
    # Yes - after aggregation, # of surfaces = # of zones, # of system nodes = # of zones 
    
    df_combined = pd.concat([zonebased_df, remaining_df], ignore_index=True)
    
    return df_combined

# =============================================================================
# Getting Required Data from Dataframe
# =============================================================================

# =============================================================================
# Basic Computation
# =============================================================================

