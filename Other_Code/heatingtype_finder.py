
# =============================================================================
# Import Required Modules
# =============================================================================

import os

# =============================================================================
# For Commercial IDF File, Find Heating Type
# =============================================================================

def findheatingtype(idf_filepath):
    
    search_string = 'ALL OBJECTS IN CLASS: COIL:HEATING:'

    with open(idf_filepath, 'r') as file:
        lines = file.read()

    for line in lines:
        if search_string in line:
            return line
        
# =============================================================================
# Test
# =============================================================================
    
idf_filepath = r"D:\Building_Modeling_Code\Data\Commercial_Prototypes\ASHRAE\90_1_2013\ASHRAE901_Hospital_STD2013_Buffalo.idf"