
# -*- coding: utf-8 -*-
"""
Created on Wed Apr 20 21:11:17 2022

@author: ninad gaikwad 
"""

# =============================================================================
# Import Required Modules
# =============================================================================

# External Modules
import os
import numpy as np
import pandas as pd
import scipy.io
import opyplus as op
import re
import shutil
import datetime
import csv

Current_FilePath = os.path.dirname(__file__)
Simulation_FolderPath = os.path.join(Current_FilePath,  '..',  '..', 'Results', 'Processed_BuildingSim_Data')
Simulation_Name_List = os.listdir(Simulation_FolderPath)[1:]

for simulation_name in Simulation_Name_List:
  
    buildingType = simulation_name.split('_')[3]
    eio_filepath = os.path.join(Current_FilePath, '..','..', "Results", "Processed_BuildingSim_Data", simulation_name, "Sim_OutputFiles", "eplusout.eio")

    i = 1
    line_num = 0
    lines = []

    with open (eio_filepath, 'r') as file:
        for line in file:
            lines.append(line.strip())
            if "! <Zone Summary>" in line:
                print(line)
                line_num = i
            else:
                i += 1

    zone_summary = lines[line_num].split(',')
    number_of_zones = zone_summary[1]
    line_num = line_num + 1
    #line_num = line_num + 2 + int(number_of_zones)

    lines = lines[line_num:(line_num + int(number_of_zones) + 1)]

    i = 0

    for line in lines:
        if i > 0:
            if line.split(',')[29] == 'No':
                lines.pop(i)
        i += 1    

    csv_filename = buildingType + "_ZoneInformation.csv"
    csv_filepath = os.path.join(Current_FilePath, "..", "..", "Results", "Zone_Information_Tables", csv_filename)

    with open(csv_filepath, 'w', newline='') as csv_file:
        csv_writer = csv.writer(csv_file)
        for line in lines:
            line_array = line.split(',')
            
            # Remove preceding spaces from each element in line_array
            cleaned_line_array = [element.strip() for element in line_array]
        
            # Write the cleaned line_array to the CSV file
            csv_writer.writerow(cleaned_line_array)
        







