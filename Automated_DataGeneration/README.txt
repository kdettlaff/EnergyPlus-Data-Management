Input_Textfile_Generator

1. Creates IDF_FilePathList, Weather_FilePathList, and CompletedSimulation_FolderPathList.
2. IDF_FilePathList contains the filepath to every idf file which will be simulated. 
3. Weather_FilePathList contains the filepath to every weather file which will be simulated. 
4. Completed_Simulation_FolderPathList contains the destination of the results of each simulation. 

The user may modify this script as needed to create a customized list of simulations to run. 

EP_DataGenerator_Script_Final

1. Runs simulation for each IDF File in IDF_FilePathList and it's corresponding Weather File in Weather_FilePathList. 
2. Places results in specified Results_FolderPath.

EP_ZoneInformation_Table

1. Reads the Eio File for each building. 
2. Creates a CSV file of zone information for each building. 
3. Places the CSV's into a folder called Zone_Information_Tables
4. Places Zone_Information_Tables Folder in specified Results Folderpath. 

TO DO:
1. Modify to replace simulationName_filepathlist with Completed_Simulation_FilePathList as this gives users more flexibility as to where they want to store their results. 
2. In functions module (being developed separately) replace simulate_building inputs simulation_name and results_folderpath with just results_folderpath. The name is contained within the folderpath. 
3. Modify aggregation script to draw data from database rather than from pickle files. This will simplify things and will remove the need for the creation of the zone information tables. 












