BuildingIDs_DataUploader 

1. Parses the name of the completed simulation to obtain model information. 
2. Uploads model information to BuildingIDs table. 
3. Does this for every simulation in completed_simulation_folderpaths textfile. 

BuildingTimeSeriesData_Uploader

1. Opens pickle file and reads into a dictionary, where each key is the Variable Name and each value is a dataframe.
2. Processes data for each variable into the database, making sure to avoid adding duplicate rows. 
3. Does this for each folderpath in completed_simulations_txt. 

EioTableData_DataUploader

1. Reads pickled eio file, uploads zone information to the database. 
2. Does this for each completed simulation folderpath in completed_simulations_txt

TO DO:
1. add main function for BuildingIDs_DataUploader
2. For BuildingTimeSeriesData_Uploader, make sure we are also adding the aggregated data pickle if it exists. 