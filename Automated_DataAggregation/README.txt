EP_DataAggregation_Script2

1. Reads each completed simulation folder path in completed simulations.
2. Reads the zone information table for the relevant building from the Zone_Information_Tables Folder. 
3. Aggregates time series data for each zone to a single zone. 
4. Stores aggregated data in the same folder as the completed simulation. 

TO DO:

1. Change Aggregation Script so that it uses data retrieved from the database rather than data from a pickle file and the zone information tables. 
2. We could potentially have two aggregation scripts: one that uses the database, one that uses files. 



