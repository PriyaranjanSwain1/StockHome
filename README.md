# StockHome
This Project is to extract data of Stockholm Historical Weather Observations and load into HDFS . Spark and Scala has been used for processing of the data .

Approaches taken for the project:

Data Extraction :
1. Each Data file has been downloaded for different time range from below URLs
https://bolin.su.se/data/stockholm-thematic/raw_individual_temperature_observations.php
https://bolin.su.se/data/stockholm-thematic/barometer_readings_in_original_units.php
2. Data files are saved under resources directory for further use as original text formart.

Data Formatting and Cleaning :
1. As observed the separators are different in rows . wrote an UDF to convert any number of spaces into single space.
2. Splitted the row and loaded each data into individual columns of Spark Dataframe.
3. For barometer reading few columns are ignored like 'barometer temperature observations for 1756_1858' , 'air pressure reduced to 0 degC' & 'thermometer observations' for 1859-1861
4. As the unit of air pressure were different in different files , everything changed to a single unit.
5. All files are merged to single dataframe


Data Loading :
1. Data has been loaded to hdfs path by partitioning over year.
2. File format is used as ORC for storing.



