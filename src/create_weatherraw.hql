CREATE TABLE IF NOT EXISTS 
WeatherRaw ( 
station_identifier String,
observation_date String,
observation_type String,
observation_value String,
observation_measure String,
observation_quality String,
observation_source String,
observation_time String
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

