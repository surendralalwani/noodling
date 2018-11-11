#!/bin/bash
echo "Creating table weatherraw if it does not exists"
hive -S -f create_weatherraw.hql
retVal=$?
if [ $retVal -ne 0 ]
then
	echo "Failed crerating tble"
	exit 1
fi
echo "Created table weatherraw"
echo "Loading data into weatherraw"
file=2017.csv
hive -S -e "LOAD DATA LOCAL INPATH '$file' OVERWRITE INTO TABLE WeatherRaw; select count(*) from weatherraw;"

