# noodling

## Infrastrcture setup
```
docker pull cloudera/quickstart:latest
docker run --hostname=quickstart.cloudera --privileged=true -t -i cloudera/quickstart /usr/bin/docker-quickstart
cp  /usr/lib/hive/conf/hive-site.xml    /usr/lib/spark/conf/
```

## download.sh
Download the weather data file and unzip it.

## create_weatherraw.hql
Hive SQL script that creates the weather table and is called from load_weatherraw.sh

## load_weatherraw.sh
Creates the weatherraw table and loads the raw weather data 

## aggregate_weather.py
Takes the weatherraw data from hive table and creates weathercurated hive table

