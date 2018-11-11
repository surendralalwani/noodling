#!/bin/bash
#export ftp_proxy=

#URL of remotre file to download
url="ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/by_year/2017.csv.gz"

#Get the filename from URL
fname=${url##*/}

#Remove the gz extension 
fcsv=${fname%.*}

#Download, gunzip and save the CSV data file
wget -O - "$url" | gunzip -c > $fcsv
retVal=$?
if [ $retVal -ne 0 ]
then
    echo "Error downloading file"
    exit 1
fi

