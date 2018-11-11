import pyspark
from pyspark.sql import HiveContext
from pyspark.sql.functions import *
from pyspark.sql.types import * 

if __name__ == "__main__":
	#get the spark context
	sc = pyspark.SparkContext("local", "PySparkWordCount") 

	#get hive context
	hc = HiveContext(sc)

	sql = '''
		select 
			 station_identifier as StationIdentifier
			,observation_date as ObservationDate
			,observation_type
			,observation_value
		from
			weatherraw
		where
			observation_type in ('PRCP', 'SNOW', 'SNWD', 'TMAX', 'TMIN', 'EVAP', 'WESD', 'WESF', 'PSUN')
	'''	

	#Get the data from weatherraw table and change the data type for observation date
	weatherraw = hc.sql(sql).withColumn("ObservationDate", from_unixtime(unix_timestamp('ObservationDate', 'YYYYMMDD')).cast(DateType()))

	#Pivot the data based on the observation type
	pivoted = weatherraw.groupBy("StationIdentifier", "ObservationDate").pivot("observation_type") \
		.agg(first("observation_value").alias("observation_value")).cache()

	#transform the pivoted data into final required form
	weathercurated = pivoted \
		.withColumn("Precipitation", col("PRCP")/10) \
		.withColumn("MaxTemparature", col("TMAX")/10 ) \
		.withColumn("MinTemparature", col("TMIN")/10) \
		.withColumn("Snowfall", col("SNOW").cast(IntegerType())) \
		.withColumn("SnowDepth", col("SNWD").cast(IntegerType())) \
		.withColumn("Evaporation", col("EVAP")/10) \
		.withColumn("WaterEquivalentSnowDepth", col("WESD")/10) \
		.withColumn("WaterEquivalentSnowFall", col("WESF")/10) \
		.withColumn("Sunshine", col("PSUN").cast(FloatType())) \
		.drop("PRCP").drop("TMAX").drop("TMIN").drop("SNOW").drop("SNWD").drop("EVAP").drop("WESD").drop("WESF").drop("PSUN")


	#Write to hive table
	weathercurated.registerTempTable("t_weathercurated") 
	sql='''drop table weathercurated'''
	hc.sql(sql)
	sql='''create table weathercurated as select * from t_weathercurated'''
	hc.sql(sql)

