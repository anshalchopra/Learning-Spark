# Import necessary modules from PySpark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

# Define the schema for the fire dataset
fire_schema = StructType([
    StructField('CallNumber', IntegerType(), True),
    StructField('UnitID', StringType(), True),
    StructField('IncidentNumber', IntegerType(), True),
    StructField('CallType', StringType(), True),
    StructField('CallDate', StringType(), True),
    StructField('WatchDate', StringType(), True),
    StructField('CallFinalDisposition', StringType(), True),
    StructField('AvailableDtTm', StringType(), True),
    StructField('Address', StringType(), True),
    StructField('City', StringType(), True),
    StructField('Zipcode', IntegerType(), True),
    StructField('Battalion', StringType(), True),
    StructField('StationArea', StringType(), True),
    StructField('Box', StringType(), True),
    StructField('OriginalPriority', StringType(), True),
    StructField('Priority', StringType(), True),
    StructField('FinalPriority', IntegerType(), True),
    StructField('ALSUnit', BooleanType(), True),
    StructField('CallTypeGroup', StringType(), True),
    StructField('NumAlarms', IntegerType(), True),
    StructField('UnitType', StringType(), True),
    StructField('UnitSequenceInCallDispatch', IntegerType(), True),
    StructField('FirePreventionDistrict', StringType(), True),
    StructField('SupervisorDistrict', StringType(), True),
    StructField('Neighborhood', StringType(), True),
    StructField('Location', StringType(), True),
    StructField('RowID', StringType(), True),
    StructField('Delay', FloatType(), True)
])

# Initialize SparkSession
spark = (SparkSession
         .builder
         .appName("SF Fire Calls Analysis")
         .getOrCreate())

# Load the CSV file into a DataFrame
sf_fire_file = './data/sf-fire-calls.csv'
fire_df = spark.read.csv(sf_fire_file, header=True, schema=fire_schema)

# Select specific columns and filter out rows where CallType is "Medical Incident"
few_fire_df = (fire_df
               .select("IncidentNumber", "AvailableDtTm", "CallType")
               .where(col("CallType") != "Medical Incident"))
few_fire_df.show(5, truncate=False)

# Count distinct CallTypes
(fire_df
 .select("CallType")
 .where(isnotnull("CallType"))
 .agg(countDistinct('CallType').alias("DistinctCallTypes"))
 .show())

# Display all distinct CallTypes
(fire_df
 .select("CallType")
 .where(isnotnull("CallType"))
 .distinct()
 .show(truncate=False))

# Rename column "Delay" to "ResponseDelayedMins" and filter rows with delays > 5 mins
new_fire_df = fire_df.withColumnRenamed("Delay", "ResponseDelayedMins")
(new_fire_df
 .select("ResponseDelayedMins")
 .where(col("ResponseDelayedMins") > 5)
 .sort(desc("ResponseDelayedMins"))
 .show(5, truncate=False))

# Convert string date columns to timestamp and drop the original columns
fire_ts_df = (new_fire_df
              .withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy"))
              .drop("CallDate")
              .withColumn("OnWatchDate", to_timestamp(col("WatchDate"), "MM/dd/yyyy"))
              .drop("WatchDate")
              .withColumn("AvailableDtTS", to_timestamp(col("AvailableDtTm"), "MM/dd/yyyy hh:mm:ss a"))
              .drop("AvailableDtTm"))

# Display the newly created timestamp columns
fire_ts_df.select("IncidentDate", "OnWatchDate", "AvailableDtTS").show(5, False)

# List distinct years of incidents and order them
fire_ts_df.select(year('IncidentDate')).distinct().orderBy(year('IncidentDate')).show()

# Group by CallType, count occurrences, and sort by count in descending order
(fire_ts_df
 .select("CallType")
 .where(isnotnull("CallType"))
 .groupBy("CallType")
 .count()
 .orderBy("count", ascending=False)
 .show(n=10, truncate=False))

# Perform aggregate calculations on NumAlarms and ResponseDelayedMins
(fire_ts_df
 .select(
    sum("NumAlarms").alias("TotalAlarms"),
    avg("ResponseDelayedMins").alias("AvgResponseDelay"),
    min("ResponseDelayedMins").alias("MinResponseDelay"),
    max("ResponseDelayedMins").alias("MaxResponseDelay")
)
 .show())
