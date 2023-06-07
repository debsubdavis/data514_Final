from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.types import FloatType
from utils.mongo_utils import MongoUtils

import datetime
from dateutil import relativedelta

import json

spark = SparkSession.builder.appName('pandasToSparkDF').getOrCreate()

portland_calendar = spark.read.csv("data/portland/calendar.csv", header=True, inferSchema=True)

portland_calendar = portland_calendar.withColumn('available', when(col('available') == 't', True).otherwise(False))
portland_calendar = portland_calendar.withColumn('date', to_date(col('date'), 'yyyy-MM-dd'))
portland_calendar = portland_calendar.withColumn('price', regexp_replace('price', ',', ''))
portland_calendar = portland_calendar.withColumn('price', regexp_replace('price', '$', '').cast("float"))

df_listing = portland_calendar.filter(col('available') == True)
df_listing = df_listing.sort("listing_id", "date")
df_listing.show(100)

windowSpec = Window.partitionBy("listing_id").orderBy("date")
df_listing = df_listing.withColumn("prev_date", lag(df_listing.date).over(windowSpec))
df_listing = df_listing.withColumn(
    "available_range",
    when((datediff(df_listing.date, df_listing.prev_date) > 1) | isnull(df_listing.prev_date), 1).otherwise(0)
)

df_listing = df_listing.withColumn(
    "group_id",
    sum("available_range").over(windowSpec)
)

df_availability = df_listing.groupby("listing_id", "group_id").agg(
    min("date").alias("start_date"),
    max("date").alias("end_date")
)


df_availability = df_availability.withColumn("city", lit("portland"))
df_availability = df_availability.drop("group_id")
df_availability = df_availability.withColumn("listing_id", df_availability["listing_id"].cast("string"))

pandas_df = df_availability.toPandas()

def get_months_in_range(start_date, end_date):
    date_list = [start_date + relativedelta.relativedelta(months=x) for x in range((end_date.year - start_date.year) * 12 + end_date.month - start_date.month + 1)]
    return [date.month for date in date_list]


pandas_df['Months in Date Range'] = pandas_df.apply(lambda row: get_months_in_range(row['start_date'], row['end_date']), axis=1)


json_data = pandas_df.to_dict(orient='records')

for record in json_data:
    record['start_date'] = record['start_date'].strftime('%Y-%m-%d')
    record['end_date'] = record['end_date'].strftime('%Y-%m-%d')

json_string = json.dumps(json_data, indent=2)

json_output_file_path = "outputs/json/portland/availability_estimates.json"

# write the json_string to the file
with open(json_output_file_path, 'w') as json_file:
    json_file.write(json_string)

MongoUtils.write_to_collection(json_string, "Availability_Estimates", "portland")