import pandas as pd
import json

from utils.mongo_utils import MongoUtils
from utils.cleaning_utils import CleaningUtils
 
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lead, lit
from pyspark.sql.window import Window



class CalendarETL:

    def preprocess_cal (self, file_path, city):
        spark = SparkSession.builder.getOrCreate()

        print("in CalendarCleaning.clean_cal")
        print(f'file_path: {file_path}, city: {city}')

        # Read the CSV file into a DataFrame
        df = spark.read.csv(file_path, header=True, inferSchema=True)

        # Select only the required columns
        selected_cols_calendar = [
            'listing_id',
            'date',
            'available',
            'price',
            'adjusted_price',
            'minimum_nights',
            'maximum_nights',
        ]
        df = df.select(selected_cols_calendar)

        # Drop rows with null values in any of the selected columns
        for col_name in selected_cols_calendar:
            df = df.filter(col(col_name).isNotNull())

        # Add a column with the city
        #df = df.withColumn('city', lit(city))

        # Define the window
        # window = Window.partitionBy('listing_id').orderBy('date')

        # # Add two new columns to df: 'next_day_available' and 'day_after_next_available'
        # df = df.withColumn('next_day_available', lead('available', 1).over(window) == 't')
        # df = df.withColumn('day_after_next_available', lead('available', 2).over(window) == 't')

        # # Add the 'Avail_Next2Days' column
        # df = df.withColumn('Avail_Next2Days', (col('next_day_available') & col('day_after_next_available')))

        # # Remove the intermediate columns
        # df = df.drop('next_day_available', 'day_after_next_available')

        df.show(10)

        return df

    def clean_cal(self, file_path, city):
        preprocessed_df = self.preprocess_cal(file_path, city)

        preprocessed_df.show(10)

        pandas_df = preprocessed_df.toPandas()

        pandas_df['date'] = pandas_df['date'].astype(str)

        selected_cols_calendar = [
            'listing_id',
            'date',
            'available',
            'price',
            'adjusted_price',
            'minimum_nights',
            'maximum_nights'#,
            #'Avail_Next2Days'
        ]
        row_lambda = lambda row: {
            "_id": str(row["listing_id"]) + "-" + str(row["date"]),
            "listing_id": str(row["listing_id"]),
            "date": row["date"],
            "available": 1 if row["available"] == "t" else (0 if row["available"] == "f" else None),
            "city": row['city'],  # Assuming all listings are in Portland
            "price": float(row["price"].replace("$", "").replace(",", "")),
            "adjusted_price": float(row["adjusted_price"].replace("$", "").replace(",", "")),
            "minimum_nights": row["minimum_nights"],
            "maximum_nights": row['maximum_nights']#,
            #"avail_next2days": row['Avail_Next2Days']
        }
        json_output_file_path = "outputs/json/" + city + "/calendar.json"
        jsonstr_cal = CleaningUtils.process_df(pandas_df, selected_cols_calendar, row_lambda, city, json_output_file_path)
        MongoUtils.write_to_collection(jsonstr_cal, "Calendar", city)
