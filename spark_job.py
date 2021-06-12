PROJECT_ID = "blank-space-315611" # your project id
BUCKET_NAME = "spark-week3" # your bucket name
DATASET_ID = "week3" # your dataset id

from datetime import date
from pyspark.sql import SparkSession, functions as f
from pyspark.sql.functions import lit, date_add
from pyspark.sql.types import DateType
import time

# create spark session
spark = SparkSession.builder.getOrCreate()

# extract data to spark dataframe
flight_data = spark.read.format("json").load(f"gs://{BUCKET_NAME}/data/*.json")
flight_data.printSchema()
flight_data.show()

# transform flight_date to date type
flight_data = flight_data.withColumn("flight_date", flight_data.flight_date.cast(DateType()))
flight_data = flight_data.withColumn("flight_date", date_add(flight_data.flight_date, 723))
flight_data.printSchema()
flight_data.show()

# transformation to get flight count by airline code
flight_data.registerTempTable("flights_data")

airline_flights = spark.sql(
    """
    select
        flight_date,
        airline_code,
        count(*) as count
    from
        flights_data
    group by
        flight_date,
        airline_code
    order by
        flight_date,
        airline_code
    """
)

# load spark dataframe to bigquery
airline_flights.write.mode('overwrite').format('bigquery') \
    .option('temporaryGcsBucket', BUCKET_NAME) \
    .option('createDisposition', 'CREATE_IF_NEEDED') \
    .save(f"{PROJECT_ID}:{DATASET_ID}.airline_flights")

# transformation to get average delays by source airport
avg_delays_by_source_airport = spark.sql(
    """
    select
        flight_date,
        source_airport,
        round(avg(arrival_delay),2) as avg_arrival_delay
    from
        flights_data
    group by
        flight_date,
        source_airport
    order by
        avg_arrival_delay
    """
)
avg_delays_by_source_airport.show()

# load spark dataframe to parquet file
avg_delays_by_source_airport.write.mode('overwrite').partitionBy("flight_date") \
    .format('parquet') \
    .save(f'gs://{BUCKET_NAME}/output/avg_delays_by_source_airport')

# transformation to get source airport flights count
source_airport_count = spark.sql(
    """
    select
        count(source_airport),
        source_airport
    from
        flights_data
    group by
        source_airport
    order by
        count(source_airport) desc
    """
)
source_airport_count.show()

# load spark dataframe to json file
source_airport_count.coalesce(1).write.format("json") \
    .save(f'gs://{BUCKET_NAME}/output/source_airport_count')

# transformation to get destination airport flights count
dest_airport_count = spark.sql(
    """
    select
        count(destination_airport),
        destination_airport
    from
        flights_data
    group by
        destination_airport
    order by
        count(destination_airport) desc
    """
)
dest_airport_count.show()

# load spark dataframe to json file
dest_airport_count.coalesce(1).write.format("json") \
    .save(f'gs://{BUCKET_NAME}/output/dest_airport_count')

# transformation to add temp table of distance category
flights_data = spark.sql(
    """
    select 
        *,
        case 
            when distance between 0 and 500 then 1 
            when distance between 501 and 1000 then 2
            when distance between 1001 and 2000 then 3
            when distance between 2001 and 3000 then 4 
            when distance between 3001 and 4000 then 5 
            when distance between 4001 and 5000 then 6 
        END distance_category 
    from 
        flights_data 
    """
)
flights_data.registerTempTable("flights_data")

# transformation to get average delays by distance category
avg_delays_by_distance_category = spark.sql(
    """
    select
        flight_date,
        distance_category,
        round(avg(arrival_delay),2) as avg_arrival_delay,
        round(avg(departure_delay),2) as avg_departure_delay
    from
        flights_data
    group by
        flight_date,
        distance_category
    order by
        distance_category
    """
)
avg_delays_by_distance_category.show()

# load spark dataframe to csv file
avg_delays_by_distance_category.write.mode('overwrite').format('csv') \
    .option('header', True) \
    .save(f'gs://{BUCKET_NAME}/output/avg_delays_by_distance_category')

spark.stop()
