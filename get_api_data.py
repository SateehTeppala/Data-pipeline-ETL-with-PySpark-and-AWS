import pyspark
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import requests
from typing import List
import functools
import logging


logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
log = logging.getLogger("FAWS")

# Setting the threshold of logger to DEBUG
log.setLevel(logging.DEBUG)



def _get_api_data(spark: SparkSession, api_url):
    log.info("Getting data from RestAPI ")
    # Fetch data from the API using requests
    response = requests.get(api_url)
    api_data = response.json()

    schema = StructType([
        StructField("First Name", StringType(), True),
        StructField("Last Name", StringType(), True),
        StructField("Email", StringType(), True),
        StructField("Gender", StringType(), True),
        StructField("Salary", IntegerType(), True),
    ])

    api_rows = [pyspark.sql.Row(**row) for row in api_data]
    log.info(f"showing first row from api response ==> \n {api_rows[0]}")
    # Create a PySpark DataFrame from the API data
    df = spark.createDataFrame(api_rows)
    # Show the DataFrame


    # Log the schema of the DataFrame
    log.info(f"printing schema of dataframe === >\n{df.dtypes}")
    log.info(f"count of df ==> {df.count()}")
    log.info(f"showing first 5 records === >\n{df.limit(5).toPandas().head().to_string(index=False)}")
  
    return df
