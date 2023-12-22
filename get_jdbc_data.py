from pyspark.sql import SparkSession,DataFrame

import logging


logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
log = logging.getLogger("FAWS")

# Setting the threshold of logger to DEBUG
log.setLevel(logging.DEBUG)

def _get_data_jdbc(spark):
    log.info("Getting data from Postgres( JDBC ) ")
    url = "jdbc:postgresql://host_name:5432/db_name"
    properties = {
        "user": "user_name",
        "password": "password",
        "driver": "org.postgresql.Driver"
    }
    table_name = "table_name"
    df = spark.read.jdbc(url, table_name, properties=properties)
    log.info(f"printing schema of dataframe === >\n{df.dtypes}")
    log.info(f"count of df ==> {df.count()}")
    log.info(f"showing first 5 records === >\n{df.limit(5).toPandas().head().to_string(index=False)}")
  
    return df
