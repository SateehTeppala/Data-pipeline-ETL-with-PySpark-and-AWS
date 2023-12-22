from pyspark.sql import SparkSession, DataFrame
from get_api_data import _get_api_data
from get_jdbc_data import _get_data_jdbc
from pyspark.sql.functions import regexp_replace, translate
from pyspark.sql.types import IntegerType, FloatType
from pyspark.sql.functions import mean
import functools
import sys
import findspark
print(findspark.find())
# print(sys.path)
# print(sys.executable)
# print(sys.version)
import boto3
import logging



logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
log = logging.getLogger("FAWS")

# Setting the threshold of logger to DEBUG
log.setLevel(logging.DEBUG)


spark = SparkSession.builder.master("local[1]") \
    .appName("AWS data-engineering") \
    .config("spark.jars", "postgresql-42.7.1.jar") \
    .config("spark.driver.extraClassPath", "postgresql-42.7.1.jar") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

log.info(f"Spark-version ==> {spark.version}")


def upload_to_s3(csv_string):
    log.info(":::::: uploading to S3 started ::::::")
    aws_access_key_id = 'OOOOOO'
    aws_secret_access_key = 'fuckyou'
    bucket_name = 'bucket_name'
    # Initialize S3 client
    s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

    s3.put_object(Body=csv_string, Bucket=bucket_name, Key="csv/a1.csv")
    log.info(":::::: uploading to S3 finished ::::::")


def append_dataframes(dataframes):
    schema = dataframes[0].schema
    # if not all(df.schema == schema for df in a):
    #     raise ValueError("All DataFrames must have the same schema as the first DataFrame.")
    result_df = functools.reduce(DataFrame.union, dataframes)
    log.info(f"Count of all appended dataframes ===>> {result_df.count()}")
    return result_df

def do_transformations(df:DataFrame):

    log.info(":::::: Transformations started ::::::")
    # removing $ symbol from salary column
    df = df.withColumn('salary', translate('salary', '$', ''))

    # changeing the data type of salary
    df = df.withColumn("salary", df["salary"].cast(IntegerType()))

    # replacing null values with mean or average in salary column
    mean_val = df.select(mean(df.salary)).collect()
    log.info(f'mean value of salary ==> {mean_val[0][0]}')
    mean_salary = mean_val[0][0]

    # now using men_salary value to fill the nulls in salary column
    df = df.na.fill(mean_salary, subset=['salary'])

    # considering only Male and Female in the gender column
    df = df.filter(df["gender"].isin(["Male", "Female"]))

    log.info(":::::: Transformations finished ::::::")

    return df

def read_data_from_json(path):
    df = spark.read.json(path)
    log.info("reading data from JSON ==> ")
    return df


if __name__ == '__main__':
    
    a = []
    df1 = _get_data_jdbc(spark)
    print("count of JDBC data ===>>", df1.count())
    df2 = _get_api_data(spark, "https://{host-name}/v2/emp/getdata/")
    print("count of API data ===>>", df2.count())
    df3 =read_data_from_json('data.json')
    print("count of JSON data ===>>", df3.count())
    a.append(df1)
    a.append(df2)
    a.append(df3)
    df1 = append_dataframes(a)
    df = do_transformations(df1)
    csv_string = df.toPandas().to_csv(index=False)
    upload_to_s3(csv_string)

    spark.stop()
