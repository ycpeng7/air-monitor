import logging
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode



def read_aq(url: str = ""):
    """
    Read air quality data from S3 and keep specific columns
    """
    spark = SparkSession\
            .builder\
            .appName("s3 Air Quality to Spark")\
            .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    if url != "":
        obj_date = '2014*'
        path = url + obj_date + '/' + obj_date + '.ndjson'
    else:
        path = "aq_test.ndjson"
    df = spark.read.json(path)
    logging.warning("Read {} rows".format(df.count()))
    df = df.select('city', 'country', 'date', 'unit', 'value')\
        .withColumn('utc_date', df['date'].utc).drop('date')
    
    return df
    

def main():
    cleaned_aq = read_aq(sys.argv[1])


if __name__ == '__main__':
    main()