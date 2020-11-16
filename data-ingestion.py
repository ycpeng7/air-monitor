from pyspark.sql import SparkSession
import sys


def read_aq(url):
    spark = SparkSession\
            .builder\
            .appName("s3 Air Quality to Spark")\
            .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    obj_date = '2013-11-26'
    df = spark.read.json(url + obj_date + '/' + obj_date + '.ndjson')
    print(df.count())


def main():
    read_aq(sys.argv[1])


if __name__ == '__main__':
    main()