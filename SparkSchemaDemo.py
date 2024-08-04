from pyspark.sql import  SparkSession
from pyspark.sql.types import StructField, StructType, DateType, StringType, IntegerType

from lib.logger import Log4J

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("SparkSchemaDemo") \
        .getOrCreate()

    logger = Log4J(spark)

    flightSchemaStruct = StructType([
        StructField("FL_DATE", DateType()),
        StructField("OP_CARRIER", StringType()),
        StructField("OP_CARRIER_FL_NUM", IntegerType()),
        StructField("ORIGIN", StringType()),
        StructField("ORIGIN_CITY_NAME", StringType()),
        StructField("DEST", StringType()),
        StructField("DEST_CITY_NAME", StringType()),
        StructField("CRS_DEP_TIME", IntegerType()),
        StructField("DEP_TIME", IntegerType()),
        StructField("WHEELS_ON", IntegerType()),
        StructField("TAXI_IN", IntegerType()),
        StructField("CRS_ARR_TIME", IntegerType()),
        StructField("ARR_TIME", IntegerType()),
        StructField("CANCELLED", IntegerType()),
        StructField("DISTANCE", IntegerType())
    ])

    flightTimeCSVDF = spark.read\
        .format("csv") \
        .option("header", "true") \
        .schema(flightSchemaStruct)\
        .load("E:/nauka/spark/SparkSchema/data/flight-time.csv")

    flightTimeCSVDF.show(5)
    logger.info("CSV Schema: " + flightTimeCSVDF.schema.simpleString())

    flightTimeJsonDF = spark.read \
        .format("json") \
        .load("E:/nauka/spark/SparkSchema/data/flight-time.json")

    flightTimeJsonDF.show(5)
    logger.info("Json Schema: " + flightTimeJsonDF.schema.simpleString())

    flightTimeParquetDF = spark.read \
        .format("parquet") \
        .load("E:/nauka/spark/SparkSchema/data/flight-time.parquet")

    flightTimeParquetDF.show(5)
    logger.info("Json Schema: " + flightTimeParquetDF.schema.simpleString())