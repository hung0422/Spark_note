from pyspark.sql import SparkSession
from pyspark.sql.functions import *

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .getOrCreate()

    data = spark.read.csv("hdfs://devenv/user/spark/spark_sql_101/crime/data/", header=True, inferSchema=True)

    crime_2015_6 = data.filter("year >= 2015").drop("lsoa_code")

    convictions_by_borough = crime_2015_6.groupBy("borough").agg({"value": "sum"})

    convictions_by_borough = convictions_by_borough.withColumnRenamed("sum(value)", "num_of_convictions")

    total_convictions = convictions_by_borough.agg({"num_of_convictions": "sum"}).collect()[0][0]

    convictions_by_borough_with_percentage = convictions_by_borough.withColumn("percentage_convictions",
                            format_number(convictions_by_borough["num_of_convictions"] / total_convictions * 100, 2))
    
    convictions_by_borough_with_percentage.show()
