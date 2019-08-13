#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : spark.py
# @Author: xi Wang
# @Date  : 2019-07-19
# @Desc  :

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import udf
from pyspark.sql import functions as F

from defineUDF.get_grade import *

if __name__ == '__main__':
    spark = SparkSession.builder.appName("project").getOrCreate()

    data2017 = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("file:///Users/searainbow/Downloads/Beijing_2017_HourlyPM25_created20170803.csv") \
        .select("Year", "Month", "Day", "Hour", "Value", "QC Name")

    """列名转成小写,空格替换成下划线"""
    data2017 = data2017.select([F.col(x).alias(x.lower().replace(" ", "_")) for x in data2017.columns])

    """dataframe udf"""
    # grade_funcation_udf = udf(get_grade, StringType())

    """spark sql udf"""
    spark.udf.register("grade_udf", get_grade, StringType())

    # group2017 = data2017.withColumn("Grade", grade_funcation_udf(data2017['Value'])) \
    #     .groupBy("Grade") \
    #     .count()
    # # df.printSchema()
    #
    # group2017.select("Grade", "count") \
    #     .withColumn("precent", group2017['count'] / data2017.count() * 100) \
    #     .show()
    #
    #
    data2017.createGlobalTempView("PM25")
    spark.sql("""
            SELECT grade_udf(value) as grade, count(*) as cnt 
                FROM global_temp.PM25 
            group by grade 
            order by cnt desc
    """).show()

    spark.stop()
