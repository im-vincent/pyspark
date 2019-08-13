#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : cal_housing.py
# @Author: xi Wang
# @Date  : 2019-07-11
# @Desc  :
from pyspark.sql import SparkSession

# 初始化SparkSession和SparkContext
from pyspark.sql.types import FloatType

spark = SparkSession.builder \
    .master("local") \
    .appName("California Housing ") \
    .config("spark.executor.memory", "1gb") \
    .getOrCreate()
sc = spark.sparkContext

# 读取数据并创建RDD
rdd = sc.textFile("/Users/searainbow/Downloads/CaliforniaHousing/cal_housing.data")

# 读取数据每个属性的定义并创建RDD
header = sc.textFile("/Users/searainbow/Downloads/CaliforniaHousing/cal_housing.domain")

rdd = rdd.map(lambda line: line.split(","))

from pyspark.sql import Row

df = rdd.map(lambda line: Row(longitude=line[0],
                              latitude=line[1],
                              housingMedianAge=line[2],
                              totalRooms=line[3],
                              totalBedRooms=line[4],
                              population=line[5],
                              households=line[6],
                              medianIncome=line[7],
                              medianHouseValue=line[8])).toDF()


def convertColumn(df, names, newType):
    for name in names:
        df = df.withColumn(name, df[name].cast(newType))
    return df


columns = ['households', 'housingMedianAge', 'latitude', 'longitude', 'medianHouseValue', 'medianIncome', 'population',
           'totalBedRooms', 'totalRooms']
from pyspark.sql.functions import *
df = convertColumn(df, columns, FloatType())
df = df.withColumn("medianHouseValue", col("medianHouseValue") / 100000)
df = df.withColumn("roomsPerHousehold", col("totalRooms") / col("households"))\
    .withColumn("populationPerHousehold", col("population") / col("households"))\
    .withColumn("bedroomsPerRoom", col("totalBedRooms") / col("totalRooms"))

df.groupBy("housingMedianAge").count().sort("housingMedianAge", ascending=False).show()

spark.stop()
