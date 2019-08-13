#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : connect_mysql.py
# @Author: xi Wang
# @Date  : 2019-07-15
# @Desc  :


from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

spark = SparkSession.builder.appName("Python Spark SQL basic example") \
    .config('spark.some.config,option0', 'some-value') \
    .getOrCreate()
ctx = SQLContext(spark)
jdbcDf = ctx.read.format("jdbc").options(url="jdbc:mysql://localhost:3306/tujia",
                                         driver="com.mysql.jdbc.Driver",
                                         dbtable="tujia_basic", user="wangxi",
                                         password="wangxi").load()

jdbcDf.createOrReplaceTempView("tujia")

sql = """SELECT trim(title) as title,
            split(types, '/')[0] as structure,
            split(types, '/')[1] as room,
            split(types, '/')[2] as area,
            substring(split(types, '/')[4], 0, length(split(types, '/')[4])-1) as bed,
            cast(price as int)
        FROM tujia
        ORDER BY bed, price
        """



sqlDF = spark.sql(sql)

sqlDF.show()
# sqlDF.coalesce(1).write.format("csv").option("header", "true").save("/Users/searainbow/Documents/code/python/pyspark/test.csv")

import matplotlib.pyplot as plt
import seaborn as sns


s = sqlDF.toPandas()

x = s['title']
y = s['price']

sns.barplot(x, y)
plt.show()

spark.stop()