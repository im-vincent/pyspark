#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : WordCount.py
# @Author: xi Wang
# @Date  : 2019-07-11
# @Desc  :


from pyspark.sql import SparkSession
from pyspark.sql.functions import *

if __name__ == "__main__":
    spark = SparkSession.builder.appName('WordCount').getOrCreate()

    lines = spark.read.text("sample.txt")

    wordCounts = lines.select(explode(split(lines.value, " ")).alias("word")).groupBy("word").count()
    wordCounts.show()

    spark.stop()
