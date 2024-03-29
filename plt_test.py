#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : plt_test.py
# @Author: xi Wang
# @Date  : 2019-07-16
# @Desc  :


import matplotlib.pyplot as plt
import seaborn as sns
# 数据准备
x = ['Cat1', 'Cat2', 'Cat3', 'Cat4', 'Cat5']
y = [5, 4, 8, 12, 7]
# 用Matplotlib画条形图
plt.bar(x, y)
plt.show()
# 用Seaborn画条形图
sns.barplot(x, y)
plt.show()