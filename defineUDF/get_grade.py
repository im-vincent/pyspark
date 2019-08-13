#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : get_grade.py
# @Author: xi Wang
# @Date  : 2019-07-19
# @Desc  :


def get_grade(value):
    if value <= 50:
        return "健康"
    elif value <= 100:
        return "中等"
    elif value <= 150:
        return "对敏感人群不健康"
    elif value <= 200:
        return "不健康"
    elif value <= 300:
        return "非常不健康"
    elif value <= 500:
        return "危险"
    elif value > 500:
        return "爆表"
    else:
        return None
