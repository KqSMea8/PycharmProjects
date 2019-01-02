#!/usr/bin/env python
# coding=utf8
import sys, os, signal, datetime, logging, time, hashlib, codecs, csv, cPickle, copy

def calculate_gini(values):
    '''
    Given a list of numerical values, calculate its gini factor and related bin values
    params values: iterable numerical values
    return value: gini: float 
    return value: ratio_list: [float]
    '''
    sorted_value_list = sorted(values)
    acc_value_list = [0] * len(values)
    ratio_list = [0] * len(sorted_value_list)
    acc_value_list[0] = sorted_value_list[0]
    for loop in range(1, len(sorted_value_list)):
        acc_value_list[loop] = acc_value_list[loop - 1] + sorted_value_list[loop]
    sum_value = sum(sorted_value_list)
    gini = 0
    for loop, val in enumerate(acc_value_list):
        ratio = float(val) / sum_value
        gini += (float(loop + 1) / len(acc_value_list) - ratio)
        ratio_list[loop] = ratio

    gini /= len(acc_value_list)
    gini /= 0.5

    return gini, ratio_list

if __name__ == '__main__':
    signal.signal(signal.SIGTERM, signal.SIG_DFL)
    signal.signal(signal.SIGINT, signal.SIG_DFL)

    values = [5, 3, 7, 1, 9]
    gini, ratios = calculate_gini(values)
    print gini
    print ', '.join(['%8.4f' % ratio for ratio in ratios])

    values = [3, 3, 3, 3, 3]
    gini, ratios = calculate_gini(values)
    print gini
    print ', '.join(['%8.4f' % ratio for ratio in ratios])
