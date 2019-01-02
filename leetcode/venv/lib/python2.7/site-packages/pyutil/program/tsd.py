#!/usr/bin/env python

import time
import simplejson as json 
import socket
import logging
import math
from httplib import HTTPConnection

class Tsd():
    """
    tsdb http api wrapper
    """
    def __init__(self, host="10.4.16.47", port=8400):
        self.port = str(port)
        self.host = host
        self.purl = "/api/query?"

    def _filter(self, begin, end, points):
        """
        filter the data in the extra timespan
        """
        values = {}
        if str(begin).find("d-ago") > 0:
            day = begin.strip("d-ago")
            begin = int(end) - int(day)*24*3600
        elif str(begin).find("h-ago") > 0:
            hour = begin.strip("h-ago")
            begin = int(end) - int(hour)*3600
        elif str(begin).find("m-ago") > 0:
            minute = begin.strip("m-ago")
            begin = int(end) - int(minute)*60
        for k in sorted(points.keys()):
            if int(k) >= int(begin) and int(k) <= int(end):
                values[k] = points[k]
        return values

    def query(self, start_time, end_time, metric_name, murl, tag="", timeout=60, retry=3):
        """kwds:metric tags pairs,
           eg: tag="host=*,type=op"
        """
        if tag != "":
            tag = "{%s}" % tag
        qurl = "%sstart=%s&end=%s&m=%s:%s%s&ascii" %(self.purl, start_time, end_time, murl, metric_name, tag)
        conn = HTTPConnection(self.host, self.port, timeout=timeout)
        conn.request('GET', qurl)
        for i in range(retry):
            try:
                response = conn.getresponse()
                text = response.read()
                break
            except Exception,e:
                msg = "requeset tsdb error"
                logging.warn("Cli request tsdb error: %s, retry num: %d" % (str(e), i))
                if i == retry -1:
                    return None, str(e)

        res = json.loads(text)
        if response.status != 200:
            msg = res['error']['message']
            return None, msg

        results = []
        for ret in res:
            if 'dps' in ret.keys():
                ret['dps'] = self._filter(start_time, end_time, ret['dps'])
                results.append(ret)

        return results, ""


class TsdbAnalyze():
    """
    analyze tsdb metric data
    """
    def __init__(self, host="10.4.16.47", port=8402):
        self.tsd = Tsd(host, str(port))

    def query(self, start_time, end_time, metric_name, murl, tag="", timeout=60, retry=3):
        points = self.tsd.query(start_time, end_time, metric_name, murl, tag, timeout, retry)
        adj_points = self.adjust_timestamp(points[0])
        
        return adj_points

    def adjust_timestamp(self, points):
        for point in points:
            for timestamp in point['dps'].keys():
                key = str((int(timestamp)/15)*15)
                tmp_value = point['dps'][timestamp]
                del point['dps'][timestamp]
                point['dps'][key] = tmp_value 

        return points

    def variance(self, points):
        ts_list = self.get_timestamp_list(points)
        average_points = self.average(points)
        variance_dict = {}
        for point in points:
            sum = 0
            for timestamp in ts_list:
                value =  point['dps'][str(timestamp)] if str(timestamp) in point['dps'].keys() else 0
                sum = sum + (value - average_points[timestamp])*(value - average_points[timestamp])
            key = point['tags'].values()[0]
            value = math.sqrt(sum)
            variance_dict[key] = value
        return variance_dict
        
    def average(self, points, only_number=False):
        ts_list = self.get_timestamp_list(points)
        av_dict = {}
        for timestamp in ts_list:
            sum = 0
            num = 0
            for point in points:
                sum = sum + point['dps'][str(timestamp)] if str(timestamp) in point['dps'].keys() and  point['dps'][timestamp] >0 else 0
                num = num + 1
            average = sum/num
            av_dict[timestamp] = average
        return av_dict

    def average_all(self, points, only_number=False):
        av_dict = self.average(points, only_number)

        if len(av_dict) == 0:
            return None
        sum = 0
        for key in av_dict.keys():
            sum = sum + av_dict[key]

        return sum/len(av_dict)

    def get_timestamp_list(self, points):
        timestamp_list = []
        for point in points:
            timestamp_list.extend(point['dps'].keys())
        timestamp_list = sorted(list(set(timestamp_list)))
        return timestamp_list

    def rate_sum(self, points):
        sum = 0
        ts_list = self.get_timestamp_list(points)
        for point in points:
            values = point['dps']
            temp_sum = 0
            for i in range(len(ts_list)-1):
                temp_diff = values[ts_list[i+1]] - values[ts_list[i]]
                if temp_diff < 0.0001:
                    continue
                temp_sum = temp_sum + temp_diff
            sum = sum + temp_sum
        return sum


if __name__ == "__main__":
    ta = TsdbAnalyze()
    ret = ta.query(1425266738, 1425266782, 'data.sort.throughput', 'sum:rate', tag="host=*")
    #res = ta.adjust_timestamp(ret)
    #res = ta.average(ret)
    res = ta.variance(ret)
    sorted(res.items(), key=lambda res:res[1]) 
    print res
