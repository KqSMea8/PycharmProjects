#!/usr/bin/env python

import time
import simplejson as json 
from httplib import HTTPConnection

class TsdbQuery():
    def __init__(self, port="80", host="metrics.byted.org"):
        self.port = port
        self.host = host
        self.purl = "/api/query?"

        #self.url = "%s%s&%s&m=%s&ascii" % (self.basic_url, start_time, end_time, metric_name)

    def filter(self, begin, end, points):
        values = []
        if str(begin).find("d-ago") > 0:
            day = begin.strip("d-ago")
            begin = int(end) - int(day)*24*3600
        for k in sorted(points.keys()):
            if int(k) >= int(begin) and int(k) <= int(end):
                values.append(points[k])
        return values

    def sum(self, start_time, end_time, metric_name, timeout=600, **kwds):
        """
            kwds:metric tags pairs
        """
        tags = ","
        tpre = "host=*"
        for key in kwds:
            if key == "host":
                tpre = "%s=%s" %(key, str(kwds[key]))
            tags = tags + "%s=%s," %(key, str(kwds[key]))
        tags = tags.rstrip(",")
        qurl = "%sstart=%s&end=%s&m=sum:%s{%s%s}&ascii" %(self.purl, start_time, end_time, metric_name, tpre, tags)
        print qurl
        conn = HTTPConnection(self.host, self.port, timeout=timeout)
        conn.request('GET', qurl)
        response = conn.getresponse()
        text = response.read()
        if response.status != 200:
            res = json.loads(text)
            msg = res['error']['message']
            return None, msg

        sum = 0
        points = json.loads(text)
        for point in points:
            values = self.filter(start_time, end_time, point['dps'])
            temp_sum = 0
            for i in range(len(values)-1):
                temp_diff = values[i+1] - values[i]
                if temp_diff < 0.0001:
                    continue
                temp_sum = temp_sum + temp_diff
            sum = sum + temp_sum

        return sum
                

    def sum_rate(self, start_time, end_time, metric_name, timeout=600, **kwds):
        """
            kwds:metric tags pairs
        """
        tags = ""
        for key in kwds:
            if key == "host":
                tpre = "%s=%s" %(key, str(kwds[key]))
            tags = tags + "%s=%s," %(key, str(kwds[key]))
        tags = tags.rstrip(",")
        qurl = "%sstart=%s&end=%s&m=sum:rate:%s{%s}&ascii" %(self.purl, start_time, end_time, metric_name, tags)
        print qurl
        conn = HTTPConnection(self.host, self.port, timeout=timeout)
        conn.request('GET', qurl)
        response = conn.getresponse()
        text = response.read()
        if response.status != 200:
            res = json.loads(text)
            msg = res['error']['message']
            return None, msg

        max = 0
        min = 1000000000
        points = json.loads(text)
        if points <= 0:
            return 0,0
        for point in points:
            values = self.filter(start_time, end_time, point['dps'])
            for i in range(len(values)-1):
                temp_diff = values[i+1] - values[i]
                if values[i] > max:
                    max = values[i]
                if values[i] < min:
                    min = values[i]

        return min,max

    def avg(self, start_time, end_time, metric_name, timeout=600, **kwds):
        """
            kwds:metric tags pairs
        """
        tags = ""
        for key in kwds:
            if key == "host":
                tpre = "%s=%s" %(key, str(kwds[key]))
            tags = tags + "%s=%s," %(key, str(kwds[key]))
        tags = tags.rstrip(",")
        qurl = "%sstart=%s&end=%s&m=avg:%s{%s}&ascii" %(self.purl, start_time, end_time, metric_name, tags)
        print qurl
        conn = HTTPConnection(self.host, self.port, timeout=timeout)
        conn.request('GET', qurl)
        response = conn.getresponse()
        text = response.read()
        if response.status != 200:
            res = json.loads(text)
            msg = res['error']['message']
            return None, msg

        max = 0
        min = 1000000000
        points = json.loads(text)
        if points <= 0:
            return 0,0
        for point in points:
            values = self.filter(start_time, end_time, point['dps'])
            for i in range(len(values)-1):
                temp_diff = values[i+1] - values[i]
                if values[i] > max:
                    max = values[i]
                if values[i] < min:
                    min = values[i]

        return min,max

if __name__ == "__main__":
    import sys
    #if len(sys.argv) < 4:
    #    print "Usage : ./tsdbquery.py start_timestamp end_timestamp, metric_name tagk=tagv tagk=tagv"
    #    sys.exit(1)
    tq = TsdbQuery()
    """
    start_time = sys.argv[1]
    end_time = sys.argv[2]
    if end_time == "now":
        end_time = str(int(time.time()))
    metric_name = sys.argv[3]
    tags_dict = {}
    for i in range(4, len(sys.argv)):
        key = sys.argv[i].split("=")[0]
        value = sys.argv[i].split("=")[1]
        tags_dict[key] = value
    """
    sum = tq.sum("1466565927", "1466566927", "data.adengine.thrift.func_get_text_link.throughput")
    #sum = tq.sum(start_time, end_time, metric_name, **tags_dict)
    print sum

