#!/usr/bin/env python
# -*- coding: utf-8 -*-
import unittest
import time, string, random, logging, os

#import pyutil.twemproxy as twemproxy
from pyutil.twemproxy import ClientConf

ZONE = os.path.dirname(__file__) + '/' + './conf'

class TestBasic(unittest.TestCase):
    def setUp(self):
        pass

    def test_clusters(self):
        expected = {
            'springdb_c1': {
                'cluster': 'springdb_c1',
                'real_cluster': 'springdb_c1',
                'servers': ['192.168.20.41:5390', '192.168.20.41:5390'],
                'tables': ['sandbox', 'unittest'],
            },
            'springdb_ref1': {
                'cluster': 'springdb_ref1',
                'real_cluster': 'springdb_c1',
                'servers': ['192.168.20.41:5390', '192.168.20.41:5390'],
                'tables': ['sandbox', 'unittest'],
            },
        }

        for cluster, cluster_conf in expected.items():
            result = ClientConf.get_cluster_conf('springdb', cluster, ZONE)
            self.assertEqual(result, cluster_conf)

    def test_failure(self):
        clusters = ['springdb_f1', 'springdb_f2', 'springdb_ref2']
        for cluster in clusters:
            with self.assertRaises(ValueError):
                result = ClientConf.get_cluster_conf('springdb', cluster, ZONE)

    def test_none(self):
        with self.assertRaises(ValueError):
            ClientConf.get_cluster_conf('springdb', 'springdb_ref4', zone=ZONE)

if __name__ == '__main__':
    FORMAT = '%(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(format=FORMAT)
    #logging.getLogger().setLevel(logging.INFO)
    logging.getLogger().setLevel(logging.DEBUG)
    unittest.main()

