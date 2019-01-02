#!/usr/bin/env python
# coding: utf-8
__author__ = 'zhenghuabin'

import unittest
import os


class TestHierConf(unittest.TestCase):
    def setUp(self):
        self.dir_online = 'conf1'
        self.dir_test = 'conf2'
        self.db_online = {'ss': '10.4.17.11:3306', 'op': '10.4.18.11:3307'}
        self.db_test = {'op': '10.4.18.11:3307'}
        self.redis_online = {'ad': '10.4.15.11:8888,10.4.15.12:7777', 'dongtai': '10.4.13.15:3333'}
        self.redis_test = {'ad': '10.4.17.164:8888,10.4.15.12:7777'}
        import yaml

        try:
            os.mkdir(self.dir_online)
            os.mkdir(self.dir_test)
        except:
            pass

        with open(os.path.join(self.dir_online, 'db'), 'w') as fp:
            yaml.dump(self.db_online, fp)
        with open(os.path.join(self.dir_online, 'redis'), 'w') as fp:
            yaml.dump(self.redis_online, fp)
        with open(os.path.join(self.dir_test, 'db'), 'w') as fp:
            yaml.dump(self.db_test, fp)
        with open(os.path.join(self.dir_test, 'redis'), 'w') as fp:
            yaml.dump(self.redis_test, fp)

    def tearDown(self):
        pass

    def test_file_backend(self):
        from pyutil.program import hier_conf

        hier_conf.config_conf(hier_conf.FileConfBackend([self.dir_online, self.dir_test]))
        conf = hier_conf.conf
        self.assertEqual(conf.get('/db/ss'), self.db_online['ss'])
        self.assertEqual(conf.get('/db/op'), self.db_test['op'])
        self.assertEqual(conf.get('/redis/ad'), self.redis_test['ad'])
        self.assertEqual(conf.get_values('/redis/ad'), self.redis_test['ad'].split(','))
        self.assertEqual(conf.get('/not/exists'), None)
        self.assertEqual(conf.get_values('/not/exists'), [])

