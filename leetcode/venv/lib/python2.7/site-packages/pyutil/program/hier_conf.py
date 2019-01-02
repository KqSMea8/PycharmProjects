#!/usr/bin/env python
# coding: utf-8

#
# hier_conf 是具有命名空间/hierarchy层次的配置管理
# 主要解决打平的配置管理在使用中遇到名字冲突的问题
#
# 配置文件使用yaml格式，主要为了方便解析
#
__author__ = 'zhenghuabin'
import os
from threading import RLock


class ConfBackend(object):
    def __init__(self):
        pass

    def get(self, key):
        return None


class FileConfBackend(ConfBackend):
    def __init__(self, prefix_paths=['/etc/ss_conf', ]):
        """
        可以指定多个配置路径，后面的配置项覆盖前面的
        :param prefix_paths:
        :return:
        """
        super(FileConfBackend, self).__init__()
        self.prefix_paths = prefix_paths
        self.path_doc_map = dict()
        self.lock = RLock()

    def _load_doc(self, p):
        doc = dict()
        for prefix in self.prefix_paths:
            file = os.path.join(prefix, p)
            if not os.path.isfile(file):
                continue
            with open(file, 'r') as fp:
                import yaml

                obj = yaml.safe_load(fp)
                doc.update(obj)
        return doc

    def get(self, key):
        key = key.strip('/')
        p, k = os.path.split(key)
        with self.lock:
            doc = self.path_doc_map.get(p)
            if doc is None:
                doc = self._load_doc(p)
                self.path_doc_map[p] = doc
            return doc.get(k)
        return None


class Conf(object):
    """
    usage:
    >>> conf = Conf(FileConfBackend())
    >>> conf.get('/some/key')
    >>> conf.get_values('/some/list')
    """

    def __init__(self, backend=None):
        self.backend = backend or FileConfBackend()
        pass

    def get(self, key):
        return self.backend.get(key)

    def get_values(self, key):
        val = self.backend.get(key) or ''
        return filter(lambda x: x, [element.strip() for element in val.split(',')])


conf = Conf()


def config_conf(backend):
    """
    usage:
    >>> from pyutil.program import hier_conf
    >>> hier_conf.config_conf(FileConfBackend(['/etc/ss_conf',]))
    >>> hier_conf.conf.get('/some/key')
    >>> hier_conf.conf.get_values('/some/list')
    """
    global conf
    conf = Conf(backend)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='convert conf file to yaml format conf')
    parser.add_argument('--infile', required=True, help=('input file name'))
    parser.add_argument('--outfile', help=('output file name'))
    args = parser.parse_args()
    input_file = args.infile
    output_file = args.outfile or os.path.splitext(input_file)[0] + '.yaml'
    from pyutil.program.conf2 import Conf

    conf = Conf(input_file)
    obj = conf.get_all()
    fp = open(output_file, 'w')
    for k, v in obj.iteritems():
        fp.write('%s: %s\n' % (k, v))





