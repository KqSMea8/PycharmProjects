#!/usr/bin/python2.6
#coding=utf8

import logging, logging.handlers, datetime, re
from scribe_logger.logger import ScribeLogHandler

def config_logging(conf=None):
    '''
    Deprecated, use config_logging or init_logging in pyutil.program.log
    '''
    if not conf:
        class Conf:
            pass
        conf = Conf()
        conf.log_level = logging.DEBUG
        conf.console_log_level = logging.DEBUG
        conf.log_file = 'server.log'
    log_format = '%(asctime)s %(levelname)-5s %(message)s'
    if hasattr(conf, 'log_format'):
        log_format = conf.log_format
    if not hasattr(conf, 'console_log_level'):
        conf.console_log_level = logging.ERROR
    logger = logging.getLogger()
    logger.setLevel(conf.log_level)
    ch = logging.StreamHandler()
    ch.setFormatter(logging.Formatter(log_format))
    ch.setLevel(conf.console_log_level)
    logger.addHandler(ch)
    fh = logging.handlers.TimedRotatingFileHandler(conf.log_file, 'midnight')
    #fh.setFormatter(logging.Formatter('%(asctime)s %(levelname)-5s %(message)s', '%m-%d %H:%M:%S'))
    fh.setFormatter(logging.Formatter(log_format))
    fh.setLevel(conf.log_level)
    logger.addHandler(fh)
    scribe_host = '127.0.0.1'
    scribe_port = 1464
    if hasattr(conf, 'scribe_host'):
        scribe_host = conf.scribe_host
    if hasattr(conf, 'scribe_port'):
        scribe_port = conf.scribe_port
    if hasattr(conf, 'category'):
        scribe = ScribeLogHandler(category=conf.category, host=scribe_host, port=scribe_port)
        scribe.setFormatter(logging.Formatter(log_format))
        logger.addHandler(scribe)
    elif hasattr(conf, 'log_category'):
        scribe = ScribeLogHandler(category=conf.log_category, host=scribe_host, port=scribe_port)
        scribe.setFormatter(logging.Formatter(log_format))
        logger.addHandler(scribe)

def str_dict(struct):
    if isinstance(struct, dict):
        for k, v in struct.items():
            del struct[k]
            struct[str(k)] = '' if v is None else str(v)
    elif hasattr(struct, '__dict__'):
        attr_names = struct.__dict__.keys()
        for name in attr_names:
            sub_struct = getattr(struct, name)
            str_dict(sub_struct)
    elif isinstance(struct, (list, tuple)):
        for sub_struct in struct:
            str_dict(sub_struct)

def should_truncate_value(key):
    if 'url' in key:
        return False
    else:
        return True

def get_dict_content(d):
    new_d = {}
    image_keys = ('contact_avatar', 'phone_image', )
    ignore_keys = ('Referer', )
    for k, v in d.items():
        if k in ignore_keys:
            continue
        nv = v
        if k in image_keys:
            nv = 'image(not shown)'
        else:
            if should_truncate_value(k):
                nv = str(v).decode('utf8', 'ignore')
                if len(nv) > 60:
                    nv = nv[:30].encode('utf8', 'ignore') + ' ... ' + nv[-30:].encode('utf8', 'ignore')
                else:
                    nv = nv[:60].encode('utf8', 'ignore')
            nv = ' '.join(nv.split('\n'))
        if isinstance(nv, unicode):
            nv = nv.encode('utf8')
        if isinstance(k, unicode):
            k = k.encode('utf8')
        new_d[str(k)] = nv
    content_list = []
    priority_keys = ('task_id', 'type', 'seed_id', 'chan', 'url', 'title', 'page_num', 'city_id')
    value_only_keys = ('chan', 'type', 'url', 'title')
    for k in priority_keys:
        if k in new_d:
            v = new_d.pop(k)
            if not k in value_only_keys:
                if k == 'task_id':
                    continue
                else:
                    v = '%s=%s' %(k, v)
            content_list.append(v)
    return ', '.join(content_list + ['%s=%s' %(k, v) for k, v in new_d.items()])

def get_task_info(task):
    content_list = []
    priority_keys = ('task_id', 'type', 'seed_id', 'chan', 'url', 'page_num')
    value_only_keys = ('chan', 'type', 'url')
    for k in priority_keys:
        if k in task:
            v = task[k]
            if not k in value_only_keys:
                if k == 'task_id':
                    content = '%s_%03d' %(k, int(v))
                else:
                    content = '%s=%s' %(k, v)
            else:
                if k == 'type':
                    content = '%5s' %v
                else:
                    content = v
            content_list.append(content)
    return ', '.join(content_list)

def normalize_number(value):
    if isinstance(value, int):
        return value
    value = unicode(value)
    for k, v in [(u'一', '1'), (u'两', '2'), (u'二', '2'), (u'三', '3'), (u'四', '4'), (u'五', '5'), (u'六', '6'), (u'七', '7'), (u'八', '8'), (u'九', '9'), 
                (u'零', '0'), (u'〇', '0')]:
        value = value.replace(k, v)
    try:
        digits = ''
        for v in value:
            if v.isdigit() or v == '.':
                digits += v
        value = int(float(digits))
    except:
        value = None
    return value

def smart_decode(value):
    methods = ('utf8', 'gbk', )
    min_bad_chars_count = 999999
    decoded_value = ''
    for method in methods:
        dv = value.decode(method, 'replace')
        bad_chars_count = len(re.findall(u'\uFFFD', dv))
        if bad_chars_count == 0:
            return dv
        if bad_chars_count < min_bad_chars_count:
            decoded_value = dv
            min_bad_chars_count = bad_chars_count
    if min_bad_chars_count >= 50:
        return ''
    return decoded_value

class MultiPatternMatcher(object):

    def __init__(self, patterns=[]):
        self.root = {}
        for pattern in patterns:
            if isinstance(pattern, (tuple, list)):
                self.add_pattern(*pattern)
            else:
                self.add_pattern(pattern)

    def add_pattern(self, pattern, data=None):
        cur_node = self.root
        for char in pattern:
            cur_node = cur_node.setdefault(char, {})
        cur_node['$'] = True
        cur_node['data'] = data

    def _match(self, text):
        text_len = len(text)
        for start in range(0, text_len):
            cur_node, cur_pattern, cur_data = self.root, '', None
            for end in range(start, text_len):
                char = text[end]
                if char in cur_node:
                    cur_node = cur_node[char]
                else:
                    break
                if cur_node.get('$', False) == True:
                    cur_pattern = text[start: end+1]
                    cur_data = cur_node['data']
            if cur_pattern:
                return cur_pattern, cur_data
        return None, None

    def match(self, text):
        pattern, data = self._match(text)
        return pattern, data
