# -*- coding: utf-8 -*-
import collections
import contextlib
import functools
import re
import socket
import sys
import time
import traceback

import elasticsearch

global es_client
es_client = None

name_re = re.compile(r"^[-a-z0-9]+$")

RELEASE_TYPES = ("release", "restart", "rollback")

def init(conf):
    es_hosts = filter(None, conf.get_values("elasticsearch_hosts"))
    if not es_hosts:
        raise ValueError("miss a elasticsearch_hosts configure")

    global es_client
    es_client = elasticsearch.Elasticsearch(hosts=es_hosts)

def make_es_index(es_index, es_type, es_doc):
    try:
        if es_client is None:
            raise ValueError("should invoke init(conf) at first")

        return es_client.index(es_index, es_type, es_doc)
    except Exception:
        sys.stderr.write(traceback.format_exc())
        return

def _convert(iterator):
    if iterator is None:
        return []

    if isinstance(iterator, basestring):
        raise TypeError("invalid argument type")

    if not isinstance(iterator, collections.Iterable):
        raise TypeError("the argument must be iterable")

    return iterator

@contextlib.contextmanager
def collect_release_info(repo, operator, commit_message="", extra_arguments=None,
                         changed_files=None, release_type="relase", reviewers=None,
                         services=None, repo_owners=None, extra_info=""):
    if release_type not in RELEASE_TYPES:
        raise ValueError("unsupported relase_type")

    if not isinstance(extra_info, basestring):
        raise TypeError("extra_info must be a string")

    extra_arguments = _convert(extra_arguments)
    changed_files = _convert(changed_files)
    reviewers = _convert(reviewers)
    services = _convert(services)
    repo_owners = _convert(repo_owners)
    repo_owners = repo_owners if repo_owners else [operator]

    doc = dict(
        begin=int(time.time()),
        repo=repo,
        operator=operator,
        release_type=release_type,
        commit_message=commit_message,
        extra_arguments=",".join(extra_arguments),
        changed_files=",".join(changed_files),
        reviewers=",".join(reviewers),
        services=",".join(services),
        repo_owners=",".join(repo_owners),
        extra_info=extra_info,
    )

    try:
        yield
    except Exception:
        raise

    doc["end"] = int(time.time())
    doc["duration"] = doc["end"] - doc["begin"]
    make_es_index("release_info", repo, doc)

def collect_operation_info(command_type, command, operator, service="", extra_info=""):
    if not isinstance(extra_info, basestring):
        raise TypeError("extra_info must be a string")

    doc = dict(
        command=command,
        operator=operator,
        service=service,
        timestamp=int(time.time()),
        host=socket.gethostname(),
        extra_info=extra_info,
    )

    make_es_index("operation_info", command_type, doc)

@contextlib.contextmanager
def collect_exec_result(name, err_return_code=-1, succ_return_code=0, extra_info=""):
    if not (isinstance(err_return_code, int)
            and isinstance(succ_return_code, int)):
        raise TypeError("err_return_code/succ_return_code must be int")

    if not isinstance(extra_info, basestring):
        raise TypeError("extra_info must be a string")

    if not name_re.search(name):
        raise TypeError("name could be alphabet, digit, underscore and dash")

    doc = dict(
        begin=int(time.time()),
        host=socket.gethostname(),
        traceback="",
        extra_info=extra_info,
    )

    exception = None

    try:
        yield
    except Exception as e:
        exception = e
        doc["return_code"] = err_return_code
        doc["traceback"] = traceback.format_exc()
    else:
        doc["return_code"] = succ_return_code

    doc["end"] = int(time.time())
    doc["duration"] = doc["end"] - doc["begin"]

    make_es_index("exec_result", name, doc)

    if exception:
        raise exception

def emit_release_info(repo, operator, commit_message="", extra_arguments=None,
                      changed_files=None, release_type="release", reviewers=None,
                      services=None, repo_owners=None, extra_info=""):

    def deco(func):

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            with collect_release_info(
                    repo,
                    operator,
                    commit_message=commit_message,
                    extra_arguments=extra_arguments,
                    changed_files=changed_files,
                    release_type=release_type,
                    reviewers=reviewers,
                    services=services,
                    repo_owners=repo_owners,
                    extra_info=extra_info):
                return func(*args, **kwargs)

        return wrapper

    return deco

def emit_exec_result(name=None, err_return_code=-1, succ_return_code=0, extra_info=""):
    """ Collect execution result and send those to Elasticsearch.
    """

    def deco(func):

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            _name = name if name and name.strip() else func.func_name
            with collect_exec_result(_name,
                                     err_return_code=err_return_code,
                                     succ_return_code=succ_return_code,
                                     extra_info=extra_info):
                return func(*args, **kwargs)

        # You can't decorate a lambda without "name" argument.
        if not (name and name.strip()) and func.func_name == "<lambda>":
            raise TypeError("must specify the name argument")

        return wrapper

    return deco
