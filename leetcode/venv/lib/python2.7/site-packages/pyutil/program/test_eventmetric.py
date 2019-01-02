#!/usr/bin/env python
# -*- coding: utf-8 -*-
import socket
import time

import mock
from nose import tools as nosetools

from pyutil.program import eventmetric

@nosetools.raises(ZeroDivisionError)
@mock.patch("pyutil.program.eventmetric.make_es_index")
def test_emit_exec_result_default_err_return_code(mock_index):
    do = eventmetric.emit_exec_result(name="dummy")(lambda: 1/0)
    do()

    err = "ZeroDivisionError: integer division or modulo by zero"
    real_doc = mock_index.call_args[0][2]
    nosetools.eq_(-1, real_doc["return_code"])
    nosetools.assert_in(err, real_doc["traceback"])

@nosetools.raises(ZeroDivisionError)
@mock.patch("pyutil.program.eventmetric.make_es_index")
def test_emit_exec_result_specify_err_return_code(mock_index):
    do = eventmetric.emit_exec_result(name="dummy", err_return_code=1)(lambda: 1/0)
    do()

    err = "ZeroDivisionError: integer division or modulo by zero"
    real_doc = mock_index.call_args[0][2]
    nosetools.eq_(1, real_doc["return_code"])
    nosetools.assert_in(err, real_doc["traceback"])

@mock.patch("pyutil.program.eventmetric.make_es_index")
def test_emit_exec_result_default_succ_return_code(mock_index):
    do = eventmetric.emit_exec_result(name="dummy")(lambda: 1)
    do()

    real_doc = mock_index.call_args[0][2]
    nosetools.eq_(0, real_doc["return_code"])

@mock.patch("pyutil.program.eventmetric.make_es_index")
def test_emit_exec_result_specify_succ_return_code(mock_index):
    do = eventmetric.emit_exec_result(name="dummy", succ_return_code=1)(lambda: 0)
    do()

    real_doc = mock_index.call_args[0][2]
    nosetools.eq_(1, real_doc["return_code"])

@nosetools.raises(TypeError)
def test_emit_exec_result_invalid_err_return_code():
    do = eventmetric.emit_exec_result(name="dummy", err_return_code="-1")(lambda: 0)
    do()

@nosetools.raises(TypeError)
def test_emit_exec_result_invalid_succ_return_code():
    do = eventmetric.emit_exec_result(name="dummy", succ_return_code="0")(lambda: 0)
    do()

@mock.patch("pyutil.program.eventmetric.make_es_index")
def test_emit_exec_result_default_name(mock_index):

    @eventmetric.emit_exec_result()
    def do():
        pass

    do()

    real_es_type = mock_index.call_args[0][1]
    nosetools.eq_("do", real_es_type)

@nosetools.raises(TypeError)
def test_emit_exec_result_invalid_name():
    do = eventmetric.emit_exec_result(name="dummy.a")(lambda: 0)
    do()

@nosetools.raises(TypeError)
def test_emit_exec_result_default_name_with_lambda():
    eventmetric.emit_exec_result()(lambda: 0)

@nosetools.raises(TypeError)
def test_emit_exec_result_invalid_extra_info():
    repo = "dummy-repo"
    operator = "dummy-operator"
    ext = object()
    do = eventmetric.emit_exec_result(name="dummy", extra_info=ext)(lambda: 0)
    do()    

@mock.patch("pyutil.program.eventmetric.make_es_index")
def test_emit_exec_result(mock_index):
    ext = "dummy-extra-info"
    do = eventmetric.emit_exec_result(name="dummy", extra_info=ext)(lambda: 0)
    do()

    real_doc = mock_index.call_args[0][2]
    nosetools.assert_is_instance(real_doc["begin"], int)
    nosetools.assert_is_instance(real_doc["end"], int)
    nosetools.assert_is_instance(real_doc["duration"], int)
    nosetools.eq_(0, real_doc["duration"])
    nosetools.eq_(socket.gethostname(), real_doc["host"])
    nosetools.eq_(ext, real_doc["extra_info"])

@mock.patch("pyutil.program.eventmetric.make_es_index")
def test_emit_operation_info(mock_index):
    ext = "dummy-extra-info"
    cmd_type = "systemctl"
    cmd = "systemctl --user restart dummy"
    operator = "dummy-operator"
    service = "dummy-service"
    eventmetric.collect_operation_info(cmd_type, cmd, operator, service=service,
                                       extra_info=ext)

    real_doc = mock_index.call_args[0][2]
    nosetools.eq_(cmd, real_doc["command"])
    nosetools.eq_(operator, real_doc["operator"])
    nosetools.ok_(isinstance(real_doc["timestamp"], int))
    nosetools.eq_(socket.gethostname(), real_doc["host"])
    nosetools.eq_(ext, real_doc["extra_info"])
    nosetools.eq_(service, real_doc["service"])

@nosetools.raises(ValueError)
def test_init_with_invalid_hosts():
    conf = mock.MagicMock()
    conf.get = mock.MagicMock(return_value="")
    eventmetric.init(conf)

def test_init_multiple_hosts():
    conf = mock.MagicMock()
    conf.get_values = mock.MagicMock(return_value=[
        "127.0.0.1:9200", "localhost", "dummy"
    ])
    eventmetric.init(conf)

    real_hosts = eventmetric.es_client.transport.hosts
    expect_hosts = [
        dict(host="127.0.0.1", port=9200),
        dict(host="localhost"),
        dict(host="dummy"),
    ]
    nosetools.assert_list_equal(expect_hosts, real_hosts)

@nosetools.raises(ZeroDivisionError)
def test_emit_release_info_should_raise_exception():
    repo = "dummy-repo"
    operator = "dummy-operator"
    do = eventmetric.emit_release_info(repo, operator)(lambda: 1/0)
    do()

@nosetools.raises(ValueError)
def test_emit_release_info_invalid_type():
    repo = "dummy-repo"
    operator = "dummy-operator"
    err_type = "invalid-type"
    do = eventmetric.emit_release_info(
        repo,
        operator,
        release_type=err_type
    )(lambda: 0)
    do()

@nosetools.raises(TypeError)
def test_emit_release_info_invalid_extra_info():
    repo = "dummy-repo"
    operator = "dummy-operator"
    ext = object()
    do = eventmetric.emit_release_info(repo, operator, extra_info=ext)(lambda: 0)
    do()    

@nosetools.raises(TypeError)
def test_emit_release_info_invalid_iterator_type():
    repo = "dummy-repo"
    operator = "dummy-operator"
    do = eventmetric.emit_release_info(repo, operator, reviewers="err")(lambda: 0)
    do()    

@mock.patch("pyutil.program.eventmetric.make_es_index")
def test_emit_release_info_default_owner(mock_index):
    repo = "dummy-repo"
    operator = "dummy-operator"
    do = eventmetric.emit_release_info(repo, operator)(lambda: 0)
    do()

    real_doc = mock_index.call_args[0][2]
    nosetools.eq_(operator, real_doc["repo_owners"])

@mock.patch("pyutil.program.eventmetric.make_es_index")
def test_emit_release_info(mock_index):
    repo = "dummy-repo"
    operator = "dummy-operator"
    commit = "feat: dummy-feature"
    extra_args = "dummy-arg1,dummy-arg2"
    files = "dummy.log,dummy.data"
    reviewers = "x,fili"
    services = "foo,bar"
    repo_owners = "dummy-repo-owner-one,dummy-repo-owner-two"
    ext = "dummy-extra-info"
    do = eventmetric.emit_release_info(
        repo,
        operator,
        commit_message=commit,
        extra_arguments=extra_args.split(","),
        changed_files=files.split(","),
        reviewers=reviewers.split(","),
        services=services.split(","),
        repo_owners=repo_owners.split(","),
        extra_info=ext
    )(lambda: 0)
    do()

    nosetools.eq_("release_info", mock_index.call_args[0][0])
    nosetools.eq_(repo, mock_index.call_args[0][1])

    real_doc = mock_index.call_args[0][2]
    nosetools.eq_(repo, real_doc.get("repo"))
    nosetools.eq_(operator, real_doc.get("operator"))
    nosetools.eq_("release", real_doc.get("release_type"))
    nosetools.eq_(commit, real_doc.get("commit_message"))
    nosetools.eq_(extra_args, real_doc.get("extra_arguments"))
    nosetools.eq_(files, real_doc.get("changed_files"))
    nosetools.eq_(reviewers, real_doc.get("reviewers"))
    nosetools.eq_(services, real_doc.get("services"))
    nosetools.assert_is_instance(real_doc["begin"], int)
    nosetools.assert_is_instance(real_doc["end"], int)
    nosetools.assert_is_instance(real_doc["duration"], int)
    nosetools.eq_(0, real_doc["duration"])
    nosetools.eq_(repo_owners, real_doc["repo_owners"])
    nosetools.eq_(ext, real_doc["extra_info"])

def demo():
    index = "exec_result"
    doc_type = "dummy-example"

    @eventmetric.emit_exec_result(name=doc_type)
    def example():
        time.sleep(1)
        return "ok"

    host = "10.2.210.121:9200"
    conf = mock.MagicMock()
    conf.get = mock.MagicMock(return_value=host)
    eventmetric.init(conf)

    before_time = int(time.time())
    example()

    # Wait for indexing was successful.
    time.sleep(1)

    res = eventmetric.es_client.search(
        index=index,
        doc_type=doc_type,
        body=dict(
            query=dict(term=dict(return_code=0)),
            sort=[dict(begin="desc")]
        ),
        params=dict(size=1),
    )
    nosetools.ok_(res)
    nosetools.assert_in("hits", res)
    nosetools.ok_(res["hits"])
    nosetools.assert_in("hits", res["hits"])
    nosetools.eq_(1, len(res["hits"]["hits"]))

    begin = res["hits"]["hits"][0]["_source"]["begin"]
    nosetools.assert_less_equal(before_time, begin)
