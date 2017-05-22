import subprocess
import time
from contextlib import closing
from datetime import datetime, timedelta
from typing import Any, Callable

import pytest

from greenstalk import Client
from greenstalk.exceptions import (
    DeadlineSoonError, JobTooBigError, NotFoundError, NotIgnoredError,
    TimedOutError
)

TEST_PORT = 4444


def with_beanstalkd(**kwargs: Any) -> Callable:
    def decorator(test: Callable) -> Callable:
        def wrapper() -> None:
            args = ('beanstalkd', '-l', '127.0.0.1', '-p', str(TEST_PORT))
            beanstalkd = subprocess.Popen(args)
            time.sleep(0.01)
            try:
                with closing(Client(port=TEST_PORT, **kwargs)) as c:
                    test(c)
            finally:
                beanstalkd.terminate()
                beanstalkd.wait()
        return wrapper
    return decorator


@with_beanstalkd()
def test_basic_usage(c: Client) -> None:
    c.use('emails')
    put_jid = c.put('测试@example.com')
    c.watch('emails')
    c.ignore('default')
    reserve_jid, body = c.reserve()
    assert put_jid == reserve_jid
    assert body == '测试@example.com'
    c.delete(reserve_jid)


@with_beanstalkd()
def test_put_priority(c: Client) -> None:
    c.put('2', priority=2)
    c.put('1', priority=1)
    _, body = c.reserve()
    assert body == '1'
    _, body = c.reserve()
    assert body == '2'


@with_beanstalkd()
def test_delays(c: Client) -> None:
    c.put('delayed', delay=timedelta(seconds=1))
    before = datetime.now()
    jid, body = c.reserve()
    assert body == 'delayed'
    assert datetime.now() - before >= timedelta(seconds=1)
    c.release(jid, delay=timedelta(seconds=2))
    with pytest.raises(TimedOutError):
        c.reserve(timeout=timedelta(seconds=1))
    jid, _ = c.reserve(timeout=timedelta(seconds=1))
    c.bury(jid)
    with pytest.raises(TimedOutError):
        c.reserve(timeout=timedelta(seconds=0))


@with_beanstalkd()
def test_ttr(c: Client) -> None:
    c.put('two second ttr', ttr=timedelta(seconds=2))
    before = datetime.now()
    jid, _ = c.reserve()
    with pytest.raises(DeadlineSoonError):
        c.reserve()
    c.touch(jid)
    with pytest.raises(DeadlineSoonError):
        c.reserve()
    c.release(jid)
    delta = datetime.now() - before
    assert delta >= timedelta(seconds=1, milliseconds=950)
    assert delta <= timedelta(seconds=2, milliseconds=50)


@with_beanstalkd()
def test_reserve_raises_on_timeout(c: Client) -> None:
    before = datetime.now()
    with pytest.raises(TimedOutError):
        c.reserve(timeout=timedelta(seconds=1))
    delta = datetime.now() - before
    assert delta >= timedelta(seconds=1)
    assert delta <= timedelta(seconds=1, milliseconds=50)


@with_beanstalkd(use='hosts', watch='hosts')
def test_initialize_with_tubes(c: Client) -> None:
    c.put('www.example.com')
    jid, body = c.reserve()
    assert body == 'www.example.com'
    c.delete(jid)
    c.use('default')
    c.put('')
    with pytest.raises(TimedOutError):
        c.reserve(timeout=timedelta())


@with_beanstalkd(watch=['static', 'dynamic'])
def test_initialize_watch_multiple(c: Client) -> None:
    c.use('static')
    c.put(b'haskell')
    c.put(b'rust')
    c.use('dynamic')
    c.put(b'python')
    jid, body = c.reserve(timeout=timedelta())
    assert body == 'haskell'
    jid, body = c.reserve(timeout=timedelta())
    assert body == 'rust'
    jid, body = c.reserve(timeout=timedelta())
    assert body == 'python'


@with_beanstalkd(encoding=None)
def test_binary_jobs(c: Client) -> None:
    with open('python-logo.png', 'rb') as f:
        image = f.read()
    c.put(image)
    jid, body = c.reserve()
    assert body == image


@with_beanstalkd(use='default')
def test_max_job_size(c: Client) -> None:
    with pytest.raises(JobTooBigError):
        c.put(bytes(2**16))


@with_beanstalkd()
def test_job_not_found(c: Client) -> None:
    with pytest.raises(NotFoundError):
        c.delete(87)


@with_beanstalkd()
def test_not_ignored(c: Client) -> None:
    with pytest.raises(NotIgnoredError):
        c.ignore('default')


@with_beanstalkd(encoding=None)
def test_str_body_no_encoding(c: Client) -> None:
    with pytest.raises(TypeError):
        c.put('a str job')
