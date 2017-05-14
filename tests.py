import subprocess
import time
from datetime import datetime, timedelta

import pytest

from greenstalk import Client
from greenstalk.exceptions import (
    DeadlineSoonError,
    JobTooBigError,
    NotFoundError,
    NotIgnoredError,
    TimedOutError,
)

TEST_PORT = 4444


def with_beanstalkd(test):
    def wrapper():
        args = ('beanstalkd', '-l', '127.0.0.1', '-p', str(TEST_PORT))
        beanstalkd = subprocess.Popen(args)
        time.sleep(0.001)
        try:
            test(Client(port=TEST_PORT))
        finally:
            beanstalkd.terminate()
            beanstalkd.wait()
    return wrapper


@with_beanstalkd
def test_basic_usage(c):
    c.use(b'email')
    c.put(b'test@example.com')
    c.watch(b'email')
    c.ignore(b'default')
    jid, body = c.reserve()
    assert body == b'test@example.com'
    c.delete(jid)


@with_beanstalkd
def test_put_priority(c):
    c.put(b'2', priority=2)
    c.put(b'1', priority=1)
    _, body = c.reserve()
    assert body == b'1'
    _, body = c.reserve()
    assert body == b'2'


@with_beanstalkd
def test_delays(c):
    c.put(b'delayed', delay=timedelta(seconds=1))
    before = datetime.now()
    jid, body = c.reserve()
    assert body == b'delayed'
    assert datetime.now() - before >= timedelta(seconds=1)
    c.release(jid, delay=timedelta(seconds=2))
    with pytest.raises(TimedOutError):
        c.reserve(timeout=timedelta(seconds=1))
    jid, _ = c.reserve(timeout=timedelta(seconds=1))
    c.bury(jid)
    with pytest.raises(TimedOutError):
        c.reserve(timeout=timedelta(seconds=0))


@with_beanstalkd
def test_ttr(c):
    c.put(b'two second ttr', ttr=timedelta(seconds=2))
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


@with_beanstalkd
def test_reserve_throws_on_time_out(c):
    before = datetime.now()
    with pytest.raises(TimedOutError):
        c.reserve(timeout=timedelta(seconds=1))
    delta = datetime.now() - before
    assert delta >= timedelta(seconds=1)
    assert delta <= timedelta(seconds=1, milliseconds=50)


@with_beanstalkd
def test_max_job_size(c):
    with pytest.raises(JobTooBigError):
        c.put(bytes(2**16))


@with_beanstalkd
def test_job_not_found(c):
    with pytest.raises(NotFoundError):
        c.delete(87)


@with_beanstalkd
def test_not_ignored(c):
    with pytest.raises(NotIgnoredError):
        c.ignore(b'default')
