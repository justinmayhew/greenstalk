import subprocess
import time
from contextlib import closing
from datetime import datetime, timedelta
from typing import Any, Callable

import pytest

from greenstalk import (
    BuriedError, Client, DeadlineSoonError, JobTooBigError, NotFoundError,
    NotIgnoredError, TimedOutError, UnknownResponseError
)
from greenstalk.client import DEFAULT_PRIORITY, DEFAULT_TTR

TEST_PORT = 4444


def with_subprocess(*cli: Any) -> Callable:
    def configuration(**kwargs: Any) -> Callable:
        def decorator(test: Callable) -> Callable:
            def wrapper() -> None:
                cmdline = list(cli)
                try:
                    response = kwargs.pop('response')
                except KeyError:
                    pass
                else:
                    cmdline.append(response)
                process = subprocess.Popen(cmdline)
                time.sleep(0.1)
                try:
                    with closing(Client(port=TEST_PORT, **kwargs)) as c:
                        test(c)
                finally:
                    process.terminate()
                    process.wait()
            return wrapper
        return decorator
    return configuration


with_beanstalkd = with_subprocess('beanstalkd', '-l', '127.0.0.1', '-p', str(TEST_PORT))
with_fake = with_subprocess('scripts/fake-response', str(TEST_PORT))


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


@with_beanstalkd()
def test_peek(c: Client) -> None:
    put_jid = c.put('job')
    peek_jid, body = c.peek(put_jid)
    assert put_jid == peek_jid
    assert body == 'job'


@with_beanstalkd()
def test_peek_not_found(c: Client) -> None:
    with pytest.raises(NotFoundError):
        c.peek(111)


@with_beanstalkd()
def test_peek_ready(c: Client) -> None:
    put_jid = c.put('ready')
    peek_jid, body = c.peek_ready()
    assert put_jid == peek_jid
    assert body == 'ready'


@with_beanstalkd()
def test_peek_ready_not_found(c: Client) -> None:
    c.put('delayed', delay=timedelta(seconds=10))
    with pytest.raises(NotFoundError):
        c.peek_ready()


@with_beanstalkd()
def test_peek_delayed(c: Client) -> None:
    put_jid = c.put('delayed', delay=timedelta(seconds=10))
    peek_jid, body = c.peek_delayed()
    assert put_jid == peek_jid
    assert body == 'delayed'


@with_beanstalkd()
def test_peek_delayed_not_found(c: Client) -> None:
    with pytest.raises(NotFoundError):
        c.peek_delayed()


@with_beanstalkd()
def test_peek_buried(c: Client) -> None:
    put_jid = c.put('buried')
    jid, _ = c.reserve()
    c.bury(jid)
    peek_jid, body = c.peek_buried()
    assert put_jid == peek_jid
    assert body == 'buried'


@with_beanstalkd()
def test_peek_buried_not_found(c: Client) -> None:
    c.put('a ready job')
    with pytest.raises(NotFoundError):
        c.peek_buried()


@with_beanstalkd()
def test_kick(c: Client) -> None:
    c.put('a delayed job', delay=timedelta(hours=1))
    c.put('another delayed job', delay=timedelta(hours=1))
    c.put('a ready job')
    jid, _ = c.reserve()
    c.bury(jid)
    assert c.kick(10) == 1
    assert c.kick(10) == 2


@with_beanstalkd()
def test_kick_job(c: Client) -> None:
    jid = c.put('a delayed job', delay=timedelta(hours=1))
    c.kick_job(jid)
    c.reserve(timeout=timedelta())


@with_beanstalkd()
def test_stats_job(c: Client) -> None:
    jid = c.put('job')
    assert c.stats_job(jid) == {
        'id': 1,
        'tube': 'default',
        'state': 'ready',
        'pri': DEFAULT_PRIORITY,
        'age': 0,
        'delay': 0,
        'ttr': DEFAULT_TTR.total_seconds(),
        'time-left': 0,
        'file': 0,
        'reserves': 0,
        'timeouts': 0,
        'releases': 0,
        'buries': 0,
        'kicks': 0,
    }


@with_beanstalkd(use='foo')
def test_stats_tube(c: Client) -> None:
    assert c.stats_tube('default') == {
        'name': 'default',
        'current-jobs-urgent': 0,
        'current-jobs-ready': 0,
        'current-jobs-reserved': 0,
        'current-jobs-delayed': 0,
        'current-jobs-buried': 0,
        'total-jobs': 0,
        'current-using': 0,
        'current-watching': 1,
        'current-waiting': 0,
        'cmd-delete': 0,
        'cmd-pause-tube': 0,
        'pause': 0,
        'pause-time-left': 0,
    }


@with_beanstalkd()
def test_stats(c: Client) -> None:
    s = c.stats()
    assert s['current-jobs-urgent'] == 0
    assert s['current-jobs-ready'] == 0
    assert s['current-jobs-reserved'] == 0
    assert s['current-jobs-delayed'] == 0
    assert s['current-jobs-buried'] == 0
    assert s['cmd-put'] == 0
    assert s['cmd-peek'] == 0
    assert s['cmd-peek-ready'] == 0
    assert s['cmd-peek-delayed'] == 0
    assert s['cmd-peek-buried'] == 0
    assert s['cmd-reserve'] == 0
    assert s['cmd-reserve-with-timeout'] == 0
    assert s['cmd-delete'] == 0
    assert s['cmd-release'] == 0
    assert s['cmd-use'] == 0
    assert s['cmd-watch'] == 0
    assert s['cmd-ignore'] == 0
    assert s['cmd-bury'] == 0
    assert s['cmd-kick'] == 0
    assert s['cmd-touch'] == 0
    assert s['cmd-stats'] == 1
    assert s['cmd-stats-job'] == 0
    assert s['cmd-stats-tube'] == 0
    assert s['cmd-list-tubes'] == 0
    assert s['cmd-list-tube-used'] == 0
    assert s['cmd-list-tubes-watched'] == 0
    assert s['cmd-pause-tube'] == 0
    assert s['job-timeouts'] == 0
    assert s['total-jobs'] == 0
    assert 'max-job-size' in s
    assert s['current-tubes'] == 1
    assert s['current-connections'] == 1
    assert s['current-producers'] == 0
    assert s['current-workers'] == 0
    assert s['current-waiting'] == 0
    assert s['total-connections'] == 1
    assert 'pid' in s
    assert 'version' in s
    assert 'rusage-utime' in s
    assert 'rusage-stime' in s
    assert s['uptime'] == 0
    assert s['binlog-oldest-index'] == 0
    assert s['binlog-current-index'] == 0
    assert s['binlog-records-migrated'] == 0
    assert s['binlog-records-written'] == 0
    assert 'binlog-max-size' in s
    assert 'id' in s
    assert 'hostname' in s


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


@with_fake(response='BURIED 10\r\n')
def test_put_buried_error(c: Client) -> None:
    with pytest.raises(BuriedError) as e:
        c.put('hello')
    assert e.value.jid == 10


@with_fake(response='BURIED\r\n')
def test_release_buried_error(c: Client) -> None:
    with pytest.raises(BuriedError) as e:
        c.release(1)
    assert e.value.jid is None


@with_fake(response='FOO 1 2 3\r\n')
def test_unknown_response_error(c: Client) -> None:
    with pytest.raises(UnknownResponseError) as e:
        c.reserve()
    assert e.value.status == b'FOO'
    assert e.value.values == [b'1', b'2', b'3']


@with_fake(response='RESERVED 1 4\r\nABC\r\n')
def test_eof_error(c: Client) -> None:
    with pytest.raises(ConnectionError) as e:
        c.reserve()
    assert e.value.args[0] == "Unexpected EOF reading chunk"


@with_fake(response='USING a')
def test_response_missing_crlf(c: Client) -> None:
    with pytest.raises(AssertionError):
        c.use('a')


@with_fake(response='')
def test_unexpected_eof(c: Client) -> None:
    with pytest.raises(ConnectionError) as e:
        c.reserve()
    assert e.value.args[0] == "Unexpected EOF"
