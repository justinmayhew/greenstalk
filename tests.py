# pyright: strict, reportPrivateUsage=false
import json
import os
import signal
import subprocess
import time
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Callable, Iterable, Iterator, Union

import pytest

from greenstalk import (
    DEFAULT_PRIORITY,
    DEFAULT_TTR,
    DEFAULT_TUBE,
    Address,
    BuriedError,
    BuriedWithJobIDError,
    Client,
    DeadlineSoonError,
    DrainingError,
    Job,
    JobTooBigError,
    NotFoundError,
    NotIgnoredError,
    TimedOutError,
    UnknownResponseError,
    _parse_chunk,
    _parse_response,
)

BEANSTALKD_PATH = os.getenv("BEANSTALKD_PATH", "beanstalkd")
DEFAULT_INET_ADDRESS = ("127.0.0.1", 4444)
DEFAULT_UNIX_ADDRESS = "/tmp/greenstalk-test.sock"

TestFunc = Callable[[Client], None]
WrapperFunc = Callable[[], None]
DecoratorFunc = Callable[[TestFunc], WrapperFunc]


def with_beanstalkd(
    address: Address = DEFAULT_INET_ADDRESS,
    use: str = DEFAULT_TUBE,
    watch: Union[str, Iterable[str]] = DEFAULT_TUBE,
) -> DecoratorFunc:
    def decorator(test: TestFunc) -> WrapperFunc:
        def wrapper() -> None:
            cmd = [BEANSTALKD_PATH]
            if isinstance(address, str):
                cmd.extend(["-l", "unix:" + address])
            else:
                host, port = address
                cmd.extend(["-l", host, "-p", str(port)])
            with subprocess.Popen(cmd) as beanstalkd:
                time.sleep(0.1)
                try:
                    with Client(address, use=use, watch=watch) as c:
                        test(c)
                finally:
                    beanstalkd.terminate()

        return wrapper

    return decorator


@contextmanager
def assert_seconds(n: int) -> Iterator[None]:
    start = datetime.now()
    yield
    duration = datetime.now() - start
    assert duration >= timedelta(seconds=n)
    assert duration <= timedelta(seconds=n, milliseconds=50)


@with_beanstalkd(DEFAULT_UNIX_ADDRESS)
def test_basic_usage(c: Client) -> None:
    c.use("emails")
    id = c.put("测试@example.com".encode("utf-8"))
    c.watch("emails")
    c.ignore("default")
    job = c.reserve()
    assert id == job.id
    assert job.body.decode("utf-8") == "测试@example.com"
    c.delete(job)


@with_beanstalkd(DEFAULT_UNIX_ADDRESS)
def test_put_priority(c: Client) -> None:
    c.put(b"2", priority=2)
    c.put(b"1", priority=1)
    job = c.reserve()
    assert job.body == b"1"
    job = c.reserve()
    assert job.body == b"2"


@with_beanstalkd(DEFAULT_UNIX_ADDRESS)
def test_delays(c: Client) -> None:
    with assert_seconds(1):
        c.put(b"delayed", delay=1)
        job = c.reserve()
        assert job.body == b"delayed"
    with assert_seconds(2):
        c.release(job, delay=2)
        with pytest.raises(TimedOutError):
            c.reserve(timeout=1)
        job = c.reserve(timeout=1)
    c.bury(job)
    with pytest.raises(TimedOutError):
        c.reserve(timeout=0)


@with_beanstalkd(DEFAULT_UNIX_ADDRESS)
def test_ttr(c: Client) -> None:
    c.put(b"two second ttr", ttr=2)
    with assert_seconds(1):
        job = c.reserve()
        with pytest.raises(DeadlineSoonError):
            c.reserve()
    with assert_seconds(1):
        c.touch(job)
        with pytest.raises(DeadlineSoonError):
            c.reserve()
    c.release(job)


@with_beanstalkd(DEFAULT_UNIX_ADDRESS)
def test_reserve_raises_on_timeout(c: Client) -> None:
    with assert_seconds(1):
        with pytest.raises(TimedOutError):
            c.reserve(timeout=1)


@with_beanstalkd()
def test_reserve_job(c: Client) -> None:
    id1 = c.put(b"a")
    id2 = c.put(b"b")
    j1 = c.reserve_job(id1)
    j2 = c.reserve_job(id2)
    with pytest.raises(NotFoundError):
        c.reserve_job(id1)
    with pytest.raises(NotFoundError):
        c.reserve_job(id2)
    with pytest.raises(TimedOutError):
        c.reserve(timeout=0)
    c.delete(j1)
    c.delete(j2)
    with pytest.raises(TimedOutError):
        c.reserve(timeout=0)


@with_beanstalkd(use="hosts", watch="hosts")
def test_initialize_with_tubes(c: Client) -> None:
    c.put(b"www.example.com")
    job = c.reserve()
    assert job.body == b"www.example.com"
    c.delete(job.id)
    c.use("default")
    c.put(b"")
    with pytest.raises(TimedOutError):
        c.reserve(timeout=0)


@with_beanstalkd(watch=["static", "dynamic"])
def test_initialize_watch_multiple(c: Client) -> None:
    c.use("static")
    c.put(b"haskell")
    c.put(b"rust")
    c.use("dynamic")
    c.put(b"python")
    job = c.reserve(timeout=0)
    assert job.body == b"haskell"
    job = c.reserve(timeout=0)
    assert job.body == b"rust"
    job = c.reserve(timeout=0)
    assert job.body == b"python"


@with_beanstalkd()
def test_peek(c: Client) -> None:
    id = c.put(b"job")
    job = c.peek(id)
    assert job.id == id
    assert job.body == b"job"


@with_beanstalkd()
def test_peek_not_found(c: Client) -> None:
    with pytest.raises(NotFoundError):
        c.peek(111)


@with_beanstalkd()
def test_peek_ready(c: Client) -> None:
    id = c.put(b"ready")
    job = c.peek_ready()
    assert job.id == id
    assert job.body == b"ready"


@with_beanstalkd()
def test_peek_ready_not_found(c: Client) -> None:
    c.put(b"delayed", delay=10)
    with pytest.raises(NotFoundError):
        c.peek_ready()


@with_beanstalkd()
def test_peek_delayed(c: Client) -> None:
    id = c.put(b"delayed", delay=10)
    job = c.peek_delayed()
    assert job.id == id
    assert job.body == b"delayed"


@with_beanstalkd()
def test_peek_delayed_not_found(c: Client) -> None:
    with pytest.raises(NotFoundError):
        c.peek_delayed()


@with_beanstalkd()
def test_peek_buried(c: Client) -> None:
    id = c.put(b"buried")
    job = c.reserve()
    c.bury(job)
    job = c.peek_buried()
    assert job.id == id
    assert job.body == b"buried"


@with_beanstalkd()
def test_peek_buried_not_found(c: Client) -> None:
    c.put(b"a ready job")
    with pytest.raises(NotFoundError):
        c.peek_buried()


@with_beanstalkd()
def test_kick(c: Client) -> None:
    c.put(b"a delayed job", delay=30)
    c.put(b"another delayed job", delay=45)
    c.put(b"a ready job")
    job = c.reserve()
    c.bury(job)
    assert c.kick(10) == 1
    assert c.kick(10) == 2


@with_beanstalkd()
def test_kick_job(c: Client) -> None:
    id = c.put(b"a delayed job", delay=3600)
    c.kick_job(id)
    c.reserve(timeout=0)


@with_beanstalkd()
def test_stats_job(c: Client) -> None:
    assert c.stats_job(c.put(b"job")) == {
        "id": 1,
        "tube": "default",
        "state": "ready",
        "pri": DEFAULT_PRIORITY,
        "age": 0,
        "delay": 0,
        "ttr": DEFAULT_TTR,
        "time-left": 0,
        "file": 0,
        "reserves": 0,
        "timeouts": 0,
        "releases": 0,
        "buries": 0,
        "kicks": 0,
    }


@with_beanstalkd(use="foo")
def test_stats_tube(c: Client) -> None:
    assert c.stats_tube("default") == {
        "name": "default",
        "current-jobs-urgent": 0,
        "current-jobs-ready": 0,
        "current-jobs-reserved": 0,
        "current-jobs-delayed": 0,
        "current-jobs-buried": 0,
        "total-jobs": 0,
        "current-using": 0,
        "current-watching": 1,
        "current-waiting": 0,
        "cmd-delete": 0,
        "cmd-pause-tube": 0,
        "pause": 0,
        "pause-time-left": 0,
    }


@with_beanstalkd()
def test_stats(c: Client) -> None:
    s = c.stats()
    assert s["current-jobs-urgent"] == 0
    assert s["current-jobs-ready"] == 0
    assert s["current-jobs-reserved"] == 0
    assert s["current-jobs-delayed"] == 0
    assert s["current-jobs-buried"] == 0
    assert s["cmd-put"] == 0
    assert s["cmd-peek"] == 0
    assert s["cmd-peek-ready"] == 0
    assert s["cmd-peek-delayed"] == 0
    assert s["cmd-peek-buried"] == 0
    assert s["cmd-reserve"] == 0
    assert s["cmd-reserve-with-timeout"] == 0
    assert s["cmd-delete"] == 0
    assert s["cmd-release"] == 0
    assert s["cmd-use"] == 0
    assert s["cmd-watch"] == 0
    assert s["cmd-ignore"] == 0
    assert s["cmd-bury"] == 0
    assert s["cmd-kick"] == 0
    assert s["cmd-touch"] == 0
    assert s["cmd-stats"] == 1
    assert s["cmd-stats-job"] == 0
    assert s["cmd-stats-tube"] == 0
    assert s["cmd-list-tubes"] == 0
    assert s["cmd-list-tube-used"] == 0
    assert s["cmd-list-tubes-watched"] == 0
    assert s["cmd-pause-tube"] == 0
    assert s["job-timeouts"] == 0
    assert s["total-jobs"] == 0
    assert "max-job-size" in s
    assert s["current-tubes"] == 1
    assert s["current-connections"] == 1
    assert s["current-producers"] == 0
    assert s["current-workers"] == 0
    assert s["current-waiting"] == 0
    assert s["total-connections"] == 1
    assert "pid" in s
    assert "version" in s
    assert "rusage-utime" in s
    assert "rusage-stime" in s
    assert s["uptime"] == 0
    assert s["binlog-oldest-index"] == 0
    assert s["binlog-current-index"] == 0
    assert s["binlog-records-migrated"] == 0
    assert s["binlog-records-written"] == 0
    assert "binlog-max-size" in s
    assert "id" in s
    assert "hostname" in s
    assert s["draining"] == "false"


@with_beanstalkd()
def test_tubes(c: Client) -> None:
    assert c.tubes() == ["default"]
    c.use("a")
    assert set(c.tubes()) == {"default", "a"}
    c.watch("b")
    c.watch("c")
    assert set(c.tubes()) == {"default", "a", "b", "c"}


@with_beanstalkd()
def test_using(c: Client) -> None:
    assert c.using() == "default"
    c.use("another")
    assert c.using() == "another"


@with_beanstalkd()
def test_watching(c: Client) -> None:
    assert c.watching() == ["default"]
    c.watch("another")
    assert set(c.watching()) == {"default", "another"}


@with_beanstalkd()
def test_pause_tube(c: Client) -> None:
    c.put(b"")
    with assert_seconds(1):
        c.pause_tube("default", 1)
        c.reserve()


@with_beanstalkd(use="default")
def test_max_job_size(c: Client) -> None:
    with pytest.raises(JobTooBigError):
        c.put(bytes(2 ** 16))


@with_beanstalkd()
def test_job_not_found(c: Client) -> None:
    with pytest.raises(NotFoundError):
        c.delete(87)


@with_beanstalkd()
def test_delete_job_reserved_by_other(c: Client) -> None:
    c.put(b"", ttr=1)
    with Client(DEFAULT_INET_ADDRESS) as other:
        job = other.reserve()
        with pytest.raises(NotFoundError):
            c.delete(job)


@with_beanstalkd()
def test_not_ignored(c: Client) -> None:
    with pytest.raises(NotIgnoredError):
        c.ignore("default")


def test_drain_mode() -> None:
    cmd = [BEANSTALKD_PATH, "-l", "unix:" + DEFAULT_UNIX_ADDRESS]
    with subprocess.Popen(cmd) as beanstalkd:
        time.sleep(0.1)
        try:
            with Client(address=DEFAULT_UNIX_ADDRESS) as c:
                assert c.put(b"first") == 1
                beanstalkd.send_signal(signal.SIGUSR1)
                time.sleep(0.1)
                with pytest.raises(DrainingError):
                    c.put(b"second")
                assert c.stats()["draining"] == "true"
        finally:
            beanstalkd.terminate()


@with_beanstalkd(DEFAULT_INET_ADDRESS)
def test_client_repr_inet(c: Client) -> None:
    host, port = DEFAULT_INET_ADDRESS
    assert repr(c) == f"greenstalk.Client(host='{host}', port={port})"


@with_beanstalkd(DEFAULT_UNIX_ADDRESS)
def test_client_repr_unix(c: Client) -> None:
    assert repr(c) == f"greenstalk.Client(socket='{DEFAULT_UNIX_ADDRESS}')"


def test_job_repr() -> None:
    body = json.dumps({"user_id": 123}).encode("utf-8")
    job = Job(id=456, body=body)
    assert repr(job) == """greenstalk.Job(id=456, body=b'{"user_id": 123}')"""


def test_buried_error_with_id() -> None:
    with pytest.raises(BuriedWithJobIDError) as e:
        _parse_response(b"BURIED 10\r\n", b"")
    assert e.value.job_id == 10


def test_buried_error_without_id() -> None:
    with pytest.raises(BuriedError):
        _parse_response(b"BURIED\r\n", b"")


def test_unknown_response_error() -> None:
    with pytest.raises(UnknownResponseError) as e:
        _parse_response(b"FOO 1 2 3\r\n", b"")
    assert e.value.status == b"FOO"
    assert e.value.values == [b"1", b"2", b"3"]


def test_chunk_unexpected_eof() -> None:
    with pytest.raises(ConnectionError) as e:
        _parse_chunk(b"ABC\r\n", 4)
    assert e.value.args[0] == "Unexpected EOF reading chunk"


def test_response_missing_crlf() -> None:
    with pytest.raises(AssertionError):
        _parse_response(b"USING a", b"")


def test_unexpected_eof() -> None:
    with pytest.raises(ConnectionError) as e:
        _parse_response(b"", b"")
    assert e.value.args[0] == "Unexpected EOF"
