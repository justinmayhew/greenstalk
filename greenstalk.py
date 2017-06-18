import socket
from datetime import timedelta
from typing import BinaryIO, Dict, Iterable, List, Optional, Tuple, Union

__version__ = '0.4.0'

Body = Union[bytes, str]
Stats = Dict[str, Union[str, int]]

DEFAULT_TUBE = 'default'
DEFAULT_PRIORITY = 2**16
DEFAULT_DELAY = timedelta()
DEFAULT_TTR = timedelta(seconds=60)


class Job:
    __slots__ = ('id', 'body')

    def __init__(self, id: int, body: Body) -> None:
        self.id = id
        self.body = body


JobOrID = Union[Job, int]


class Error(Exception):
    pass


class BeanstalkdError(Error):
    """An error read from a beanstalkd response."""


class BadFormatError(BeanstalkdError):
    pass


class BuriedError(BeanstalkdError):

    def __init__(self, values: List[bytes] = None) -> None:
        if values:
            self.id = int(values[0])  # type: Optional[int]
        else:
            self.id = None


class DeadlineSoonError(BeanstalkdError):
    pass


class DrainingError(BeanstalkdError):
    pass


class ExpectedCrlfError(BeanstalkdError):
    pass


class InternalError(BeanstalkdError):
    pass


class JobTooBigError(BeanstalkdError):
    pass


class NotFoundError(BeanstalkdError):
    pass


class NotIgnoredError(BeanstalkdError):
    pass


class OutOfMemoryError(BeanstalkdError):
    pass


class TimedOutError(BeanstalkdError):
    pass


class UnknownCommandError(BeanstalkdError):
    pass


ERROR_RESPONSES = {
    b'BAD_FORMAT':      BadFormatError,
    b'BURIED':          BuriedError,
    b'DEADLINE_SOON':   DeadlineSoonError,
    b'DRAINING':        DrainingError,
    b'EXPECTED_CRLF':   ExpectedCrlfError,
    b'INTERNAL_ERROR':  InternalError,
    b'JOB_TOO_BIG':     JobTooBigError,
    b'NOT_FOUND':       NotFoundError,
    b'NOT_IGNORED':     NotIgnoredError,
    b'OUT_OF_MEMORY':   OutOfMemoryError,
    b'TIMED_OUT':       TimedOutError,
    b'UNKNOWN_COMMAND': UnknownCommandError,
}


class UnknownResponseError(Error):

    def __init__(self, status: bytes, values: List[bytes]) -> None:
        self.status = status
        self.values = values


class Client:
    """A client implementing the beanstalk protocol. Upon creation a TCP
    connection with beanstalkd is established and tubes are initialized.

    :param host: IP or hostname of the beanstalkd instance.
    :param port: Port of the beanstalkd instance.
    :param encoding: The encoding to encode and decode jobs with.
    :param use: Initialize the currently used tube.
    :param watch: Initialize the watch list.
    """

    def __init__(self,
                 host: str = '127.0.0.1',
                 port: int = 11300,
                 encoding: Optional[str] = 'utf-8',
                 use: str = DEFAULT_TUBE,
                 watch: Union[str, Iterable[str]] = DEFAULT_TUBE) -> None:
        self._sock = socket.create_connection((host, port))
        self._reader = self._sock.makefile('rb')  # type: BinaryIO
        self.encoding = encoding

        if use != DEFAULT_TUBE:
            self.use(use)

        if isinstance(watch, str):
            if watch != DEFAULT_TUBE:
                self.watch(watch)
                self.ignore(DEFAULT_TUBE)
        else:
            for tube in watch:
                self.watch(tube)
            if DEFAULT_TUBE not in watch:
                self.ignore(DEFAULT_TUBE)

    def close(self) -> None:
        """Closes the TCP connection to beanstalkd. The client instance should
        not be used after calling this method."""
        self._reader.close()
        self._sock.close()

    def _send_cmd(self, cmd: bytes, expected: bytes) -> List[bytes]:
        self._sock.sendall(cmd + b'\r\n')

        line = self._reader.readline()
        if not line:
            raise ConnectionError("Unexpected EOF")

        assert line[-2:] == b'\r\n'
        line = line[:-2]

        status, *values = line.split()

        if status == expected:
            return values

        if status in ERROR_RESPONSES:
            raise ERROR_RESPONSES[status](values)

        raise UnknownResponseError(status, values)

    def _read_data(self, size: int) -> bytes:
        data = self._reader.read(size + 2)

        assert data[-2:] == b'\r\n'
        data = data[:-2]

        if len(data) != size:
            raise ConnectionError("Unexpected EOF reading chunk")

        return data

    def _read_job(self, values: List[bytes]) -> Job:
        assert len(values) == 2
        body = self._read_data(int(values[1]))
        return Job(int(values[0]), self._decode_body(body))

    def _decode_body(self, body: bytes) -> Body:
        if self.encoding is not None:
            return body.decode(self.encoding)
        return body

    def put(self,
            body: Body,
            priority: int = DEFAULT_PRIORITY,
            delay: timedelta = DEFAULT_DELAY,
            ttr: timedelta = DEFAULT_TTR) -> int:
        """Inserts a job into the currently used tube and returns the job ID.

        :param body: Data representing the job.
        :param priority: An integer between 0 and 4,294,967,295 where 0 is the
                         most urgent.
        :param delay: Amount of time the job will remain in the delayed state
                      before moving to the ready state.
        :param ttr: Time to run: the maximum amount of time the job can be reserved
                    for before timing out.
        """
        if isinstance(body, str):
            if self.encoding is None:
                raise TypeError("Unable to encode string with no encoding set")
            body = body.encode(self.encoding)

        cmd = b'put %d %d %d %d\r\n%b' % (
            priority,
            delay.total_seconds(),
            ttr.total_seconds(),
            len(body),
            body,
        )
        values = self._send_cmd(cmd, b'INSERTED')
        return int(values[0])

    def use(self, tube: str) -> None:
        """Changes the currently used tube.

        :param tube: Name of the tube to use.
        """
        cmd = b'use %b' % tube.encode('ascii')
        self._send_cmd(cmd, b'USING')

    def reserve(self, timeout: timedelta = None) -> Job:
        """Reserves a job from a tube on the watch list, giving this client
        exclusive access to it for the TTR. Returns the reserved job.

        This blocks until a job is reserved unless a ``timeout`` is given,
        which will raise a :class:`TimedOutError <greenstalk.TimedOutError>` if
        a job cannot be reserved within that time.

        :param timeout: Maximum amount of time to wait.
        """
        if timeout is None:
            cmd = b'reserve'
        else:
            cmd = b'reserve-with-timeout %d' % timeout.total_seconds()
        values = self._send_cmd(cmd, b'RESERVED')
        return self._read_job(values)

    def delete(self, job: JobOrID) -> None:
        """Deletes a job.

        :param job: Job or the ID of the job to delete.
        """
        cmd = b'delete %d' % _to_id(job)
        self._send_cmd(cmd, b'DELETED')

    def release(self,
                job: Job,
                priority: int = DEFAULT_PRIORITY,
                delay: timedelta = DEFAULT_DELAY) -> None:
        """Releases a reserved job. This is typically done if the job could not
        be finished and a retry is desired.

        :param job: Job to release.
        :param priority: Priority of the job.
        :param delay: Amount of time the job will remain in the delayed state
                      before moving to the ready state.
        """
        cmd = b'release %d %d %d' % (job.id, priority, delay.total_seconds())
        self._send_cmd(cmd, b'RELEASED')

    def bury(self, job: Job, priority: int = DEFAULT_PRIORITY) -> None:
        """Buries a reserved job. This is typically done if the job could not be
        finished and a retry is **not** desired.

        :param job: Job to bury.
        :param priority: Priority of the job.
        """
        cmd = b'bury %d %d' % (job.id, priority)
        self._send_cmd(cmd, b'BURIED')

    def touch(self, job: Job) -> None:
        """Refreshes the TTR of a reserved job.

        :param job: Job to touch.
        """
        cmd = b'touch %d' % job.id
        self._send_cmd(cmd, b'TOUCHED')

    def watch(self, tube: str) -> int:
        """Adds a tube to the watch list. Returns the number of tubes this
        client is watching.

        :param tube: Name of the tube to watch.
        """
        cmd = b'watch %b' % tube.encode('ascii')
        values = self._send_cmd(cmd, b'WATCHING')
        return int(values[0])

    def ignore(self, tube: str) -> int:
        """Removes a tube from the watch list. Returns the number of tubes this
        client is watching.

        :param tube: Name of the tube to ignore.
        """
        cmd = b'ignore %b' % tube.encode('ascii')
        values = self._send_cmd(cmd, b'WATCHING')
        return int(values[0])

    def peek(self, id: int) -> Job:
        """Returns a job by ID.

        :param id: ID of the job to return.
        """
        # TODO: this should only return the body.
        cmd = b'peek %d' % id
        values = self._send_cmd(cmd, b'FOUND')
        return self._read_job(values)

    def peek_ready(self) -> Job:
        """Returns the next ready job in the currently used tube."""
        cmd = b'peek-ready'
        values = self._send_cmd(cmd, b'FOUND')
        return self._read_job(values)

    def peek_delayed(self) -> Job:
        """Returns the next available delayed job in the currently used tube."""
        cmd = b'peek-delayed'
        values = self._send_cmd(cmd, b'FOUND')
        return self._read_job(values)

    def peek_buried(self) -> Job:
        """Returns the oldest buried job in the currently used tube."""
        cmd = b'peek-buried'
        values = self._send_cmd(cmd, b'FOUND')
        return self._read_job(values)

    def kick(self, bound: int) -> int:
        """Moves delayed and buried jobs into the ready queue. Only jobs from
        the currently used tube are moved.

        A kick will only move jobs in a single state. If there are any buried
        jobs, only those will be moved. Otherwise delayed jobs will be moved.

        :param bound: Maximum number of jobs to move.
        """
        cmd = b'kick %d' % bound
        values = self._send_cmd(cmd, b'KICKED')
        return int(values[0])

    def kick_job(self, job: JobOrID) -> None:
        """Moves a delayed or buried job into the ready queue.

        :param job: Job or the ID of the job to move.
        """
        cmd = b'kick-job %d' % _to_id(job)
        self._send_cmd(cmd, b'KICKED')

    def stats_job(self, job: JobOrID) -> Stats:
        """Returns job statistics.

        :param job: Job or the ID of the job.
        """
        cmd = b'stats-job %d' % _to_id(job)
        values = self._send_cmd(cmd, b'OK')
        data = self._read_data(int(values[0]))
        return _parse_simple_yaml(data)

    def stats_tube(self, tube: str) -> Stats:
        """Returns tube statistics.

        :param tube: Name of the tube.
        """
        cmd = b'stats-tube %b' % tube.encode('ascii')
        values = self._send_cmd(cmd, b'OK')
        data = self._read_data(int(values[0]))
        return _parse_simple_yaml(data)

    def stats(self) -> Stats:
        """Returns system statistics."""
        cmd = b'stats'
        values = self._send_cmd(cmd, b'OK')
        data = self._read_data(int(values[0]))
        return _parse_simple_yaml(data)


def _to_id(j: JobOrID) -> int:
    return j.id if isinstance(j, Job) else j


def _parse_simple_yaml(buf: bytes) -> Stats:
    data = buf.decode('ascii')

    assert data[:4] == '---\n'
    data = data[4:]  # strip YAML head

    stats = {}
    for line in data.splitlines():
        key, value = line.split(': ', 1)  # type: Tuple[str, Union[str, int]]
        try:
            value = int(value)
        except ValueError:
            pass
        stats[key] = value

    return stats
