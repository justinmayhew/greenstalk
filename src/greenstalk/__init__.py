import socket
from typing import (
    Any,
    Generic,
    Iterable,
    List,
    Literal,
    Optional,
    Tuple,
    TypedDict,
    TypeVar,
    Union,
    cast,
    overload,
)

__version__ = "2.1.1"

Address = Union[Tuple[str, int], str]
ConnectionTarget = Union[Address, socket.socket]

Stats = TypedDict(
    "Stats",
    {
        "current-jobs-urgent": int,
        "current-jobs-ready": int,
        "current-jobs-reserved": int,
        "current-jobs-delayed": int,
        "current-jobs-buried": int,
        "cmd-put": int,
        "cmd-peek": int,
        "cmd-peek-ready": int,
        "cmd-peek-delayed": int,
        "cmd-peek-buried": int,
        "cmd-reserve": int,
        "cmd-reserve-with-timeout": int,
        "cmd-touch": int,
        "cmd-use": int,
        "cmd-watch": int,
        "cmd-ignore": int,
        "cmd-delete": int,
        "cmd-release": int,
        "cmd-bury": int,
        "cmd-kick": int,
        "cmd-stats": int,
        "cmd-stats-job": int,
        "cmd-stats-tube": int,
        "cmd-list-tubes": int,
        "cmd-list-tube-used": int,
        "cmd-list-tubes-watched": int,
        "cmd-pause-tube": int,
        "job-timeouts": int,
        "total-jobs": int,
        "max-job-size": int,
        "current-tubes": int,
        "current-connections": int,
        "current-producers": int,
        "current-workers": int,
        "current-waiting": int,
        "total-connections": int,
        "pid": int,
        "version": str,
        "rusage-utime": str,
        "rusage-stime": str,
        "uptime": int,
        "binlog-oldest-index": int,
        "binlog-current-index": int,
        "binlog-max-size": int,
        "binlog-records-written": int,
        "binlog-records-migrated": int,
        "draining": Literal["true", "false"],
        "id": str,
        "hostname": str,
        "os": str,
        "platform": str,
    },
)

StatsJob = TypedDict(
    "StatsJob",
    {
        "id": int,
        "tube": str,
        "state": Literal["ready", "delayed", "reserved", "buried"],
        "pri": int,
        "age": int,
        "delay": int,
        "ttr": int,
        "time-left": int,
        "file": int,
        "reserves": int,
        "timeouts": int,
        "releases": int,
        "buries": int,
        "kicks": int,
    },
)

StatsTube = TypedDict(
    "StatsTube",
    {
        "name": str,
        "current-jobs-urgent": int,
        "current-jobs-ready": int,
        "current-jobs-reserved": int,
        "current-jobs-delayed": int,
        "current-jobs-buried": int,
        "total-jobs": int,
        "current-using": int,
        "current-waiting": int,
        "current-watching": int,
        "pause": int,
        "cmd-delete": int,
        "cmd-pause-tube": int,
        "pause-time-left": int,
    },
)

DEFAULT_TUBE = "default"
DEFAULT_PRIORITY = 2**16
DEFAULT_DELAY = 0
DEFAULT_TTR = 60

TBody = TypeVar("TBody", str, bytes)


class Job(Generic[TBody]):
    """A job returned from the server."""

    def __init__(self, id: int, body: TBody) -> None:
        #: A server-generated unique identifier assigned to the job on creation.
        self.id: int = id

        #: The content of the job. Also referred to as the message or payload.
        #: Producers and consumers need to agree on how these bytes are interpreted.
        self.body: TBody = body

    def __repr__(self) -> str:
        return f"greenstalk.Job(id={self.id!r}, body={self.body!r})"


JobOrID = Union[Job[TBody], int]


class Error(Exception):
    """Base class for non-connection related exceptions. Connection related
    issues use the built-in ``ConnectionError``.
    """


class UnknownResponseError(Error):
    """The server sent a response that this client does not understand."""

    def __init__(self, status: bytes, values: List[bytes]) -> None:
        #: The status code of the response.
        #: Contains ``b'SOME_ERROR'`` for the response ``b'SOME_ERROR 1 2 3\r\n'``.
        self.status: bytes = status

        #: The remaining split values after the status code.
        #: Contains ``[b'1', b'2', b'3']`` for the response ``b'SOME_ERROR 1 2 3\r\n'``.
        self.values: List[bytes] = values


class BeanstalkdError(Error):
    """Base class for error messages returned from the server."""


class BadFormatError(BeanstalkdError):
    """The client sent a malformed command."""


class BuriedError(BeanstalkdError):
    """The server ran out of memory trying to grow the priority queue and had to
    bury the job.

    This can be raised in response to a put or release command.
    """

    def __init__(self, values: Optional[List[bytes]] = None) -> None:
        if values:
            #: A server-generated unique identifier that was assigned to the buried job.
            self.id: Optional[int] = int(values[0])
        else:
            self.id = None


class DeadlineSoonError(BeanstalkdError):
    """The client has a reserved job timing out within the next second."""


class DrainingError(BeanstalkdError):
    """The client tried to insert a job while the server was in drain mode."""


class ExpectedCrlfError(BeanstalkdError):
    """The client sent a job body without a trailing CRLF."""


class InternalError(BeanstalkdError):
    """The server detected an internal error."""


class JobTooBigError(BeanstalkdError):
    """The client attempted to insert a job larger than ``max-job-size``."""


class NotFoundError(BeanstalkdError):
    """For the delete, release, bury, and kick commands, it means that the job
    does not exist or is not reserved by the client.

    For the peek commands, it means the requested job does not exist or that
    there are no jobs in the requested state.
    """


class NotIgnoredError(BeanstalkdError):
    """The client attempted to ignore the only tube on its watch list."""


class OutOfMemoryError(BeanstalkdError):
    """The server could not allocate enough memory for a job."""


class TimedOutError(BeanstalkdError):
    """A job could not be reserved within the specified timeout."""


class UnknownCommandError(BeanstalkdError):
    """The client sent a command that the server does not understand."""


ERROR_RESPONSES = {
    b"BAD_FORMAT": BadFormatError,
    b"BURIED": BuriedError,
    b"DEADLINE_SOON": DeadlineSoonError,
    b"DRAINING": DrainingError,
    b"EXPECTED_CRLF": ExpectedCrlfError,
    b"INTERNAL_ERROR": InternalError,
    b"JOB_TOO_BIG": JobTooBigError,
    b"NOT_FOUND": NotFoundError,
    b"NOT_IGNORED": NotIgnoredError,
    b"OUT_OF_MEMORY": OutOfMemoryError,
    b"TIMED_OUT": TimedOutError,
    b"UNKNOWN_COMMAND": UnknownCommandError,
}


class Client(Generic[TBody]):
    """A client implementing the beanstalk protocol. Upon creation a connection
    with beanstalkd is established and tubes are initialized.

    :param address: A socket address pair (host, port), a Unix domain socket path,
                    or a socket that is already connected to a beanstalkd server.
    :param encoding: The encoding used to encode and decode job bodies.
    :param use: The tube to use after connecting.
    :param watch: The tubes to watch after connecting. The ``default`` tube will
                  be ignored if it's not included.
    """

    @overload
    def __init__(
        self: "Client[bytes]",
        address: ConnectionTarget,
        encoding: None,
        use: str = DEFAULT_TUBE,
        watch: Union[str, Iterable[str]] = DEFAULT_TUBE,
    ) -> None: ...

    @overload
    def __init__(
        self: "Client[str]",
        address: ConnectionTarget,
        encoding: str = "utf-8",
        use: str = DEFAULT_TUBE,
        watch: Union[str, Iterable[str]] = DEFAULT_TUBE,
    ) -> None: ...

    def __init__(
        self,
        address: ConnectionTarget,
        encoding: Optional[str] = "utf-8",
        use: str = DEFAULT_TUBE,
        watch: Union[str, Iterable[str]] = DEFAULT_TUBE,
    ) -> None:
        if isinstance(address, socket.socket):
            self._sock = address
            self._address = self._sock.getpeername()
        elif isinstance(address, str):
            self._sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            self._sock.connect(address)
            self._address = address
        else:
            self._sock = socket.create_connection(address)
            self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            self._address = address

        self._reader = self._sock.makefile("rb")
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

    def __enter__(self) -> "Client[TBody]":
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    def close(self) -> None:
        """Closes the connection to beanstalkd. The client instance should not
        be used after calling this method."""
        self._reader.close()
        self._sock.close()

    def _send_cmd(self, cmd: bytes, expected: bytes) -> List[bytes]:
        self._sock.sendall(cmd + b"\r\n")
        line = self._reader.readline()
        return _parse_response(line, expected)

    def _read_chunk(self, size: int) -> bytes:
        data = self._reader.read(size + 2)
        return _parse_chunk(data, size)

    def _int_cmd(self, cmd: bytes, expected: bytes) -> int:
        (n,) = self._send_cmd(cmd, expected)
        return int(n)

    def _job_cmd(self, cmd: bytes, expected: bytes) -> Job[TBody]:
        id, size = (int(n) for n in self._send_cmd(cmd, expected))
        chunk = self._read_chunk(size)
        if self.encoding is None:
            body: bytes = chunk
        else:
            body = chunk.decode(self.encoding)  # type: ignore
        return cast(Job[TBody], Job(id, body))

    def _peek_cmd(self, cmd: bytes) -> Job[TBody]:
        return self._job_cmd(cmd, b"FOUND")

    def _stats_cmd(self, cmd: bytes) -> Union[Stats, StatsJob, StatsTube]:
        size = self._int_cmd(cmd, b"OK")
        chunk = self._read_chunk(size)
        return _parse_stats(chunk)

    def _list_cmd(self, cmd: bytes) -> List[str]:
        size = self._int_cmd(cmd, b"OK")
        chunk = self._read_chunk(size)
        return _parse_list(chunk)

    def put(
        self,
        body: TBody,
        priority: int = DEFAULT_PRIORITY,
        delay: int = DEFAULT_DELAY,
        ttr: int = DEFAULT_TTR,
    ) -> int:
        """Inserts a job into the currently used tube and returns the job ID.

        :param body: The data representing the job.
        :param priority: An integer between 0 and 4,294,967,295 where 0 is the
                         most urgent.
        :param delay: The number of seconds to delay the job for.
        :param ttr: The maximum number of seconds the job can be reserved for
                    before timing out.
        """
        if isinstance(body, str):
            if self.encoding is None:
                raise TypeError("Unable to encode string with no encoding set")
            buf = body.encode(self.encoding)
        else:
            buf = body
        cmd = b"put %d %d %d %d\r\n%b" % (priority, delay, ttr, len(buf), buf)
        return self._int_cmd(cmd, b"INSERTED")

    def use(self, tube: str) -> None:
        """Changes the currently used tube.

        :param tube: The tube to use.
        """
        self._send_cmd(b"use %b" % tube.encode("ascii"), b"USING")

    def reserve(self, timeout: Optional[int] = None) -> Job[TBody]:
        """Reserves a job from a tube on the watch list, giving this client
        exclusive access to it for the TTR. Returns the reserved job.

        This blocks until a job is reserved unless a ``timeout`` is given,
        which will raise a :class:`TimedOutError <greenstalk.TimedOutError>` if
        a job cannot be reserved within that time.

        :param timeout: The maximum number of seconds to wait.
        """
        if timeout is None:
            cmd = b"reserve"
        else:
            cmd = b"reserve-with-timeout %d" % timeout
        return self._job_cmd(cmd, b"RESERVED")

    def reserve_job(self, id: int) -> Job[TBody]:
        """Reserves a job by ID, giving this client exclusive access to it for
        the TTR. Returns the reserved job.

        A :class:`NotFoundError <greenstalk.NotFoundError>` is raised if a job
        with the specified ID could not be reserved.

        :param id: The ID of the job to reserve.
        """
        return self._job_cmd(b"reserve-job %d" % id, b"RESERVED")

    def delete(self, job: JobOrID[TBody]) -> None:
        """Deletes a job.

        :param job: The job or job ID to delete.
        """
        self._send_cmd(b"delete %d" % _to_id(job), b"DELETED")

    def release(
        self,
        job: Job[TBody],
        priority: int = DEFAULT_PRIORITY,
        delay: int = DEFAULT_DELAY,
    ) -> None:
        """Releases a reserved job.

        :param job: The job to release.
        :param priority: An integer between 0 and 4,294,967,295 where 0 is the
                         most urgent.
        :param delay: The number of seconds to delay the job for.
        """
        self._send_cmd(b"release %d %d %d" % (job.id, priority, delay), b"RELEASED")

    def bury(self, job: Job[TBody], priority: int = DEFAULT_PRIORITY) -> None:
        """Buries a reserved job.

        :param job: The job to bury.
        :param priority: An integer between 0 and 4,294,967,295 where 0 is the
                         most urgent.
        """
        self._send_cmd(b"bury %d %d" % (job.id, priority), b"BURIED")

    def touch(self, job: Job[TBody]) -> None:
        """Refreshes the TTR of a reserved job.

        :param job: The job to touch.
        """
        self._send_cmd(b"touch %d" % job.id, b"TOUCHED")

    def watch(self, tube: str) -> int:
        """Adds a tube to the watch list. Returns the number of tubes this
        client is watching.

        :param tube: The tube to watch.
        """
        return self._int_cmd(b"watch %b" % tube.encode("ascii"), b"WATCHING")

    def ignore(self, tube: str) -> int:
        """Removes a tube from the watch list. Returns the number of tubes this
        client is watching.

        :param tube: The tube to ignore.
        """
        return self._int_cmd(b"ignore %b" % tube.encode("ascii"), b"WATCHING")

    def peek(self, id: int) -> Job[TBody]:
        """Returns a job by ID.

        :param id: The ID of the job to peek.
        """
        return self._peek_cmd(b"peek %d" % id)

    def peek_ready(self) -> Job[TBody]:
        """Returns the next ready job in the currently used tube."""
        return self._peek_cmd(b"peek-ready")

    def peek_delayed(self) -> Job[TBody]:
        """Returns the next available delayed job in the currently used tube."""
        return self._peek_cmd(b"peek-delayed")

    def peek_buried(self) -> Job[TBody]:
        """Returns the oldest buried job in the currently used tube."""
        return self._peek_cmd(b"peek-buried")

    def kick(self, bound: int) -> int:
        """Moves delayed and buried jobs into the ready queue and returns the
        number of jobs effected.

        Only jobs from the currently used tube are moved.

        A kick will only move jobs in a single state. If there are any buried
        jobs, only those will be moved. Otherwise delayed jobs will be moved.

        :param bound: The maximum number of jobs to kick.
        """
        return self._int_cmd(b"kick %d" % bound, b"KICKED")

    def kick_job(self, job: JobOrID[TBody]) -> None:
        """Moves a delayed or buried job into the ready queue.

        :param job: The job or job ID to kick.
        """
        self._send_cmd(b"kick-job %d" % _to_id(job), b"KICKED")

    def stats_job(self, job: JobOrID[TBody]) -> StatsJob:
        """Returns job statistics.

        :param job: The job or job ID to return statistics for.
        """
        return cast(StatsJob, self._stats_cmd(b"stats-job %d" % _to_id(job)))

    def stats_tube(self, tube: str) -> StatsTube:
        """Returns tube statistics.

        :param tube: The tube to return statistics for.
        """
        return cast(StatsTube, self._stats_cmd(b"stats-tube %b" % tube.encode("ascii")))

    def stats(self) -> Stats:
        """Returns system statistics."""
        return cast(Stats, self._stats_cmd(b"stats"))

    def tubes(self) -> List[str]:
        """Returns a list of all existing tubes."""
        return self._list_cmd(b"list-tubes")

    def using(self) -> str:
        """Returns the tube currently being used by the client."""
        (tube,) = self._send_cmd(b"list-tube-used", b"USING")
        return tube.decode("ascii")

    def watching(self) -> List[str]:
        """Returns a list of tubes currently being watched by the client."""
        return self._list_cmd(b"list-tubes-watched")

    def pause_tube(self, tube: str, delay: int) -> None:
        """Prevents jobs from being reserved from a tube for a period of time.

        :param tube: The tube to pause.
        :param delay: The number of seconds to pause the tube for.
        """
        self._send_cmd(b"pause-tube %b %d" % (tube.encode("ascii"), delay), b"PAUSED")

    def __repr__(self) -> str:
        if isinstance(self._address, str):
            return f"greenstalk.Client(socket={self._address!r})"

        host, port = self._address
        return f"greenstalk.Client(host={host!r}, port={port!r})"


def _to_id(j: JobOrID[TBody]) -> int:
    return j.id if isinstance(j, Job) else j


def _parse_response(line: bytes, expected: bytes) -> List[bytes]:
    if not line:
        raise ConnectionError("Unexpected EOF")

    assert line[-2:] == b"\r\n"
    line = line[:-2]

    status, *values = line.split()

    if status == expected:
        return values

    if status in ERROR_RESPONSES:
        raise ERROR_RESPONSES[status](values)

    raise UnknownResponseError(status, values)


def _parse_chunk(data: bytes, size: int) -> bytes:
    assert data[-2:] == b"\r\n"
    data = data[:-2]

    if len(data) != size:
        raise ConnectionError("Unexpected EOF reading chunk")

    return data


def _parse_stats(buf: bytes) -> Union[Stats, StatsJob, StatsTube]:
    data = buf.decode("ascii")

    assert data[:4] == "---\n"
    data = data[4:]  # strip YAML head

    stats: Union[Stats, StatsJob, StatsTube] = {}  # type: ignore
    for line in data.splitlines():
        key, value = line.split(": ", 1)
        try:
            v: Union[int, str] = int(value)
        except ValueError:
            v = _maybe_strip_quotes(value)
        stats[key] = v  # type: ignore

    return stats


def _parse_list(buf: bytes) -> List[str]:
    data = buf.decode("ascii")

    assert data[:4] == "---\n"
    data = data[4:]  # strip YAML head

    values: List[str] = []
    for line in data.splitlines():
        assert line.startswith("- ")
        values.append(line[2:])

    return values


def _maybe_strip_quotes(s: str) -> str:
    if len(s) >= 2 and s[0] == '"' and s[-1] == '"':
        return s[1:-1]
    return s
