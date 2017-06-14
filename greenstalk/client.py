import socket
from datetime import timedelta
from typing import BinaryIO, Dict, Iterable, List, Optional, Tuple, Union

from .exceptions import ERROR_RESPONSES, UnknownResponseError

Body = Union[bytes, str]
Stats = Dict[str, Union[str, int]]

DEFAULT_TUBE = 'default'
DEFAULT_PRIORITY = 2**16
DEFAULT_DELAY = timedelta()
DEFAULT_TTR = timedelta(seconds=60)


class Client:
    """Client implementation of the beanstalk protocol."""

    def __init__(self,
                 host: str = '127.0.0.1',
                 port: int = 11300,
                 encoding: Optional[str] = 'utf-8',
                 use: str = DEFAULT_TUBE,
                 watch: Union[str, Iterable[str]] = DEFAULT_TUBE) -> None:
        """Configure the client and connect to beanstalkd."""
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
        """Close the connection to beanstalkd."""
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

    def _read_job(self, values: List[bytes]) -> Tuple[int, Body]:
        assert len(values) == 2
        jid = int(values[0])
        size = int(values[1])

        body = self._read_data(size)
        return jid, self._decode_body(body)

    def _decode_body(self, body: bytes) -> Body:
        if self.encoding is not None:
            return body.decode(self.encoding)
        return body

    # Producer Commands

    def put(self,
            body: Body,
            priority: int = DEFAULT_PRIORITY,
            delay: timedelta = DEFAULT_DELAY,
            ttr: timedelta = DEFAULT_TTR) -> int:
        """Enqueue a job into the currently used tube."""
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
        """
        Change the currently used tube.

        Future put commands will enqueue into the currently used tube.
        """
        cmd = b'use %b' % tube.encode('ascii')
        self._send_cmd(cmd, b'USING')

    # Consumer Commands

    def reserve(self, timeout: timedelta = None) -> Tuple[int, Body]:
        """Dequeue a job from a tube on the watch list."""
        if timeout is None:
            cmd = b'reserve'
        else:
            cmd = b'reserve-with-timeout %d' % timeout.total_seconds()
        values = self._send_cmd(cmd, b'RESERVED')
        return self._read_job(values)

    def delete(self, jid: int) -> None:
        """Delete a job to signal that the associated work is complete."""
        cmd = b'delete %d' % jid
        self._send_cmd(cmd, b'DELETED')

    def release(self,
                jid: int,
                priority: int = DEFAULT_PRIORITY,
                delay: timedelta = DEFAULT_DELAY) -> None:
        """
        Release a reserved job back into the ready queue.

        This signals that the associated work is incomplete. Consumers will be
        able to reserve and retry the job.
        """
        cmd = b'release %d %d %d' % (jid, priority, delay.total_seconds())
        self._send_cmd(cmd, b'RELEASED')

    def bury(self, jid: int, priority: int = DEFAULT_PRIORITY) -> None:
        """Put a job into the buried FIFO until it's kicked."""
        cmd = b'bury %d %d' % (jid, priority)
        self._send_cmd(cmd, b'BURIED')

    def touch(self, jid: int) -> None:
        """Request additional time to complete a job."""
        cmd = b'touch %d' % jid
        self._send_cmd(cmd, b'TOUCHED')

    def watch(self, tube: str) -> int:
        """
        Add a tube to the watch list.

        Future reserve commands will dequeue jobs from any tube on the watch
        list.
        """
        cmd = b'watch %b' % tube.encode('ascii')
        values = self._send_cmd(cmd, b'WATCHING')
        return int(values[0])

    def ignore(self, tube: str) -> int:
        """Remove a tube from the watch list."""
        cmd = b'ignore %b' % tube.encode('ascii')
        values = self._send_cmd(cmd, b'WATCHING')
        return int(values[0])

    def peek(self, jid: int) -> Tuple[int, Body]:
        cmd = b'peek %d' % jid
        values = self._send_cmd(cmd, b'FOUND')
        return self._read_job(values)

    def peek_ready(self) -> Tuple[int, Body]:
        cmd = b'peek-ready'
        values = self._send_cmd(cmd, b'FOUND')
        return self._read_job(values)

    def peek_delayed(self) -> Tuple[int, Body]:
        cmd = b'peek-delayed'
        values = self._send_cmd(cmd, b'FOUND')
        return self._read_job(values)

    def peek_buried(self) -> Tuple[int, Body]:
        cmd = b'peek-buried'
        values = self._send_cmd(cmd, b'FOUND')
        return self._read_job(values)

    def kick(self, bound: int) -> int:
        cmd = b'kick %d' % bound
        values = self._send_cmd(cmd, b'KICKED')
        return int(values[0])

    def kick_job(self, jid: int) -> None:
        cmd = b'kick-job %d' % jid
        self._send_cmd(cmd, b'KICKED')

    def stats_job(self, jid: int) -> Stats:
        cmd = b'stats-job %d' % jid
        values = self._send_cmd(cmd, b'OK')
        data = self._read_data(int(values[0]))
        return _parse_simple_yaml(data)

    def stats_tube(self, tube: str) -> Stats:
        cmd = b'stats-tube %b' % tube.encode('ascii')
        values = self._send_cmd(cmd, b'OK')
        data = self._read_data(int(values[0]))
        return _parse_simple_yaml(data)

    def stats(self) -> Stats:
        cmd = b'stats'
        values = self._send_cmd(cmd, b'OK')
        data = self._read_data(int(values[0]))
        return _parse_simple_yaml(data)


def _parse_simple_yaml(buf: bytes) -> Stats:
    data = buf.decode('ascii')

    assert data[:4] == '---\n'
    data = data[4:]  # strip YAML head

    stats = {}
    for line in data.splitlines():
        key, value = line.split(': ')  # type: Tuple[str, Union[str, int]]
        try:
            value = int(value)
        except ValueError:
            pass
        stats[key] = value

    return stats
