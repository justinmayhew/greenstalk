import socket
from datetime import timedelta
from typing import Any, Iterable, List, Optional, Tuple, Union

from .exceptions import ERROR_REPLIES, DisconnectError

DEFAULT_TUBE = 'default'
DEFAULT_PRIORITY = 2**16
DEFAULT_DELAY = timedelta()
DEFAULT_TTR = timedelta(seconds=60)


class Client:

    def __init__(self,
                 host: str = '127.0.0.1',
                 port: int = 11300,
                 encoding: Optional[str] = 'utf-8',
                 use: str = DEFAULT_TUBE,
                 watch: Union[str, Iterable[str]] = DEFAULT_TUBE) -> None:
        self._sock = socket.create_connection((host, port))
        self._reader = self._sock.makefile('rb')
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

    def close(self):
        self._reader.close()
        self._sock.close()

    def _send_command(self,
                      line: bytes,
                      *args: Any,
                      body: Optional[bytes] = None) -> None:
        command = (line + b'\r\n') % args
        if body is not None:
            command = command + body + b'\r\n'
        self._sock.sendall(command)

    def _read_reply(self, expected: bytes) -> List[bytes]:
        line = self._reader.readline()[:-2]
        if not line:
            raise DisconnectError
        reply, *args = line.split()
        if reply != expected:
            if reply in ERROR_REPLIES:
                raise ERROR_REPLIES[reply]
            # Unknown reply, probably disconnected mid-message.
            raise DisconnectError
        return args

    def _request(self,
                 line: bytes,
                 *args: Any,
                 body: Optional[bytes] = None,
                 expected: bytes) -> List[bytes]:
        self._send_command(line, *args, body=body)
        return self._read_reply(expected)

    # Producer Commands

    def put(self,
            body: Union[bytes, str],
            priority: int = DEFAULT_PRIORITY,
            delay: timedelta = DEFAULT_DELAY,
            ttr: timedelta = DEFAULT_TTR) -> int:
        if isinstance(body, str):
            body = body.encode(self.encoding)
        args = self._request(b'put %d %d %d %d', priority,
                             delay.total_seconds(), ttr.total_seconds(),
                             len(body), body=body, expected=b'INSERTED')
        return int(args[0])

    def use(self, tube: str) -> None:
        self._request(b'use %b', tube.encode('ascii'), expected=b'USING')

    # Consumer Commands

    def reserve(self,
                timeout: timedelta = None) -> Tuple[int, Union[bytes, str]]:
        expected = b'RESERVED'
        if timeout is None:
            args = self._request(b'reserve', expected=expected)
        else:
            args = self._request(b'reserve-with-timeout %d',
                                 timeout.total_seconds(), expected=expected)
        size = int(args[1])
        body = self._reader.read(size + 2)[:-2]
        if len(body) != size:
            raise DisconnectError
        if self.encoding is not None:
            body = body.decode(self.encoding)
        return int(args[0]), body

    def delete(self, jid: int) -> None:
        self._request(b'delete %d', jid, expected=b'DELETED')

    def release(self,
                jid: int,
                priority: int = DEFAULT_PRIORITY,
                delay: timedelta = DEFAULT_DELAY) -> None:
        self._request(b'release %d %d %d', jid, priority,
                      delay.total_seconds(), expected=b'RELEASED')

    def bury(self, jid: int, priority: int = DEFAULT_PRIORITY) -> None:
        self._request(b'bury %d %d', jid, priority, expected=b'BURIED')

    def touch(self, jid: int) -> None:
        self._request(b'touch %d', jid, expected=b'TOUCHED')

    def watch(self, tube: str) -> int:
        args = self._request(b'watch %b', tube.encode('ascii'),
                             expected=b'WATCHING')
        return int(args[0])

    def ignore(self, tube: str) -> int:
        args = self._request(b'ignore %b', tube.encode('ascii'),
                             expected=b'WATCHING')
        return int(args[0])
