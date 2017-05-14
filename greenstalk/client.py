import socket
from datetime import timedelta
from typing import Any, List, Optional, Tuple

from .exceptions import ERROR_REPLIES, UnexpectedDisconnectError

DEFAULT_PRIORITY = 2**16
DEFAULT_DELAY = timedelta()
DEFAULT_TTR = timedelta(seconds=60)


class Client:

    def __init__(self, host: str = '127.0.0.1', port: int = 11300) -> None:
        self._sock = socket.create_connection((host, port))
        self._reader = self._sock.makefile('rb')

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
            raise UnexpectedDisconnectError
        reply, *args = line.split()
        if reply != expected:
            if reply in ERROR_REPLIES:
                raise ERROR_REPLIES[reply]
            # Unknown reply, probably disconnected mid-message.
            raise UnexpectedDisconnectError
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
            body: bytes,
            priority: int = DEFAULT_PRIORITY,
            delay: timedelta = DEFAULT_DELAY,
            ttr: timedelta = DEFAULT_TTR) -> int:
        args = self._request(b'put %d %d %d %d', priority,
                             delay.total_seconds(), ttr.total_seconds(),
                             len(body), body=body, expected=b'INSERTED')
        return int(args[0])

    def use(self, tube: bytes) -> None:
        self._request(b'use %s', tube, expected=b'USING')

    # Consumer Commands

    def reserve(self, timeout: timedelta = None) -> Tuple[int, bytes]:
        expected = b'RESERVED'
        if timeout is None:
            args = self._request(b'reserve', expected=expected)
        else:
            args = self._request(b'reserve-with-timeout %d',
                                 timeout.total_seconds(), expected=expected)
        size = int(args[1])
        body = self._reader.read(size + 2)[:-2]
        if len(body) != size:
            raise UnexpectedDisconnectError
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

    def watch(self, tube: bytes) -> int:
        args = self._request(b'watch %s', tube, expected=b'WATCHING')
        return int(args[0])

    def ignore(self, tube: bytes) -> int:
        args = self._request(b'ignore %s', tube, expected=b'WATCHING')
        return int(args[0])
