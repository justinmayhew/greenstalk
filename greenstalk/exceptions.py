from typing import List, Optional


class Error(Exception):
    pass


class BeanstalkdError(Error):
    """An error read from a beanstalkd response."""


class BadFormatError(BeanstalkdError):
    pass


class BuriedError(BeanstalkdError):

    def __init__(self, values: List[bytes] = None) -> None:
        if values:
            self.jid = int(values[0])  # type: Optional[int]
        else:
            self.jid = None


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
