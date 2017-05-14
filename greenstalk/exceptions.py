class Error(Exception):
    pass


class BadFormatError(Error):
    pass


class BuriedError(Error):
    pass


class DeadlineSoonError(Error):
    pass


class DrainingError(Error):
    pass


class ExpectedCrlfError(Error):
    pass


class InternalError(Error):
    pass


class JobTooBigError(Error):
    pass


class NotFoundError(Error):
    pass


class NotIgnoredError(Error):
    pass


class OutOfMemoryError(Error):
    pass


class TimedOutError(Error):
    pass


class UnknownCommandError(Error):
    pass


ERROR_REPLIES = {
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


class UnexpectedDisconnectError(Error):
    pass
