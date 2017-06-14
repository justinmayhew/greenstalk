"""A client for beanstalkd: the simple, fast work queue."""
from .client import Client
from .exceptions import (
    BadFormatError, BeanstalkdError, BuriedError, DeadlineSoonError,
    DrainingError, Error, ExpectedCrlfError, InternalError, JobTooBigError,
    NotFoundError, NotIgnoredError, OutOfMemoryError, TimedOutError,
    UnknownCommandError, UnknownResponseError
)

__version__ = '0.3.0'
