API Reference
=============

Job
---

.. autoclass:: greenstalk.Job
    :members:

Client
------

.. autoclass:: greenstalk.Client
    :members:

Exceptions
----------

For completeness all errors that beanstalkd can return are listed here.
:class:`BadFormatError <greenstalk.BadFormatError>` and
:class:`ExpectedCrlfError <greenstalk.ExpectedCrlfError>` *should* be
unreachable unless there's a bug in this library.

.. autoclass:: greenstalk.Error

.. autoclass:: greenstalk.BeanstalkdError

.. autoclass:: greenstalk.NotFoundError

.. autoclass:: greenstalk.TimedOutError

.. autoclass:: greenstalk.DeadlineSoonError

.. autoclass:: greenstalk.NotIgnoredError

.. autoclass:: greenstalk.BuriedError

.. autoclass:: greenstalk.BuriedWithJobIDError
    :members:

.. autoclass:: greenstalk.DrainingError

.. autoclass:: greenstalk.JobTooBigError

.. autoclass:: greenstalk.OutOfMemoryError

.. autoclass:: greenstalk.InternalError

.. autoclass:: greenstalk.BadFormatError

.. autoclass:: greenstalk.ExpectedCrlfError

.. autoclass:: greenstalk.UnknownCommandError

.. autoclass:: greenstalk.UnknownResponseError
    :members:
