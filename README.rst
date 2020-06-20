Greenstalk
==========

Greenstalk is a small and unopinionated Python client library for communicating
with the `beanstalkd`_ work queue. The API provided mostly maps one-to-one with
commands in the `protocol`_.

.. image:: https://img.shields.io/pypi/v/greenstalk.svg
    :target: https://pypi.org/project/greenstalk/

.. image:: https://secure.travis-ci.org/justinmayhew/greenstalk.svg?branch=master
    :target: https://travis-ci.org/justinmayhew/greenstalk

.. image:: https://codecov.io/github/justinmayhew/greenstalk/coverage.svg?branch=master
    :target: https://codecov.io/github/justinmayhew/greenstalk

Getting Started
---------------

.. code-block:: pycon

    >>> import greenstalk
    >>> client = greenstalk.Client(('127.0.0.1', 11300))
    >>> client.put('hello')
    1
    >>> job = client.reserve()
    >>> job.id
    1
    >>> job.body
    'hello'
    >>> client.delete(job)
    >>> client.close()

Documentation
-------------

Documentation is available on `Read the Docs`_.

.. _`beanstalkd`: https://beanstalkd.github.io/
.. _`protocol`: https://raw.githubusercontent.com/beanstalkd/beanstalkd/master/doc/protocol.txt
.. _`Read the Docs`: https://greenstalk.readthedocs.io/
