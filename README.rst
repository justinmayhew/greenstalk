Greenstalk
==========

.. image:: https://img.shields.io/pypi/v/greenstalk.svg
    :target: https://pypi.org/project/greenstalk/

.. image:: https://secure.travis-ci.org/mayhewj/greenstalk.svg?branch=master
    :target: https://travis-ci.org/mayhewj/greenstalk

.. image:: https://codecov.io/github/mayhewj/greenstalk/coverage.svg?branch=master
    :target: https://codecov.io/github/mayhewj/greenstalk

Installation
------------

Greenstalk supports Python 3.5 and later. It's available on the Python Package
Index and can be installed by running:

.. code-block:: bash

    $ pip install greenstalk

Getting Started
---------------

.. code-block:: pycon

    >>> import greenstalk
    >>> queue = greenstalk.Client(host='127.0.0.1', port=11300)
    >>> queue.put('hello')
    1
    >>> job = queue.reserve()
    >>> job.id
    1
    >>> job.body
    'hello'
    >>> queue.delete(job)
    >>> queue.close()

Documentation
-------------

Documentation is available on `Read the Docs
<https://greenstalk.readthedocs.io/>`_.

Resources
---------

- `beanstalkd FAQ <https://github.com/beanstalkd/beanstalkd/wiki/faq>`_
- `Protocol documentation
  <https://raw.githubusercontent.com/beanstalkd/beanstalkd/master/doc/protocol.txt>`_
