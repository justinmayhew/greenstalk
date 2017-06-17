Greenstalk
==========

.. image:: https://secure.travis-ci.org/mayhewj/greenstalk.png?branch=master
        :target: https://travis-ci.org/mayhewj/greenstalk

Installation
------------

Greenstalk supports Python 3.5 and later. It's available on the Python Package
Index and can be installed by running:

.. code-block:: bash

    $ pip install greenstalk

Quickstart
----------

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

Resources
---------

- `beanstalkd FAQ <https://github.com/kr/beanstalkd/wiki/faq>`_
- `Protocol documentation
  <https://raw.githubusercontent.com/kr/beanstalkd/master/doc/protocol.txt>`_
