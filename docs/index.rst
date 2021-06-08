Greenstalk Documentation
========================

Greenstalk is a Python client library for communicating with the `beanstalkd
<https://beanstalkd.github.io/>`_ work queue.  It makes it easy to write:

- **Producers**, processes that insert jobs into a queue:

.. code-block:: python3

    import greenstalk

    with greenstalk.Client(('127.0.0.1', 11300)) as client:
        client.put('hello'.encode('utf-8'))

- **Consumers**, processes that take jobs from a queue and execute some work:

.. code-block:: python3

    import greenstalk

    with greenstalk.Client(('127.0.0.1', 11300)) as client:
        while True:
            job = client.reserve()
            print(job.body.decode('utf-8'))
            client.delete(job)

This library is a thin wrapper over the wire protocol. The documentation doesn't
attempt to fully explain the semantics of the beanstalk protocol. It's assumed
that users of this library will be referring to the official beanstalkd
documentation.

Contents
--------

.. toctree::
   :maxdepth: 2

   install
   overview
   api
