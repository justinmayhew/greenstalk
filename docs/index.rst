Welcome to Greenstalk
=====================

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

Contents
--------

.. toctree::
   :maxdepth: 2

   install
   quickstart
   api

Links
-----

This project is developed on GitHub. Contributions are welcome.

- `Code <https://github.com/justinmayhew/greenstalk>`_
- `Issue tracker <https://github.com/justinmayhew/greenstalk/issues>`_

Inspiration
-----------

Greenstalk is heavily inspired by the following libraries:

- `Go - beanstalk <https://github.com/beanstalkd/go-beanstalk>`_
- `Python - beanstalkc <https://github.com/earl/beanstalkc/>`_
