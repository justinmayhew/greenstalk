Welcome to Greenstalk
=====================

Greenstalk is a Python client library for communicating with the `beanstalkd
<https://kr.github.io/beanstalkd/>`_ work queue.  It makes it easy to write:

- **Producers**, processes that insert jobs into a queue:

.. code-block:: python3

    import greenstalk

    with greenstalk.Client(host='127.0.0.1', port=11300) as queue:
        queue.put('hello')

- **Consumers**, processes that take jobs from a queue and execute some work:

.. code-block:: python3

    import greenstalk

    with greenstalk.Client(host='127.0.0.1', port=11300) as queue:
        while True:
            job = queue.reserve()
            print(job.body)
            queue.delete(job)

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

- `Code <https://github.com/mayhewj/greenstalk>`_
- `Issue tracker <https://github.com/mayhewj/greenstalk/issues>`_

Inspiration
-----------

Greenstalk is heavily inspired by the following libraries:

- `Go - beanstalk <https://github.com/kr/beanstalk>`_
- `Python - beanstalkc <https://github.com/earl/beanstalkc/>`_
