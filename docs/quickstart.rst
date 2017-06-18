Quickstart
==========

Let's start queueing! Before we get started, make sure that Greenstalk is
installed and ``beanstalkd`` is running.

Setup
-----

Begin by importing the library:

.. code-block:: pycon3

    >>> import greenstalk

Create a :class:`Client <greenstalk.Client>`, which immediately connects to
``beanstalkd`` on the host and port specified:

.. code-block:: pycon3

    >>> queue = greenstalk.Client(host='127.0.0.1', port=11300)

Inserting Jobs
--------------

When inserting jobs, we need to specify the body. Job bodies are opaque
sequences of bytes that represent the work that needs to be done. Let's use
:meth:`put <greenstalk.Client.put>` to insert a simple string:

.. code-block:: pycon3

    >>> queue.put('hello')
    1

Consuming Jobs
--------------

Jobs are consumed using :meth:`reserve <greenstalk.Client.reserve>`. It blocks
until a job is reserved (unless the ``timeout`` argument is used):

.. code-block:: pycon3

    >>> job = queue.reserve()
    >>> job.id
    1
    >>> job.body
    'hello'

``beanstalkd`` guarantees that jobs are only reserved by a single consumer
simultaneously. Let's go ahead and tell the server that we've successfully
completed the job using :meth:`delete <greenstalk.Client.delete>`:

.. code-block:: pycon3

    >>> queue.delete(job)

Here's what you can do with a reserved job to change its state:

+-------------+------------------+---------------------------------------------+
| Command     | Normal use case  | Effect                                      |
+=============+==================+=============================================+
| ``delete``  | Success          | Job is permanently deleted                  |
+-------------+------------------+---------------------------------------------+
| ``release`` | Expected failure | Job is released back into the queue to be   |
|             |                  | retried                                     |
+-------------+------------------+---------------------------------------------+
| ``bury``    | Unknown failure  | Job is put in a special FIFO list for later |
|             |                  | inspection                                  |
+-------------+------------------+---------------------------------------------+

Body Serialization
------------------

It was mentioned earlier that a job body, from ``beanstalkd``'s point of view,
is just an opaque sequences of bytes. That means it's up to the clients to agree
on a serialization format to represent the data required to complete the job.

In the context of a web application where a user just signed up and we need to
send an email with a registration code, the producer may look something like
this:

.. code-block:: python3

    body = json.dumps({
        'email': user.email,
        'name': user.name,
        'code': code,
    })
    queue.put(body)

The consumer would then do the inverse:

.. code-block:: python3

    job = queue.reserve()
    data = json.loads(job.body)
    send_registration_email(data['email'], data['name'], data['code'])

Body Encoding
-------------

When creating a :class:`Client <greenstalk.Client>`, you can use the
``encoding`` argument to control how job bodies are encoded and decoded. It
defaults to UTF-8.

You can set the ``encoding`` to ``None`` if you're working with binary data. In
that case, you're expected to pass in ``bytes`` (rather than ``str``) bodies,
and ``bytes`` bodies will be returned.

Job Priorities
--------------

Every job has a priority which is an integer between 0 and 4,294,967,295. 0 is
the most urgent priority. The :meth:`put <greenstalk.Client.put>`,
:meth:`release <greenstalk.Client.release>` and :meth:`bury
<greenstalk.Client.bury>` methods all take an ``priority`` argument that
defaults to ``2**16`` if not specified.

Delaying a Job
--------------

Sometimes you'll want to schedule work to be executed sometime in the future.
Both the :meth:`put <greenstalk.Client.put>` and :meth:`release
<greenstalk.Client.release>` methods have a ``delay`` argument.

Time to Run
-----------

Every job has an associated time to run (TTR) value specified by the ``ttr``
argument to :meth:`put <greenstalk.Client.put>`. As soon as a job is reserved,
``beanstalkd`` starts the timer. If the client doesn't send a :meth:`delete
<greenstalk.Client.delete>`, :meth:`release <greenstalk.Client.release>`, or
:meth:`bury <greenstalk.Client.bury>` command within the TTR, the job will time
out and be released back into the ready queue.

Clients can also use the :meth:`touch <greenstalk.Client.touch>` method before
the job times out to refresh the TTR.

Job Lifecycle
-------------

Here's a great flowchart from the ``beanstalkd`` `protocol documentation`_::

     put with delay               release with delay
    ----------------> [DELAYED] <------------.
                          |                   |
                          | (time passes)     |
                          |                   |
     put                  v     reserve       |       delete
    -----------------> [READY] ---------> [RESERVED] --------> *poof*
                         ^  ^                |  |
                         |   \  release      |  |
                         |    `-------------'   |
                         |                      |
                         | kick                 |
                         |                      |
                         |       bury           |
                      [BURIED] <---------------'
                         |
                         |  delete
                          `--------> *poof*

.. _protocol documentation: https://raw.githubusercontent.com/kr/beanstalkd/master/doc/protocol.txt
