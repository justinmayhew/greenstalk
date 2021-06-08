Overview
========

Before getting started, ensure that Greenstalk is :doc:`installed <install>` and
the server is running.

Setup
-----

Begin by importing the library:

.. code-block:: pycon3

    >>> import greenstalk

Create a :class:`Client <greenstalk.Client>`, which immediately connects to
the server on the host and port specified:

.. code-block:: pycon3

    >>> client = greenstalk.Client(('127.0.0.1', 11300))

Alternatively, if your server is listening on a Unix domain socket, pass the
socket path instead:

.. code-block:: pycon3

    >>> client = greenstalk.Client('/var/run/beanstalkd/socket')

Inserting Jobs
--------------

Jobs are inserted using :meth:`put <greenstalk.Client.put>`. The job body is the
only required argument:

.. code-block:: pycon3

    >>> client.put(b'hello')
    1

Jobs are inserted into the currently used tube, which defaults to ``default``.
The currently used tube can be changed via :meth:`use <greenstalk.Client.use>`.
It can also be set with the ``use`` argument when creating a
:class:`Client <greenstalk.Client>`.

Consuming Jobs
--------------

Jobs are consumed using :meth:`reserve <greenstalk.Client.reserve>`. It blocks
until a job is reserved (unless the ``timeout`` argument is used):

.. code-block:: pycon3

    >>> job = client.reserve()
    >>> job.id
    1
    >>> job.body
    b'hello'

Jobs will only be reserved from tubes on the watch list, which initially
contains a single tube, ``default``. You can add tubes to the watch list with
:meth:`watch <greenstalk.Client.watch>` and remove them with :meth:`ignore
<greenstalk.Client.ignore>`. For convenience, it can be set with the ``watch``
argument when creating a :class:`Client <greenstalk.Client>`.

The server guarantees that jobs are only reserved by a single consumer
simultaneously. Let's go ahead and tell the server that we've successfully
completed the job using :meth:`delete <greenstalk.Client.delete>`:

.. code-block:: pycon3

    >>> client.delete(job)

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

Body Serialization and Encoding
-------------------------------

The server does not inspect the contents of job bodies, it's only concerned with
routing them between clients. This gives clients full control over how they're
sent and received on the underlying connection.

JSON serialized payloads encoded in UTF-8 are a great default representation.

Here's an example showing how a producer and consumer (likely running in
separate processes) could communicate a user registration email job.

Producer:

.. code-block:: python3

    payload = {'user_id': user_id}
    body = json.dumps(payload).encode('utf-8')
    client.put(body)

The consumer would then do the inverse:

.. code-block:: python3

    job = client.reserve()
    payload = json.loads(job.body.decode('utf-8'))
    send_registration_email(payload['user_id'])

Job Priorities
--------------

Every job has a priority which is an integer between 0 and 4,294,967,295. 0 is
the most urgent priority. The :meth:`put <greenstalk.Client.put>`,
:meth:`release <greenstalk.Client.release>` and :meth:`bury
<greenstalk.Client.bury>` methods all take a ``priority`` argument that defaults
to ``2**16``.

Delaying a Job
--------------

Sometimes you'll want to schedule work to be executed sometime in the future.
Both the :meth:`put <greenstalk.Client.put>` and :meth:`release
<greenstalk.Client.release>` methods have a ``delay`` argument.

Time to Run
-----------

Every job has an associated time to run (TTR) value specified by the ``ttr``
argument to the :meth:`put <greenstalk.Client.put>` method. It defaults to 60
seconds.

The server starts a timer when a job is reserved. If the consumer doesn't send a
:meth:`delete <greenstalk.Client.delete>`, :meth:`release
<greenstalk.Client.release>`, or :meth:`bury <greenstalk.Client.bury>` command
within the TTR, the job will time out and be released back into the ready queue.

If more time is required to complete a job, the :meth:`touch
<greenstalk.Client.touch>` method can be used to refresh the TTR.

Job Lifecycle
-------------

Here's a great flowchart from the beanstalkd `protocol documentation`_::

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

.. _protocol documentation: https://raw.githubusercontent.com/beanstalkd/beanstalkd/master/doc/protocol.txt
