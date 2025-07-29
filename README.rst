Hazelcast Python Client
=======================

.. image:: https://img.shields.io/pypi/v/hazelcast-python-client
    :target: https://pypi.org/project/hazelcast-python-client/
    :alt: PyPI
.. image:: https://img.shields.io/readthedocs/hazelcast
    :target: https://hazelcast.readthedocs.io
    :alt: Read the Docs
.. image:: https://img.shields.io/badge/slack-chat-green.svg
    :target: https://slack.hazelcast.com
    :alt: Join the community on Slack
.. image:: https://img.shields.io/pypi/l/hazelcast-python-client
    :target: https://github.com/hazelcast/hazelcast-python-client/blob/master/LICENSE.txt
    :alt: License

----

`Hazelcast <https://hazelcast.com/>`__ is an open-source distributed
in-memory data store and computation platform that provides a wide
variety of distributed data structures and concurrency primitives.

Hazelcast Python client is a way to communicate to Hazelcast clusters
and access the cluster data. The client provides a Future-based
asynchronous API suitable for wide ranges of use cases.

For a list of the features available, and for information about how 
to install and get started with the client, see the 
`Python client documentation <https://docs.hazelcast.com/hazelcast/latest/clients/python>`__.


Contributing
------------

We encourage any type of contribution in the form of issue reports or
pull requests.

Issue Reports
~~~~~~~~~~~~~

For issue reports, please share the following information with us to
quickly resolve the problems:

-  Hazelcast and the client version that you use
-  General information about the environment and the architecture you
   use like Python version, cluster size, number of clients, Java
   version, JVM parameters, operating system etc.
-  Logs and stack traces, if any
-  Detailed description of the steps to reproduce the issue

Pull Requests
~~~~~~~~~~~~~

Contributions are submitted, reviewed and accepted using the pull
requests on GitHub. For an enhancement or larger feature, please
create a GitHub issue first to discuss.

Development
^^^^^^^^^^^

1. Clone the `GitHub repository
   <https://github.com/hazelcast/hazelcast-python-client>`__.
2. Run ``python setup.py install`` to install the Python client.

If you are planning to contribute:

1. Run ``pip install -r requirements-dev.txt`` to install development
   dependencies.
2. Use `black <https://pypi.org/project/black/>`__ to reformat the code
   by running the ``black --config black.toml .`` command.
3. Use `mypy <https://pypi.org/project/mypy/>`__ to check type annotations
   by running the ``mypy hazelcast`` command.
4. Make sure that tests are passing by following the steps described
   in the next section.

Testing
^^^^^^^

In order to test Hazelcast Python client locally, you will need the
following:

-  `Supported Java virtual machine <https://docs.hazelcast.com/hazelcast/latest/deploy/versioning-compatibility#supported-java-virtual-machines>`
-  `Apache Maven <https://maven.apache.org/>`

Set the environment variables for credentials:

.. code:: bash

    export HZ_SNAPSHOT_INTERNAL_USERNAME=YOUR_MAVEN_USERNAME
    export HZ_SNAPSHOT_INTERNAL_PASSWORD=YOUR_MAVEN_PASSWORD

Following command starts the tests:

.. code:: bash

    python3 run_tests.py

Test script automatically downloads ``hazelcast-remote-controller`` and
Hazelcast. The script uses Maven to download those.

License
-------

`Apache 2.0 License <LICENSE>`__.

Copyright
---------

Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.

Visit `hazelcast.com <https://hazelcast.com>`__ for more
information.
