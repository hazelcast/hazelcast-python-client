Development and Testing
=======================

If you want to help with bug fixes, develop new features or tweak the
implementation to your application’s needs, you can follow the steps in
this section.

Building and Using Client From Sources
--------------------------------------

Follow the below steps to build and install Hazelcast Python client from
its source:

1. Clone the GitHub repository
   (https://github.com/hazelcast/hazelcast-python-client.git).
2. Run ``python setup.py install`` to install the Python client.

If you are planning to contribute:

1. Run ``pip install -r requirements-dev.txt`` to install development
   dependencies.
2. Use `black <https://pypi.org/project/black/>`__ to reformat the code
   by running the ``black --config black.toml .`` command.
3. Make sure that tests are passing by following the steps described
   in the :ref:`development_and_testing:testing` section.

Testing
-------

In order to test Hazelcast Python client locally, you will need the
following:

- Java 8 or newer
- Maven

Following commands starts the tests:

.. code:: bash

    python run_tests.py

Test script automatically downloads ``hazelcast-remote-controller`` and
Hazelcast IMDG. The script uses Maven to download those.
