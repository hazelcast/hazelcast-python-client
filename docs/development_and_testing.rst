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

If you are planning to contribute, please make sure that it fits the
guidelines described in
`PEP8 <https://www.python.org/dev/peps/pep-0008/>`__.

Testing
-------

In order to test Hazelcast Python client locally, you will need the
following:

- Java 8 or newer
- Maven

Following commands starts the tests according to your operating system:

.. code:: bash

    bash run-tests.sh

or

.. code:: powershell

    .\run-tests.ps1

Test script automatically downloads ``hazelcast-remote-controller`` and
Hazelcast IMDG. The script uses Maven to download those.
