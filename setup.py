from setuptools import setup, find_packages
from codecs import open
from os import path
from hazelcast import __version__
here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

setup(
        name='hazelcast-python-client',
        version=__version__,
        description='Hazelcast Python Client',
        long_description=long_description,
        url='https://github.com/hazelcast/hazelcast-python-client',

        author='Hazelcast Inc. Developers',
        author_email='hazelcast@googlegroups.com',

        classifiers=[
            'Development Status :: 5 - Production/Stable',
            'Intended Audience :: Developers',
            'License :: OSI Approved :: Apache Software License',
            'Natural Language :: English',
            'Operating System :: OS Independent',
            'Programming Language :: Python',
            # 'Programming Language :: Python :: 2.6',
            'Programming Language :: Python :: 2.7',
            # 'Programming Language :: Python :: 3.3',
            # 'Programming Language :: Python :: 3.4',
            'Programming Language :: Python :: Implementation :: CPython',
            # 'Programming Language :: Python :: Implementation :: PyPy',
            'Topic :: Software Development :: Libraries :: Python Modules'

        ],
        license='Apache 2.0',
        keywords='hazelcast,hazelcast client,In-Memory Data Grid,Distributed Computing',
        packages=find_packages(exclude=['examples', 'docs', 'tests', 'benchmark']),
        package_dir={'hazelcast': 'hazelcast'},
        install_requires=[],
        tests_require=['hazelcast-remote-controller', 'nose', 'coverage'],
)
