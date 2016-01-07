from setuptools import setup, find_packages
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
        name='hazelcast-python-client',
        version='0.1',
        description='Hazelcast Python Client',
        long_description=long_description,
        url='https://github.com/hazelcast/hazelcast-python-client',

        author='The Python Packaging Authority',
        author_email='pypa-dev@googlegroups.com',

        classifiers=[
            #   3 - Alpha
            #   4 - Beta
            #   5 - Production/Stable
            'Development Status :: 3 - Alpha',
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
        packages=find_packages(exclude=['examples', 'docs', 'tests']),
        package_dir={'hazelcast': 'hazelcast'},
        install_requires=[],
        tests_require=['hazelcast-remote-controller'],
)
