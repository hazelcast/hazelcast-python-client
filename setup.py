from setuptools import setup, find_packages
from codecs import open
from os import path
from hazelcast import __version__
from hazelcast.version import write_git_info, get_commit_id, get_commit_date

here = path.abspath(path.dirname(__file__))

commit_id = get_commit_id()
commit_date = get_commit_date()
write_git_info(here, commit_id, commit_date)

# Get the long description from the README file
with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()


stats_requirements = [
    'psutil',
]

extras = {
    'stats': stats_requirements,
}

setup(
        name='hazelcast-python-client',
        version=__version__,
        description='Hazelcast Python Client',
        long_description=long_description,
        long_description_content_type='text/markdown',
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
            'Programming Language :: Python :: 2.7',
            'Programming Language :: Python :: 3.4',
            'Programming Language :: Python :: 3.5',
            'Programming Language :: Python :: 3.6',
            'Programming Language :: Python :: 3.7',
            'Programming Language :: Python :: Implementation :: CPython',
            # 'Programming Language :: Python :: Implementation :: PyPy',
            'Topic :: Software Development :: Libraries :: Python Modules'

        ],
        license='Apache 2.0',
        keywords='hazelcast,hazelcast client,In-Memory Data Grid,Distributed Computing',
        packages=find_packages(exclude=['benchmarks', 'examples', 'examples.*', 'docs', 'tests', 'tests.*']),
        package_dir={'hazelcast': 'hazelcast'},
        package_data={'hazelcast': ["git_info.json"]},
        install_requires=[],
        extras_require=extras,
        tests_require=['thrift', 'nose', 'coverage', 'psutil'],
)
