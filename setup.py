import json
import subprocess

from setuptools import setup, find_packages
from codecs import open
from os import path
from hazelcast import __version__
here = path.abspath(path.dirname(__file__))


# Generate a JSON file that contains the
# client version, ID and date for
# the latest Github commit
def generate_hazelcast_info():
    info = dict()

    # Client version
    info["client_version"] = __version__

    # Latest Github commit date
    try:
        commit_date = subprocess.check_output(["git", "show", "-s", "--format=\"%cd\"", "--date=short"]).strip()
        commit_date = commit_date.replace("\"", "").replace("'").replace("-")
        info["git_commit_date"] = commit_date
    except:
        info["git_commit_date"] = ""

    # Latest Github commit id
    try:
        commit_id = subprocess.check_output(["git", "show", "-s", "--format=\"%h\""])
        commit_id = commit_id.replace("\"", "").replace("'")
        info["git_commit_id"] = commit_id
    except:
        info["git_commit_id"] = ""

    try:
        json_file_path = path.abspath(path.join(here, "hazelcast", "hazelcast_info.json"))
        with open(json_file_path, "w") as fp:
            json.dump(info, fp)
    except:
        pass


generate_hazelcast_info()

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
        install_requires=[],
        extras_require=extras,
        tests_require=['thrift', 'nose', 'coverage', 'psutil'],
)
