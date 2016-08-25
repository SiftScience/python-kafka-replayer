from setuptools import setup, find_packages
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))
with open(path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='kafka_replayer',
    version='1.0.1',
    description='Timestamp-based Kafka topic replayer',
    long_description=long_description,
    url='https://github.com/SiftScience/python-kafka-replayer',
    author='Sift Science',
    author_email='opensource@siftscience.com',
    license='Apache License 2.0',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Topic :: System :: Distributed Computing',
        'Topic :: System :: Clustering',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5'
    ],
    keywords='kafka consumer replayer replay',
    packages=find_packages(exclude=['contrib', 'docs', 'tests']),
    py_modules=['kafka_replayer'],
    install_requires=['kafka', 'six'],
    test_suite='test',
    zip_safe=False,
    include_package_data=True
)
