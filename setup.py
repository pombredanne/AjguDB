#!/usr/bin/env python
import os
from setuptools import setup
from setuptools import find_packages


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(
    name='AjguDB',
    version='0.4.2',
    author='Amirouche Boubekki',
    author_email='amirouche@hypermove.net',
    url='https://github.com/amirouche/ajgudb',
    description='Graph Database for everyday',
    long_description=read('README.rst'),
    packages=find_packages(),
    zip_safe=False,
    license='LGPLv2.1 or later',
    install_requires=[
        'plyvel',
        'msgpack-python',
    ],
    classifiers=[
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Topic :: Software Development',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 2.7',
    ],
)
