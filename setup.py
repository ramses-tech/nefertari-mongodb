import os

from setuptools import setup, find_packages

install_requires = [
    'mongoengine==0.9',
    'pymongo==2.8',
    'zope.dottedname',
    'elasticsearch',
    'python-dateutil',
    'pyramid_tm'
]

setup(
    name='nefertari_mongodb',
    version="0.1.0",
    description='mongodb engine for nefertari',
    classifiers=[
        "Programming Language :: Python",
        "Framework :: Pyramid",
        "Topic :: Internet :: WWW/HTTP",
        "Topic :: Internet :: WWW/HTTP :: WSGI :: Application",
        "Topic :: Database",
        "Topic :: Database :: Database Engines/Servers",
    ],
    author='',
    author_email='',
    url='',
    keywords='web wsgi bfg pylons pyramid rest mongodb mongoengine',
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    install_requires=install_requires,
)
