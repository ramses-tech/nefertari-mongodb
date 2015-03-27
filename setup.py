import os

from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))

install_requires = [
    'mongoengine',
    'zope.dottedname',
    'elasticsearch',
    'python-dateutil',
    'pyramid_tm'
]

setup(
    name='nefertari_mongodb',
    version="0.1",
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
