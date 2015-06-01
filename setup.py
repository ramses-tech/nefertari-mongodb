from setuptools import setup, find_packages


install_requires = [
    'mongoengine==0.9',
    'pymongo==2.8',
    'zope.dottedname',
    'elasticsearch',
    'python-dateutil',
    'pyramid_tm',
    'nefertari==0.3.1'
]


setup(
    name='nefertari_mongodb',
    version="0.2.1",
    description='mongodb engine for nefertari',
    classifiers=[
        "Programming Language :: Python",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.4",
        "Framework :: Pyramid",
        "Topic :: Internet :: WWW/HTTP",
        "Topic :: Internet :: WWW/HTTP :: WSGI :: Application",
        "Topic :: Database",
        "Topic :: Database :: Database Engines/Servers",
    ],
    author='Brandicted',
    author_email='hello@brandicted.com',
    url='https://github.com/brandicted/nefertari-mongodb',
    keywords='web wsgi bfg pylons pyramid rest mongodb mongoengine',
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    install_requires=install_requires,
)
