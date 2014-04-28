'''
Created on Feb 24, 2013
@author: nickmilon
see: https://docs.python.org/2/distutils/setupscript.html
'''
from setuptools  import setup, find_packages
from pymongo_ext import __version__
print 'packages', find_packages()
setup(
    packages=find_packages(),
    package_data={'pymongo_ext': ['js/*.js']},
    name="pymongo_ext",
    version=__version__,
    author="nickmilon",
    #author_email="nickmilon@gmail.com",
    maintainer="nickmilon",
    #maintainer_email="nickmilon@gmail.com",
    url="https://github.com/nickmilon/pymongo_ext",
    description="extensions for pymongo, see:http://github.com/mongodb/mongo-python-driver",
    long_description="see: readme",
    download_url="https://github.com/nickmilon/pymongo_ext.git",
    classifiers=[
    "Development Status :: 5 - Production/Stable",
    "Intended Audience :: Developers",
    "License :: OSI Approved :: Apache Software License",
    "Operating System :: MacOS :: MacOS X",
    "Operating System :: Microsoft :: Windows",
    "Operating System :: POSIX",
    "Programming Language :: Python :: 2.7",
    "Topic :: Database"],
    #platforms =
    license="MIT or Apache License, Version 2.0",
    keywords=["mongo", "mongodb", "pymongo", "pymongo_ext"],
    # requirements and specs
    zip_safe=False,
    tests_require=["nose"],
    #install_requires = [],
    install_requires=[
        'pymongo>=2.5',
        'gevent',
    ],
)
