from setuptools import setup

from greenstalk import __version__

setup(
    name='Greenstalk',
    version=__version__,
    description='A client for beanstalkd: the simple, fast work queue',
    long_description=open('README.rst').read(),
    author='Justin Mayhew',
    author_email='mayhew@live.ca',
    url='https://github.com/mayhewj/greenstalk',
    license='MIT',
    py_modules=['greenstalk'],
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
)
