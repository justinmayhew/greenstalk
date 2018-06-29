from setuptools import setup

from greenstalk import __version__

setup(
    name='greenstalk',
    version=__version__,
    description='A Python 3 client for the beanstalkd work queue',
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
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
)
