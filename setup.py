from setuptools import setup

setup(
    name='greenstalk',
    version='0.1.0',
    description='A client for beanstalkd: the simple, fast work queue',
    long_description=open('README.rst').read(),
    author='Justin Mayhew',
    author_email='mayhew@live.ca',
    url='https://github.com/mayhewj/greenstalk',
    license='MIT',
    packages=['greenstalk'],
)
