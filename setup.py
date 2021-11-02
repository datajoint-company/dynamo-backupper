from setuptools import setup, find_packages
from os import path

here = path.abspath(path.dirname(__file__))

setup(
    name='dynamo_backupper',
    version='1.0.0',
    description='Dynamo Table Dumper',
    author='Daniel Sitonic',
    author_email='synicix@gmail.com',
    url='https://github.com/vathes/dynamo-backupper',
    packages=find_packages(),
    install_requires=['pandas', 'boto3', 'google-api-python-client', 'google-auth-httplib2'],
    scripts=['scripts/dynamo_backupper']
)
