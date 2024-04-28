from setuptools import find_packages
from setuptools import setup

setup(
    name='data-engineering',
    version='0.1.0',
    license=' Apache License, Version 2.0',
    description='Data Engineering, PySpark: unit, integration and end to end tests',
    author='Yevgeniy Yermoshin',
    packages=find_packages(where='src'),
    include_package_data=True,
    package_dir={'': 'src'},
)
